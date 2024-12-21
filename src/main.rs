use chrono::{offset::FixedOffset, DateTime};
use comprehensive::{Resource, ResourceDependencies};
use futures::future::Either;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use lazy_static::lazy_static;
use lz4::Decoder;
use prometheus::{register_counter, register_gauge_vec};
use s3::{error::S3Error, Bucket};
use serde::Deserialize;
use sha2::{Digest, Sha512};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::ops::Bound::Included;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_task_pool::Pool;
use tokio_util::sync::CancellationToken;
use ttl_cache::TtlCache;

lazy_static! {
    static ref PROBES_STARTED: prometheus::Counter = register_counter!(
        "backup_probes_started",
        "Number of probes for backups started"
    )
    .unwrap();
    static ref PROBES_FINISHED: prometheus::Counter = register_counter!(
        "backup_probes_completed",
        "Number of probes for backups completed"
    )
    .unwrap();
    static ref LATEST_SUCCESSFUL_BACKUP: prometheus::GaugeVec = register_gauge_vec!(
        "latest_successful_backup",
        "Time of latest successful backup",
        &["backup_pvc_namespace", "backup_pvc_name"]
    )
    .unwrap();
    static ref BACKUP_COUNTS: prometheus::GaugeVec = register_gauge_vec!(
        "backup_count",
        "Backup count by outcome",
        &["backup_pvc_namespace", "backup_pvc_name", "outcome"]
    )
    .unwrap();
}

#[derive(clap::Args, Debug)]
pub struct ProberArgs {
    #[arg(long, value_parser = humantime::parse_duration, default_value = "7d")]
    check_cache_time: Duration,
    #[arg(long, default_value = "1000")]
    check_cache_capacity: usize,
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1h")]
    probe_interval: Duration,
    #[arg(long, default_value = "5")]
    backup_probe_worker_pool_size: usize,
    #[arg(long, default_value = "backupstore/volumes/")]
    backupstore_root_path: String,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BackupBlock {
    Offset: u64,
    BlockChecksum: String,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct BackupCfg {
    CompressionMethod: String,
    Blocks: Vec<BackupBlock>,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct KubernetesStatus {
    pvcName: String,
    namespace: String,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct VolumeLabels {
    KubernetesStatus: String,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case)]
struct VolumeCfg {
    Labels: VolumeLabels,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct Volume {
    root: String,
    namespace: String,
    pvc_name: String,
}

comprehensive_s3::bucket!(BackupsBucket, "Backup storage", "");

#[derive(Clone, Debug)]
struct OffsetLength(u64, u64);

#[derive(Clone, Debug)]
enum BadBackup {
    Cancelled,
    BadChecksum(u64),
    ChunkOverlap(OffsetLength, OffsetLength),
    ReadError(String),
    InternalError(String),
}

impl BadBackup {
    fn tag(&self) -> &'static str {
        match *self {
            Self::Cancelled => "probe_cancelled",
            Self::BadChecksum(_) => "wrong_checksum",
            Self::ChunkOverlap(..) => "chunk_overlap",
            Self::ReadError(_) => "read_error",
            Self::InternalError(_) => "internal_error",
        }
    }
}

impl std::fmt::Display for BadBackup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Cancelled => write!(f, "Task cancelled"),
            Self::BadChecksum(offset) => write!(f, "Checksum mismatch at offset {}", offset),
            Self::ChunkOverlap(OffsetLength(o1, l1), OffsetLength(o2, l2)) => {
                write!(f, "Overlapping chunks: [{}+{}] and [{}+{}]", o1, l1, o2, l2)
            }
            Self::ReadError(ref e) => write!(f, "{}", e),
            Self::InternalError(ref e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for BadBackup {}

impl From<S3Error> for BadBackup {
    fn from(e: S3Error) -> Self {
        Self::ReadError(e.to_string())
    }
}

impl From<std::io::Error> for BadBackup {
    fn from(e: std::io::Error) -> Self {
        Self::ReadError(e.to_string())
    }
}

fn parse_volume(c: &[u8], prefix: &str) -> Result<Volume, serde_json::Error> {
    let v = serde_json::from_slice::<VolumeCfg>(c)?;
    let ks = serde_json::from_str::<KubernetesStatus>(&v.Labels.KubernetesStatus)?;
    Ok(Volume {
        root: prefix.to_owned(),
        namespace: ks.namespace,
        pvc_name: ks.pvcName,
    })
}

async fn check_block(
    bucket: &Bucket,
    block: BackupBlock,
    blockname: String,
) -> Result<OffsetLength, BadBackup> {
    // https://github.com/longhorn/backupstore/blob/master/util/util.go
    let expect_checksum =
        hex::decode(block.BlockChecksum).map_err(|_| BadBackup::BadChecksum(block.Offset))?;
    let contents = bucket.get_object(blockname).await?;
    let mut dec = Decoder::new(contents.as_slice())?;
    let mut out = Vec::new();
    dec.read_to_end(&mut out)?;
    let l = out.len();
    let mut hasher = Sha512::new();
    hasher.update(out);
    let checksum = hasher.finalize();
    if checksum.as_slice()[..32] == expect_checksum {
        Ok(OffsetLength(
            block.Offset,
            l.try_into()
                .map_err(|_| BadBackup::InternalError(String::from("u64 fail")))?,
        ))
    } else {
        Err(BadBackup::BadChecksum(block.Offset))
    }
}

#[derive(Debug)]
struct BackupIdent {
    key: Arc<String>,
    vol: Arc<Volume>,
}

type BackupResult = Result<DateTime<FixedOffset>, BadBackup>;

#[derive(Debug)]
struct BackupDetails {
    ident: BackupIdent,
    result: BackupResult,
}

async fn check_backup_inner<T: AsRef<Bucket> + Send + Sync + 'static>(
    bucket: Arc<T>,
    worker_pool: Arc<Pool>,
    ident: BackupIdent,
    index: BackupCfg,
    ts: DateTime<FixedOffset>,
    cancel: CancellationToken,
) -> BackupDetails {
    let blocks_cancel = cancel.child_token();
    let vol = Arc::clone(&ident.vol);
    match cancel
        .run_until_cancelled(async move {
            let _auto_cancel = blocks_cancel.clone().drop_guard();
            let mut jhl: FuturesUnordered<_> = index
                .Blocks
                .into_iter()
                .map(|block| {
                    let blockname = format!(
                        "{}blocks/{}/{}/{}.blk",
                        vol.root,
                        &block.BlockChecksum[0..2],
                        &block.BlockChecksum[2..4],
                        &block.BlockChecksum
                    );
                    let bucket = Arc::clone(&bucket);
                    let cancel = blocks_cancel.clone();
                    worker_pool
                        .spawn(async move {
                            let cancel = pin!(cancel.cancelled_owned());
                            let check = pin!(check_block(
                                bucket.as_ref().as_ref(),
                                block,
                                blockname.clone()
                            ));
                            match futures::future::select(cancel, check).await {
                                Either::Left(((), _)) => Err(BadBackup::Cancelled),
                                Either::Right((Ok(ol), _)) => Ok(ol),
                                Either::Right((Err(e), _)) => Err(e),
                            }
                        })
                        .then(|sr| async move {
                            match sr {
                                Err(e) => Err(BadBackup::InternalError(format!(
                                    "Error submitting task to worker queue: {}",
                                    e
                                ))),
                                Ok(jh) => match jh.await {
                                    Err(e) => Err(BadBackup::InternalError(format!(
                                        "Error joining task from worker queue: {}",
                                        e
                                    ))),
                                    Ok(Err(e)) => Err(BadBackup::InternalError(format!(
                                        "Error running task in worker queue: {}",
                                        e
                                    ))),
                                    Ok(Ok(success)) => success,
                                },
                            }
                        })
                })
                .collect();
            let mut ranges = BTreeMap::<u64, u64>::new();
            while let Some(r) = jhl.next().await {
                match r {
                    Ok(OffsetLength(offset, len)) => {
                        if let Some((prev_offset, prev_len)) =
                            ranges.range((Included(&0), Included(&offset))).next_back()
                        {
                            if prev_offset + prev_len > offset {
                                return Err(BadBackup::ChunkOverlap(
                                    OffsetLength(*prev_offset, *prev_len),
                                    OffsetLength(offset, len),
                                ));
                            }
                        }
                        if let Some((next_offset, next_len)) = ranges
                            .range((Included(&offset), Included(&u64::MAX)))
                            .next()
                        {
                            if offset + len > *next_offset {
                                return Err(BadBackup::ChunkOverlap(
                                    OffsetLength(offset, len),
                                    OffsetLength(*next_offset, *next_len),
                                ));
                            }
                        }
                        ranges.insert(offset, len);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(ts)
        })
        .await
    {
        Some(result) => BackupDetails { ident, result },
        None => BackupDetails {
            ident,
            result: Err(BadBackup::Cancelled),
        },
    }
}

struct SingleProbe<'a, T> {
    bucket: &'a Arc<T>,
    cache: &'a mut TtlCache<Arc<String>, BackupResult>,
    worker_pool: Arc<Pool>,
    join_handles: Vec<JoinHandle<Result<BackupDetails, tokio_task_pool::Error>>>,
    immediate_results: Vec<BackupDetails>,
    cancel: CancellationToken,
}

impl<'a, T> SingleProbe<'a, T>
where
    T: AsRef<Bucket> + Send + Sync + 'static,
{
    async fn check_backup(&mut self, index: BackupCfg, key: String, ts: &str, vol: Arc<Volume>) {
        let ident = BackupIdent {
            key: Arc::new(key),
            vol,
        };
        let ts = match DateTime::parse_from_rfc3339(ts) {
            Ok(t) => t,
            Err(e) => {
                self.immediate_results.push(BackupDetails {
                    ident,
                    result: Err(BadBackup::ReadError(format!(
                        "unparseable timestamp {}: {}",
                        ts, e
                    ))),
                });
                return;
            }
        };
        if index.CompressionMethod != "lz4" {
            self.immediate_results.push(BackupDetails {
                ident,
                result: Err(BadBackup::ReadError(String::from(
                    "only support lz4 as a CompressionMethod",
                ))),
            });
            return;
        }
        if let Some(existing) = self.cache.get(&ident.key) {
            self.immediate_results.push(BackupDetails {
                ident,
                result: existing.clone(),
            });
            return;
        }
        match self
            .worker_pool
            .spawn(check_backup_inner(
                Arc::clone(self.bucket),
                Arc::clone(&self.worker_pool),
                ident,
                index,
                ts,
                self.cancel.clone(),
            ))
            .await
        {
            Ok(jh) => {
                self.join_handles.push(jh);
            }
            Err(e) => {
                log::error!("Error spawning task to check backup: {}", e);
            }
        }
    }

    async fn descend(&mut self, prefix: &str, volume: Option<&Arc<Volume>>) -> Result<(), S3Error> {
        let entries = self
            .bucket
            .as_ref()
            .as_ref()
            .list(prefix.to_owned(), Some(String::from("/")))
            .await?;
        for result in entries.into_iter() {
            let mut down_volume = None;
            for obj in result.contents.into_iter() {
                let contents = self
                    .bucket
                    .as_ref()
                    .as_ref()
                    .get_object(obj.key.clone())
                    .await?;
                match volume {
                    None => {
                        if down_volume.is_some() {
                            log::warn!("More than 1 volume at {}", prefix);
                            return Ok(());
                        }
                        down_volume = Some(Arc::new(parse_volume(contents.as_slice(), prefix)?));
                    }
                    Some(v) => {
                        let backup = serde_json::from_slice::<BackupCfg>(contents.as_slice())?;
                        self.check_backup(backup, obj.key, &obj.last_modified, Arc::clone(v))
                            .await;
                    }
                }
            }
            if let Some(cps) = result.common_prefixes {
                let (v, at_volume) = match down_volume {
                    None => (volume, false),
                    Some(ref dv) => (Some(dv), true),
                };
                for cp in cps.into_iter() {
                    if at_volume && !cp.prefix.ends_with("/backups/") {
                        continue;
                    }
                    Box::pin(self.walk(&cp.prefix, v)).await;
                }
            }
        }
        Ok(())
    }

    async fn walk(&mut self, prefix: &str, volume: Option<&Arc<Volume>>) {
        if let Err(e) = self.descend(prefix, volume).await {
            log::error!("{}: {}", prefix, e);
        }
    }

    async fn probe(
        b: &'a Arc<T>,
        cache: &'a mut TtlCache<Arc<String>, BackupResult>,
        config: &'a ProberArgs,
    ) -> Vec<BackupDetails> {
        let cancel = CancellationToken::new();
        let _auto_cancel = cancel.clone().drop_guard();
        let mut this = Self {
            bucket: b,
            cache,
            worker_pool: Arc::new(Pool::bounded(config.backup_probe_worker_pool_size)),
            join_handles: Vec::new(),
            immediate_results: Vec::new(),
            cancel,
        };
        this.walk(&config.backupstore_root_path, None).await;
        let mut out = this.immediate_results;
        for jh in this.join_handles.into_iter() {
            match jh.await {
                Err(e) => {
                    log::error!("Error joining check_backup task: {}", e);
                }
                Ok(Err(e)) => {
                    log::error!("Error running check_backup task: {}", e);
                }
                Ok(Ok(details)) => {
                    this.cache.insert(
                        Arc::clone(&details.ident.key),
                        details.result.clone(),
                        config.check_cache_time,
                    );
                    out.push(details);
                }
            }
        }
        out
    }
}

struct Prober {
    bucket: Arc<BackupsBucket>,
    config: ProberArgs,
}

#[derive(ResourceDependencies)]
struct ProberDependencies {
    bucket: Arc<BackupsBucket>,
}

impl Resource for Prober {
    type Args = ProberArgs;
    type Dependencies = ProberDependencies;
    const NAME: &str = "Backup prober";

    fn new(d: ProberDependencies, a: ProberArgs) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            bucket: d.bucket,
            config: a,
        })
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = TtlCache::new(self.config.check_cache_capacity);
        let mut good_backups = HashMap::new();
        let mut counts = HashMap::new();
        loop {
            let next_run = tokio::time::Instant::now() + self.config.probe_interval;
            PROBES_STARTED.inc();
            if tokio::time::timeout_at(next_run, async {
                // Mark everything unseen
                for (_, v) in good_backups.iter_mut() {
                    *v = None;
                }
                for (_, v) in counts.iter_mut() {
                    *v = 0;
                }

                for details in SingleProbe::probe(&self.bucket, &mut cache, &self.config)
                    .await
                    .into_iter()
                {
                    let tag = match details.result {
                        Ok(ref ts) => {
                            let this_ts = ts.timestamp();
                            let entry = good_backups
                                .entry(Arc::clone(&details.ident.vol))
                                .or_insert(Some(this_ts));
                            let replace = match *entry {
                                None => true,
                                Some(prev) => prev < this_ts,
                            };
                            if replace {
                                *entry = Some(this_ts);
                            }
                            "successful"
                        }
                        Err(ref e) => {
                            log::warn!("Bad backup: {:?}: {}", details.ident, e);
                            e.tag()
                        }
                    };
                    *counts.entry((details.ident.vol, tag)).or_insert(0) += 1;
                }
                good_backups.retain(|vol, v| {
                    match v {
                        None => {
                            let _ = LATEST_SUCCESSFUL_BACKUP
                                .remove_label_values(&[&vol.namespace, &vol.pvc_name]);
                        }
                        Some(ts) => {
                            LATEST_SUCCESSFUL_BACKUP
                                .with_label_values(&[
                                    &vol.namespace,
                                    &vol.pvc_name,
                                ])
                                .set(*ts as f64);
                        }
                    }
                    v.is_some()
                });
                for ((vol, tag), v) in counts.iter() {
                    BACKUP_COUNTS
                        .with_label_values(&[&vol.namespace, &vol.pvc_name, tag])
                        .set(*v as f64);
                }
            })
            .await
            .is_err()
            {
                log::error!("Backup probe ran out of time");
            };
            PROBES_FINISHED.inc();
            tokio::time::sleep_until(next_run).await;
        }
    }
}

#[derive(ResourceDependencies)]
struct TopDependencies {
    _prober: Arc<Prober>,
    _diag: Arc<comprehensive::diag::HttpServer>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    comprehensive::Assembly::<TopDependencies>::new()?
        .run()
        .await?;
    Ok(())
}
