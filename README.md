# Verify Longhorn backups

This utility scans an S3 bucket, verifies the
[Longhorn](https://longhorn.io/) backups therein,
then exports metrics about them which could be used
to alert if they are bad.

# Usage

```shell
check-backups \
    --bucket-name=longhorn-backup \
    --s3-region-name=foobar \
    --s3-endpoint=https://somethingn \
    --diag-http-port=6117
```

# Options

| Flag                     | Default | Meaning                 |
|--------------------------|---------|-------------------------|
| `--check_cache_time`     | 7d      | Amount of time to remember successfully verified backups, to avoid repeatedly readinng their full contents. |
| `--check_cache_capacity` | 1000    | Maximum size of this cache. |
| `--probe_interval`       | 1h      | Probe frequency |
