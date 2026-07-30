# Book Version Map

## First published edition

- Book: *The Data Pipeline Troubleshooting Lab*
- Author: MASON YE
- Companion release: `v1.0.0`
- Release date: 2026-07-31
- Supported runtime: CPython 3.12
- Fixture catalog: `1.0`
- Baseline id: `metro-v1`
- Incident ids: `MP-01` through `MP-25`
- Release manifest: [`release-manifest.json`](../release-manifest.json)
- Known errata: [GitHub Issues](https://github.com/Mason-Ye-Project/metropulse-pipeline-incident-lab/issues)

Resolve the immutable tag to its exact commit with:

```bash
git rev-parse v1.0.0^{commit}
```

The printed and Kindle editions record that commit explicitly. Later compatible
patches do not change what the first edition showed; use `v1.0.0` when
reproducing its outputs.
