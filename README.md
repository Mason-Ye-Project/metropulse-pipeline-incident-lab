# MetroPulse Incident Lab

Companion software for *The Data Pipeline Troubleshooting Lab*. It is a local,
deterministic, disposable simulator of a **fictional** city-transit data
platform called MetroPulse. Every number it produces is a synthetic lab value —
not a real transit agency, a real incident, or anyone's work history.

## Zero-install quick start

Requires only **CPython 3.12** and its standard library (no `pip install`, no
cloud account, no network).

The examples use `python` to mean the CPython 3.12 executable. If `python
--version` reports another release, substitute `python3.12` or the full path to
your 3.12 interpreter.

```bash
python -m metropulse_lab doctor
python -m metropulse_lab incidents list
python -m metropulse_lab start --incident MP-01     # create + inject + run + detect + evidence
python -m metropulse_lab reference-recovery --run <run-id>   # contain + repair + recover + verify
python -m metropulse_lab reset --run <run-id>
```

This first-edition companion is pinned to the immutable `v1.0.0` release:

```bash
git clone https://github.com/Mason-Ye-Project/metropulse-pipeline-incident-lab.git
cd metropulse-pipeline-incident-lab
git checkout v1.0.0
python -m metropulse_lab doctor
```

Readers who prefer a ZIP can download
[`MetroPulse_Incident_Lab_v1.0.0.zip`](https://github.com/Mason-Ye-Project/metropulse-pipeline-incident-lab/releases/download/v1.0.0/MetroPulse_Incident_Lab_v1.0.0.zip).
The release asset includes a SHA-256 checksum file.

Individual lifecycle steps are also available:

```
create  inject  run  detect  evidence  contain  repair  recover  verify  reset
```

## Tests

```bash
python -m unittest discover -s tests -t .
python -m metropulse_lab test-all
```

## Design guarantees

- **Deterministic.** A checked-in seed plus an incident id yields the same
  logical rows and verification result on every OS. SQLite file bytes are never
  an oracle; sorted logical rows are.
- **Evidence-first.** Reader-facing alerts and the initial evidence pack never
  name the root cause.
- **Isolated & reversible.** Each incident runs in its own disposable run
  directory under `.lab/runs/`. `reset` removes only an allowlisted run
  directory after path validation.
- **Honest scope.** Tools such as DuckDB, PostgreSQL, Spark, Airflow, Flink,
  Kafka, dbt, and Iceberg appear only as clearly labeled transfer mappings. The
  core never fabricates their logs, plans, or runtime numbers.

## License

MIT for the software, SQL, synthetic fixtures, and concise technical docs. It
does **not** license the book manuscript, cover, book-only diagrams, or KDP
metadata. See the book repository for those boundaries.

## Book and release map

- Book: *The Data Pipeline Troubleshooting Lab* by MASON YE
- First-edition lab tag: `v1.0.0`
- Supported runtime: CPython 3.12, standard library only
- Fixture catalog: `1.0`; baseline: `metro-v1`
- Version map: [`docs/book-version-map.md`](docs/book-version-map.md)
- Official source ledger: [`docs/source_ledger.csv`](docs/source_ledger.csv)
- Errata and code issues: [GitHub Issues](https://github.com/Mason-Ye-Project/metropulse-pipeline-incident-lab/issues)
