.PHONY: doctor test test-all manifest

doctor:
	python -m metropulse_lab doctor

test:
	python -m unittest discover -s tests -t .

test-all:
	python -m metropulse_lab test-all

manifest:
	python scripts/generate_release_manifest.py
