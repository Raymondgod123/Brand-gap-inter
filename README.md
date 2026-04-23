# Brand Gap Inference

Phase 1 establishes the production rails for the system before any source-specific ingestion work starts.

This scaffold includes:

- schema-bound contracts for normalized listings, taxonomy assignments, evidence, opportunities, run manifests, and task envelopes
- a zero-dependency validation layer that enforces a focused JSON Schema subset
- a deterministic eval gate over golden fixtures
- initial run metadata models so future orchestration work is resumable and auditable
- source-agnostic ingestion interfaces with replayable raw snapshot storage
- a cleaning and normalization layer with explicit duplicate handling and partial-failure reporting
- a CI workflow that blocks changes when contracts or evals regress

## Project Layout

- `schemas/` holds the master contract definitions
- `src/brand_gap_inference/` holds runtime validators, models, and the eval CLI
- `eval/fixtures/phase1/` holds golden inputs for the first gate
- `fixtures/connectors/` holds replayable connector payloads
- `data/raw/` is the default local snapshot store for raw source payloads
- `tests/` verifies the contract layer and eval runner
- `src/brand_gap_inference/normalization.py` batches raw records into validated normalized listings with run summaries
- `src/brand_gap_inference/taxonomy.py` assigns first-pass multi-axis taxonomy labels from normalized listings

## Phase 1 Commands

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -p "test_*.py"
python -m brand_gap_inference.eval_runner --fixtures-dir eval/fixtures/phase1 --thresholds eval/thresholds.json
```

If the shell does not expose Python directly, use the bundled runtime provided by Codex desktop.

## Ingestion Foundation

Phase 2 starts with a source-agnostic ingestion layer:

- `SourceConnector` defines the boundary real marketplace connectors must implement
- `FilesystemRawStore` persists raw records and a snapshot manifest under `data/raw/`
- `IngestionService` executes a connector, stores the snapshot, and reloads it for replay
- `AmazonProductConnector` normalizes product URLs, fetches the live page, and stores fetch diagnostics for replay

### Live Amazon Probe

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.amazon_ingest --url "https://www.amazon.com/dp/B098H7XWQ6" --store-dir data/raw
```

The command stores the raw HTML snapshot locally and prints a short JSON summary with the ASIN, canonical URL, final URL, title, and whether Amazon returned a robot-check page.

## Normalization Layer

The cleaning layer is built to fail clearly instead of guessing silently:

- records normalize into `NormalizedListing` outputs only after schema validation passes
- duplicate records are marked as duplicates and excluded from output counts
- invalid or blocked records are reported explicitly, not merged into successful output
- batch summaries return `success`, `partial_success`, or `failed` so operators can explain what happened

### Normalization Artifacts

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.normalize_snapshot --store-dir data/raw --source amazon --snapshot-id amazon-B098H7XWQ6-2026-04-22T01-54-11Z --output-dir artifacts/amazon-B098H7XWQ6
```

The command can now emit:

- `normalized_listings.json`
- `normalization_report.json`
- `normalization_records.json`

## Taxonomy Layer

The first taxonomy pass is rule-based so we can inspect and debug it before adding LLM-assisted reasoning:

- assigns `need_state`, `occasion`, `format`, and `audience`
- emits confidence on every assignment
- writes structured assignment and report artifacts for operator review
- is now backed by reusable golden labeled fixtures under `eval/fixtures/taxonomy_golden/`
- is now measured by threshold-based taxonomy evals under `eval/taxonomy_thresholds.json`

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.assign_taxonomy --normalized-listings artifacts/amazon-B098H7XWQ6/normalized_listings.json --snapshot-id amazon-B098H7XWQ6-2026-04-22T01-54-11Z --output-dir artifacts/amazon-B098H7XWQ6
```

### Taxonomy Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.taxonomy_eval --cases eval/fixtures/taxonomy_golden/cases.json --thresholds eval/taxonomy_thresholds.json
```

This reports:

- exact case accuracy on the golden fixture set
- per-axis accuracy for `need_state`, `occasion`, `format`, and `audience`
- invalid assignment rate
- warning expectation pass rate
- repeat-run stability

### Normalization Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.normalization_eval --cases eval/fixtures/normalization_golden/batches.json --thresholds eval/normalization_thresholds.json
```

This reports:

- record status accuracy (normalized / duplicate / invalid)
- duplicate linkage correctness
- provenance coverage for key fields
- low-confidence reason coverage for known dirty cases
- repeat-run stability on the same inputs
