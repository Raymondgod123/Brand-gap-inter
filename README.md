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

## Full Quality Gate

Use this before handoff or commit when the working slice touches pipeline behavior:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.quality_gates
```

This mirrors the CI workflow locally: unit tests plus phase1, discovery, taxonomy, normalization, product intelligence, brand positioning, brand profile, demand signal, gap validation, decision brief, and deep inference eval gates.

## MVP Run (One Amazon URL)

This is the narrow MVP flow: ingest one Amazon product page, store a raw snapshot, normalize the listing, assign taxonomy,
and generate one evidence-backed gap hypothesis report with explicit caveats.

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --url "https://www.amazon.com/dp/B098H7XWQ6"
```

Default output goes to a predictable mode folder:

- `artifacts/mvp-live-<timestamp>/`

Browser-assisted live acquisition is now available as an explicit opt-in:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --url "https://www.amazon.com/dp/B098H7XWQ6" --acquisition-mode browser --output-dir artifacts/mvp-browser-live
```

This preserves the same trust rails:

- raw snapshot storage
- safe-stop on missing critical fields
- the same normalization, taxonomy, and MVP artifact bundle contract

## MVP Run (Candidate URL Fallback)

When live source pages are unstable, run a candidate list and let the runner stop on first success:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --urls-file fixtures/mvp/demo_urls.txt
```

Optional:

- pass `--output-dir artifacts/mvp-demo` to keep attempts grouped under one folder
- leave it unset to use default `artifacts/mvp-live-fallback-<timestamp>/`

To test the browser-assisted spike against the curated live batch:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --urls-file fixtures/mvp/browser_spike_urls.txt --acquisition-mode browser --output-dir artifacts/mvp-browser-live-fallback
```

Fallback mode now also writes:

- `fallback_attempts.json`
- `fallback_report.md`

## MVP Run (Audited Snapshot Replay)

For predictable demos under live-source instability, replay a stored snapshot:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --snapshot-id "amazon-B098H7XWQ6-2026-04-22T01-54-11Z" --source amazon
```

This skips live fetch and reuses audited raw data already stored under `data/raw/`.
Default output goes to `artifacts/mvp-replay-<snapshot_id>/`.

Known good replay demo snapshot:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --snapshot-id "amazon-CLEAN00001-2026-04-24T00-00-00Z" --source amazon --output-dir artifacts/mvp-replay-demo
```

See `fixtures/mvp/demo_snapshot_ids.txt` for curated replay ids.

Known safe-stop replay example (expected to fail clearly):

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --snapshot-ids-file fixtures/mvp/demo_snapshot_failure_ids.txt --source amazon --output-dir artifacts/mvp-replay-failure-demo
```

Replay fallback mode (tries multiple audited snapshots until one succeeds):

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --snapshot-ids-file fixtures/mvp/demo_snapshot_ids.txt --source amazon --output-dir artifacts/mvp-replay-fallback
```

Outputs are written under `artifacts/mvp-<snapshot_id>/`:

- `mvp_report.md` (human-readable demo output)
- `opportunities.json` (machine-readable hypothesis output)
- plus normalization and taxonomy artifacts for traceability
- `mvp_bundle_manifest.json` (run-level artifact manifest)

`mvp_report.md` now includes:

- cleaned listing summary
- taxonomy summary
- caveats (warnings + low-confidence reasons)
- provenance snapshot for key parsed fields (brand, price, pack, unit, category, availability)

## Stakeholder Demo Commands

One-command release rehearsal (replay success + replay safe-stop):

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_release_check --output-dir artifacts/mvp-release-check
```

Optional live attempt during release rehearsal:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_release_check --include-live-attempt --output-dir artifacts/mvp-release-check-live
```

Replay success demo:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --snapshot-id "amazon-CLEAN00001-2026-04-24T00-00-00Z" --source amazon --output-dir artifacts/mvp-demo-success
```

Replay safe-stop demo:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --snapshot-ids-file fixtures/mvp/demo_snapshot_failure_ids.txt --source amazon --output-dir artifacts/mvp-demo-safe-stop
```

Live attempt demo:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.mvp_run --urls-file fixtures/mvp/demo_urls.txt --output-dir artifacts/mvp-demo-live
```

Artifact review:

```powershell
Get-ChildItem artifacts/mvp-demo-success
Get-Content artifacts/mvp-demo-success/mvp_report.md
Get-Content artifacts/mvp-demo-success/mvp_bundle_manifest.json
```

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

Browser-assisted live ingest:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.amazon_ingest --url "https://www.amazon.com/dp/B098H7XWQ6" --acquisition-mode browser --store-dir data/raw
```

The command stores the raw HTML snapshot locally and prints a short JSON summary with the ASIN, canonical URL, final URL, title, and whether Amazon returned a robot-check page.
Browser-mode summaries also include capture diagnostics such as wait strategy and visible offer-state signals.

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

## Discovery Ingestion (SERP API)

The first transition slice adds a replayable keyword discovery lane without changing the current MVP replay flow.

Set the provider key for live discovery:

```powershell
$env:PYTHONPATH = "src"
$env:SERPAPI_API_KEY = "<your_serpapi_key>"
```

Run live keyword discovery:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.discover_products --keyword "monk fruit sweetener"
```

Replay a stored discovery snapshot:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.discover_products --snapshot-id "amazon_api_discovery-serpapi-monk-fruit-sweetener-cf05cf722d-2026-04-24T12-00-00Z"
```

Outputs are written under:

- live: `artifacts/discovery-live-<timestamp>/`
- replay: `artifacts/discovery-replay-<snapshot_id>/`

Artifacts include:

- `discovery_records.json`
- `discovery_report.json`
- `discovery_bundle_manifest.json`

The discovery lane writes raw provider snapshots under `data/raw/amazon_api_discovery/` and keeps replay support intact.

## One-Shot Data Collection

The collection layer runs discovery, applies deterministic query-fit candidate selection, and optionally collects structured product detail by ASIN through SerpApi's Amazon Product API.

Product-detail extraction now treats offer state as a trust rail:

- safe primary-offer prices are accepted from `single_offer` or in-stock product-result signals
- unavailable, out-of-stock, no-featured-offer, or buying-options-only shapes block detail price extraction
- when detail offer state is unsafe, product intelligence does not fall back to the search-result price
- availability is normalized to explicit review statuses such as `Currently unavailable` or `No featured offer`

Live one-shot collection:

```powershell
$env:PYTHONPATH = "src"
$env:SERPAPI_API_KEY = "<your_serpapi_key>"
python -m brand_gap_inference.collect_data --keyword "monk fruit sweetener" --max-products 5
```

Live one-shot collection plus deterministic post-analysis:

```powershell
$env:PYTHONPATH = "src"
$env:SERPAPI_API_KEY = "<your_serpapi_key>"
python -m brand_gap_inference.collect_data --keyword "monk fruit sweetener" --max-products 5 --post-analysis deterministic
```

Live one-shot collection plus deterministic post-analysis and deep inference:

```powershell
$env:PYTHONPATH = "src"
$env:SERPAPI_API_KEY = "<your_serpapi_key>"
$env:OPENAI_API_KEY = "<your_openai_api_key>"
python -m brand_gap_inference.collect_data --keyword "monk fruit sweetener" --max-products 5 --post-analysis deep_inference
```

Replay discovery first, then skip live product-detail calls:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.collect_data --discovery-snapshot-id "<stored_discovery_snapshot_id>" --detail-mode none
```

Replay discovery and collect fresh product details for the selected ASINs:

```powershell
$env:PYTHONPATH = "src"
$env:SERPAPI_API_KEY = "<your_serpapi_key>"
python -m brand_gap_inference.collect_data --discovery-snapshot-id "<stored_discovery_snapshot_id>" --max-products 3
```

Default output goes to `artifacts/data-collection-<timestamp>/`.

Artifacts include:

- `discovery/discovery_records.json`
- `selection_report.json`
- `selected_candidates.json`
- `details/product_detail_records.json` when product detail collection is enabled
- `product_intelligence/product_intelligence_records.json` when detail records are available
- `data_collection_report.json`
- `data_collection_bundle_manifest.json`
- `analysis_stack/analysis_stack_report.json` when `--post-analysis` is enabled
- `evidence_workbench/index.html` when deterministic analysis completes
- downstream analysis artifacts under:
  - `landscape/`
  - `brand_positioning/`
  - `brand_profiles/`
  - `demand_signals/`
  - `gap_validation/`
  - `decision_brief/`
  - `evidence_workbench/`
  - `deep_inference/` when requested

Selection behavior:

- Discovery results are no longer taken strictly by raw provider rank.
- The selector now uses a deterministic query-family fit pass before collection.
- It preserves rank within the preferred pool, but filters obvious adjacent-category results when the query intent is clear.
- Selected candidates carry `selection_trace` metadata so support can see:
  - fit score
  - matched category terms
  - adjacent-category terms
  - selection bucket and reasons

Example: the query `zero calories candy` now filters out adjacent products like energy drinks and fruit snacks when they do not carry enough direct candy-format evidence.

Adjacent CPG validation contexts now cover:

- `hydration` for queries such as `electrolyte powder`
- `protein_powder` for queries such as `protein powder`
- `energy_drink` for queries such as `energy drink`

Live adjacent CPG validation sprint commands:

```powershell
$env:PYTHONPATH = "src"
$env:SERPAPI_API_KEY = "<your_serpapi_key>"

python -m brand_gap_inference.collect_data --keyword "electrolyte powder" --max-products 15 --detail-mode serpapi_product --post-analysis deterministic --output-dir artifacts/data-collection-live-electrolyte-powder-15-validation-v1

python -m brand_gap_inference.collect_data --keyword "protein powder" --max-products 15 --detail-mode serpapi_product --post-analysis deterministic --output-dir artifacts/data-collection-live-protein-powder-15-validation-v1

python -m brand_gap_inference.collect_data --keyword "energy drink" --max-products 15 --detail-mode serpapi_product --post-analysis deterministic --output-dir artifacts/data-collection-live-energy-drink-15-validation-v1
```

Each run writes an Evidence Workbench at `evidence_workbench/index.html`. Treat these pages as review surfaces: they summarize product coverage, warnings, market-structure coverage, gap candidates, and PM recommendation without changing the JSON artifacts that remain the source of truth.

Raw snapshots are stored separately:

- discovery breadth: `data/raw/amazon_api_discovery/`
- product detail enrichment: `data/raw/amazon_api_product/`

Regenerate product-intelligence artifacts from a saved collection run without calling live APIs:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.merge_product_intelligence --collection-dir artifacts/data-collection-live-monk-fruit-3
```

To run the full deterministic post-collection analysis stack on an existing saved collection:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.analyze_collection --collection-dir artifacts/data-collection-live-sugar-3
```

Optional deep inference on the same saved collection:

```powershell
$env:PYTHONPATH = "src"
$env:OPENAI_API_KEY = "<your_openai_api_key>"
python -m brand_gap_inference.analyze_collection --collection-dir artifacts/data-collection-live-sugar-3 --include-deep-inference
```

The analysis stack writes:

- `analysis_stack/analysis_stack_report.json`
- `analysis_stack/analysis_stack_report.md`
- `analysis_stack/analysis_stack_bundle_manifest.json`
- `evidence_workbench/index.html`

This gives one bounded replayable path from product intelligence into market-map, demand-signal, gap-analysis, and PM-facing decision-brief outputs without needing to rerun live collection.

## Minimalist Evidence Dashboard

The Minimalist Evidence Dashboard is the first thin UI layer on top of the Evidence Workbench. It is a static local HTML page generated from existing artifacts, not a separate dashboard product or live app.

Build it from an existing analyzed collection:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_evidence_workbench --collection-dir artifacts/data-collection-live-vegan-protein-bar-15-final-mvp
```

Artifacts include:

- `evidence_workbench/index.html`
- `evidence_workbench/evidence_workbench_manifest.json`

The page is designed for PM and operator review:

- top-level dashboard summary answering the run conclusion, review readiness, strongest evidence, weakest evidence, and PM next step
- evidence-quality panel with image coverage, promo coverage, warning totals, and missing-evidence flags
- market-structure panel comparing primary territories against multi-axis territory coverage
- gap-validation panel showing supported, tentative, and weak candidate counts plus an explicit no-gap explanation when applicable
- compact product evidence matrix for rank, brand, ASIN, territory, secondary territories, rating/reviews, media availability, and warnings
- review-readiness summary with product count, image coverage, promo coverage, warning count, gap state, and territory coverage
- review controls for search, territory filtering, evidence-status filtering, and sorting by rank/reviews/warnings/title
- decision brief headline, recommendation, rationale, and next steps
- gap candidate count and validation score summary
- primary territory counts and multi-axis territory coverage
- selected product cards with packaging images, promotional images, copy, claims, and warnings
- source artifact links back to the JSON evidence

The manifest stores dashboard-level summaries in `dashboard_summary`, `evidence_quality_summary`, `market_structure_summary`, and `product_matrix_summary`, plus the original `review_summary`. This lets automated checks verify whether a run has enough visible evidence for review without scraping the HTML.

Important limitation:

- this is an evidence-review surface, not a polished opportunity dashboard
- the JSON reports remain the source of truth
- the UI should make caveats visible instead of making the output feel more certain than it is

Product detail and product-intelligence records now carry brand-analysis intake fields:

- `media_assets.primary_image`
- `media_assets.gallery_images`
- `media_assets.promotional_images`
- `media_assets.videos`
- `promotional_content`
- `description_bullets`

Replay a stored product-detail snapshot into refreshed detail artifacts:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.extract_product_details --snapshot-id "<amazon_api_product_snapshot_id>" --output-dir artifacts/<run>/details
```

### Discovery Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.discovery_eval --cases eval/fixtures/discovery_golden/batches.json --thresholds eval/thresholds_discovery.json
```

This reports:

- batch run status accuracy
- summary accuracy
- record status accuracy
- required field coverage for valid candidates
- repeat-run stability

### Product Intelligence Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.product_intelligence_eval --cases eval/fixtures/product_intelligence_golden/batches.json --thresholds eval/thresholds_product_intelligence.json
```

This reports:

- merge status accuracy
- summary accuracy
- field provenance accuracy
- landscape claim pattern accuracy
- safety-guard accuracy for contaminated narrative sanitization
- source-failure accuracy for unavailable / no-featured-offer Amazon detail shapes
- repeat-run stability

## Brand Positioning Analysis

The next deterministic layer turns product-intelligence records into brand-positioning summaries using:

- normalized brand identity
- pricing tier within the selected competitor set
- packaging / gallery coverage
- promotional content coverage
- description bullets and title claims

Build brand-positioning artifacts from an existing collection run:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_brand_positioning --collection-dir artifacts/data-collection-live-sugar-3
```

Build directly from a product-intelligence file:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_brand_positioning --product-intelligence-records artifacts/data-collection-live-sugar-3/product_intelligence/product_intelligence_records.json --output-dir artifacts/data-collection-live-sugar-3/brand_positioning
```

Artifacts include:

- `brand_positioning/brand_positioning_records.json`
- `brand_positioning/brand_positioning_report.json`
- `brand_positioning/brand_positioning_report.md`

The current heuristics classify:

- positioning archetype such as `value_staple`, `convenience_bundle`, `health_positioned`, or `pantry_staple`
- signal strength for value, health, and convenience messaging
- a coarse visual strategy based on packaging, promo, and video coverage

Important limitation:

- this layer uses text and media presence/provenance, not image understanding yet
- it helps explain how a product appears to position itself, but it does not yet do semantic vision analysis on packaging artwork

### Brand Positioning Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.brand_analysis_eval --cases eval/fixtures/brand_positioning_golden/batches.json --thresholds eval/thresholds_brand_positioning.json
```

This reports:

- report status accuracy
- brand normalization accuracy
- archetype accuracy
- signal accuracy
- market-theme accuracy
- repeat-run stability

## Brand Profile Sprint

This sprint completes the next deterministic slice after brand positioning:

- visual brand signals from packaging/promo/bullet structure
- brand profile synthesis
- a directional competitor market map

Build the sprint artifacts from an existing collection run:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_brand_profiles --collection-dir artifacts/data-collection-live-sugar-3
```

Artifacts include:

- `brand_profiles/visual_brand_signals_records.json`
- `brand_profiles/brand_profile_records.json`
- `brand_profiles/brand_profile_report.json`
- `brand_profiles/brand_profile_report.md`

The current output answers three practical PM questions:

- what packaging / merchandising format is each brand using
- what audience and value proposition does the product appear to target
- which primary positioning territories are present or missing in the selected set
- which secondary territories a product also covers, so a multi-message product does not create a false gap

The market map is now category-context aware when a collection run includes `selection_report.json`.

Example:

- sugar-like queries keep pantry / beverage-station territory names
- candy-like queries switch to candy-specific territory names such as:
  - `value_multi_pack_candy`
  - `sharing_variety_pack`
  - `premium_indulgence_candy`
  - `mainstream_zero_sugar_candy`
- protein-bar queries switch to protein-bar-specific territory names such as:
  - `clean_plant_protein_bar`
  - `functional_performance_bar`
  - `indulgent_snack_bar`
  - `value_variety_protein_bar`
  - `family_lifestyle_snack_bar`

Important limitation:

- the market map is directional only
- it uses `territory_coverage_counts` to account for primary and secondary territory coverage, but it is not a validated market opportunity layer yet
- secondary territory signals must be specific; broad category words like `protein` or `plant based` should not create coverage by themselves

### Brand Profile Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.brand_profile_eval --cases eval/fixtures/brand_profile_golden/batches.json --thresholds eval/thresholds_brand_profile.json
```

This reports:

- report status accuracy
- visual signal accuracy
- positioning territory accuracy
- pricing stance accuracy
- market-map accuracy
- repeat-run stability

## Gap Validation

This layer turns the directional market map into scored gap candidates using selected-set evidence plus replayable demand signals:

- territory coverage gaps
- missing price-lane gaps
- review/rating traction proxies
- discovery-breadth demand proxies
- price-band realism from the current set

Gap validation now follows the category context produced by the brand-profile layer, so candy runs produce candy-shaped opportunity names instead of pantry-shaped gaps.
It also checks multi-axis territory coverage before creating missing-territory candidates, which reduces false gaps when competitors cover a territory as a secondary message.

Build demand signals from an existing collection run:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_demand_signals --collection-dir artifacts/data-collection-live-sugar-3
```

Build the gap-validation artifacts from an existing collection run:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_gap_validation --collection-dir artifacts/data-collection-live-sugar-3
```

Artifacts include:

- `demand_signals/demand_signal_records.json`
- `demand_signals/demand_signal_report.json`
- `demand_signals/demand_signal_report.md`
- `gap_validation/gap_validation_records.json`
- `gap_validation/gap_validation_report.json`
- `gap_validation/gap_validation_report.md`
- `decision_brief/decision_brief_report.json`
- `decision_brief/decision_brief_report.md`

Important limitation:

- this is still not external demand intelligence
- the validation score uses discovery-breadth terms, review/rating traction, and selected-set price coverage, so it is stronger than raw whitespace detection but weaker than search-volume or conversion validation
- a zero-candidate result can be a valid conservative output when the selected set already covers the target territories; it is not proof the whole market has no opportunity

### Demand Signal Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.demand_signal_eval --cases eval/fixtures/demand_signal_golden/batches.json --thresholds eval/thresholds_demand_signal.json
```

This reports:

- status accuracy
- demand-score accuracy
- top-rank accuracy
- repeat-run stability

### Gap Validation Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.gap_validation_eval --cases eval/fixtures/gap_validation_golden/batches.json --thresholds eval/thresholds_gap_validation.json
```

This reports:

- report status accuracy
- top-candidate accuracy
- supported-candidate count accuracy
- candidate-space accuracy
- repeat-run stability

## Decision Brief

This deterministic layer turns validated gap candidates into a plain-English PM handoff. It does not invent new opportunity logic; it summarizes what the evidence supports and what must be validated next.

Build the decision brief from an existing collection run:

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.build_decision_brief --collection-dir artifacts/data-collection-live-sugar-3
```

Artifacts include:

- `decision_brief/decision_brief_report.json`
- `decision_brief/decision_brief_report.md`

The current recommendation levels are:

- `validate_now`
- `validate_with_caution`
- `research_before_validation`
- `do_not_prioritize_yet`
- `insufficient_evidence`

Important limitation:

- the brief is decision support, not market truth
- a `validate_now` result means "move to concept validation," not "launch this product"
- a `do_not_prioritize_yet` result can mean "no priority gap found in this selected set," not only "the run failed"

### Decision Brief Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.decision_brief_eval --cases eval/fixtures/decision_brief_golden/batches.json --thresholds eval/thresholds_decision_brief.json
```

This reports:

- status accuracy
- recommendation accuracy
- top candidate-space accuracy
- actionability accuracy
- repeat-run stability

## Deep Brand Inference

This schema-bound LLM layer sits after deterministic collection, positioning, and market-map work.

It is designed to:

- synthesize an executive summary from structured upstream artifacts
- produce comparable brand profiles with explicit evidence references
- surface whitespace opportunities and risks without inventing missing facts

Current prompt context includes:

- `product_intelligence/product_intelligence_records.json`
- `landscape/landscape_report.json`
- `brand_positioning/brand_positioning_report.json`
- `brand_profiles/brand_profile_report.json`
- `demand_signals/demand_signal_report.json`
- `gap_validation/gap_validation_report.json`
- `decision_brief/decision_brief_report.json`

Run it from an existing collection directory:

```powershell
$env:PYTHONPATH = "src"
$env:OPENAI_API_KEY = "<your_openai_api_key>"
python -m brand_gap_inference.build_deep_inference --collection-dir artifacts/data-collection-live-sugar-3
```

Artifacts include:

- `deep_inference/deep_brand_inference_report.json`
- `deep_inference/deep_brand_inference_report.md`

Important limitation:

- this is an evidence-first synthesis layer, not autonomous truth
- it is only as good as the structured artifacts passed into it

### Deep Inference Quality Gate

```powershell
$env:PYTHONPATH = "src"
python -m brand_gap_inference.deep_inference_eval --cases eval/fixtures/deep_inference_golden/batches.json --thresholds eval/thresholds_deep_inference.json
```

This reports:

- status accuracy
- brand-profile field accuracy
- whitespace-opportunity accuracy
- caveat accuracy
- repeat-run stability
