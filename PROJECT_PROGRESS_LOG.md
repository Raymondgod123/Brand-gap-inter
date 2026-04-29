# Brand Gap Inference Progress Log

Last updated: 2026-04-29 (Adjacent CPG live validation sprint)

## 2026-04-29 Evidence Workbench v0

- PM decision:
  - the project is ready for a thin evidence-review UI
  - it is not ready for a polished confidence-heavy dashboard
- Implemented the first UI slice as a static local HTML artifact:
  - `evidence_workbench/index.html`
  - `evidence_workbench/evidence_workbench_manifest.json`
- Added a new CLI:
  - `python -m brand_gap_inference.build_evidence_workbench --collection-dir <collection_artifact_dir>`
- Wired the workbench into the deterministic analysis stack after the decision brief.
- The workbench shows:
  - review-readiness summary with product count, image coverage, promo-content coverage, warning count, gap state, and territory coverage
  - review controls for search, territory filtering, evidence-status filtering, and sorting
  - PM decision headline, recommendation, rationale, and next steps
  - gap candidate count and validation score summary
  - primary territory counts
  - multi-axis territory coverage
  - product cards with packaging images, promotional images, description bullets, claims, territories, and warnings
  - source artifact links back to the JSON evidence
- Design principle:
  - this is an evidence-review surface, not a dashboard that pretends the model is final market truth
  - caveats and warnings stay visible
  - JSON artifacts remain the source of truth
- Generated the workbench for the saved `vegan protein bar` MVP run:
  - `artifacts/data-collection-live-vegan-protein-bar-15-final-mvp/evidence_workbench/index.html`
- Latest saved-run review summary:
  - products: `15`
  - primary image coverage: `15/15`
  - gallery image coverage: `15/15`
  - promo-content coverage: `15/15`
  - products with warnings: `15`
  - total warnings: `64`
  - top warning types: `currency missing` (`60`), `promotional content missing` (`2`), `description bullets missing for positioning analysis` (`1`), `possible detail contamination` (`1`)
  - recommendation: `do_not_prioritize_yet`
  - gap candidates: `0`
  - territory coverage: `2` primary territories and `5` primary-plus-secondary territories
- Added regression coverage:
  - writer creates the static review page
  - CLI writes artifacts
  - missing core artifacts produce a partial-success manifest instead of a silent broken UI

## 2026-04-28 multi-axis model-accuracy sprint

- Improved the brand-profile and gap-validation model after the `vegan protein bar` MVP trial exposed a false-gap risk.
- Problem found:
  - the system could assign each product to only one main territory
  - many real products carry more than one positioning signal at once
  - this made the earlier run overstate `value_variety_protein_bar` as a priority gap even though several selected products had variety, sample-pack, count, or value evidence
- What changed:
  - brand profiles now include `secondary_territories`
  - brand-profile reports now include `territory_coverage_counts`
  - gap validation now checks both primary and secondary territory coverage before calling something a missing territory
  - decision briefs now treat a clean zero-gap result as a valid `do_not_prioritize_yet` outcome instead of an evidence failure
  - brand-profile markdown now shows multi-axis territory coverage so PMs can see why a gap was or was not generated
- Accuracy guardrail added:
  - secondary coverage for protein bars no longer gives credit for generic category words like `protein` or `plant based`
  - the model now requires sharper signals, such as organic or clean ingredients, focus or energy, variety or value, or family/on-the-go cues
- Eval guardrail added:
  - decision-brief golden eval now includes a no-priority protein-bar case
  - this checks that the system can correctly say "do not prioritize yet" when the selected set is already well covered
- Rerun result on saved artifact:
  - input artifact: `artifacts/data-collection-live-vegan-protein-bar-15-final-mvp`
  - category context: `protein_bar`
  - selected products: `15`
  - gap-validation candidates: `0`
  - decision brief recommendation: `do_not_prioritize_yet`
  - PM reading: the selected set looks crowded and well-covered; do not spend concept-build effort from this run alone
- Why this matters in plain English:
  - the system is now less likely to invent a gap just because a competitor has one strongest message while also covering other messages
  - the model still stays conservative because a no-gap result is not a claim that the whole market has no opportunity
  - it means the next useful test should use a sharper query or a narrower segment rather than simply collecting more of the same broad category
- Latest local quality-gate run:
  - 12 gates passed
  - 0 gates failed
  - full unit suite: 114 tests passed

## 2026-04-28 vegan protein bar MVP trial

- Ran the next-stage MVP test on keyword:
  - `vegan protein bar`
  - selected products: `15`
  - detail records collected: `15`
  - invalid detail records: `0`
- Pre-run expectation:
  - this would be a mature and crowded category
  - likely clusters would include clean plant protein, performance/function, indulgent snack, value/variety, and lifestyle snack positioning
  - the decision brief would likely be cautious rather than a broad launch signal
- First model output:
  - collection worked end to end
  - the model incorrectly used sugar/pantry-style territories because `protein_bar` was not recognized as a query family
  - decision brief overcalled `premium_pantry_basics`
- Sprint fix:
  - added `protein_bar` query-family selection
  - added protein-bar-specific market-map territories
  - added protein-bar-specific demand signal terms
  - added protein-bar-specific gap validation spaces
  - added regression tests for selector, brand profile, demand signals, and gap validation
- Rerun output after fix:
  - category context is now `protein_bar`
  - decision brief recommendation is `validate_now`
  - top model candidate is `value_variety_protein_bar`
  - model still appears to overstate whitespace because many selected products visibly carry variety or multi-pack evidence while being assigned to a different single primary territory
- PM implication:
  - the pipeline is mechanically strong and now category-aware for protein bars
  - the largest remaining product risk is the single-territory market map
  - next validation improvement should allow multi-tag territory coverage or separate territory vs pack/price/occasion axes before trusting `validate_now`
- Latest local quality-gate run:
  - 12 gates passed
  - 0 gates failed
  - full unit suite: 112 tests passed

## 2026-04-28 deterministic decision-brief sprint

- Added a PM-facing decision-support layer after gap validation.
- New schema:
  - `decision_brief_report`
- New runtime modules:
  - `src/brand_gap_inference/decision_brief.py`
  - `src/brand_gap_inference/build_decision_brief.py`
  - `src/brand_gap_inference/decision_brief_eval.py`
- The decision brief turns the top validated gap candidate into:
  - recommendation level
  - headline
  - executive summary
  - top opportunity metrics
  - decision rationale
  - recommended next steps
  - validation requirements
  - blocked reasons
  - quality warnings
- The brief now enriches adjacent evidence products from product-intelligence records, so PMs can see which real competitor titles, brands, ratings, and review counts support the recommendation.
- Recommendation levels are:
  - `validate_now`
  - `validate_with_caution`
  - `research_before_validation`
  - `do_not_prioritize_yet`
  - `insufficient_evidence`
- Wired `decision_brief` into the deterministic analysis stack after `gap_validation`.
- Deep inference now receives the deterministic decision brief as upstream context.
- Added eval and CI coverage:
  - `python -m brand_gap_inference.decision_brief_eval --cases eval/fixtures/decision_brief_golden/batches.json --thresholds eval/thresholds_decision_brief.json`
- Regenerated saved demo collection analysis with the decision brief:
  - `artifacts/data-collection-live-sugar-3`
  - `artifacts/data-collection-live-zero-calories-candy-15-query-fit-v1`
- Current real-output read:
  - sugar decision brief: `research_before_validation`
  - zero-calorie candy decision brief: `validate_now`
- Latest local quality-gate run:
  - 12 gates passed
  - 0 gates failed
  - full unit suite: 108 tests passed
- PM implication:
  - the pipeline now has a plain-English handoff artifact before any UI work
  - this closes the gap between "scored opportunity" and "what should the team do next?"
  - `validate_now` means move to concept validation, not launch approval

## 2026-04-28 demand-grounded gap validation sprint

- Added the first replayable demand-signal layer.
- New schemas:
  - `demand_signal_record`
  - `demand_signal_report`
- New runtime modules:
  - `src/brand_gap_inference/demand_signals.py`
  - `src/brand_gap_inference/build_demand_signals.py`
  - `src/brand_gap_inference/demand_signal_eval.py`
- Demand signals now use discovery breadth as a bounded proxy:
  - how many valid discovery candidates match a target territory
  - where the best matching rank appears
  - whether matching results include sponsored placements
  - a normalized demand score per territory
- Gap validation now includes `demand_score` in each candidate and blends it with:
  - supply gap score
  - selected-set traction
  - price realism
- The analysis stack now runs `demand_signals` before `gap_validation`.
- Deep inference prompt context now includes the demand-signal report.
- Added eval and CI coverage:
  - `python -m brand_gap_inference.demand_signal_eval --cases eval/fixtures/demand_signal_golden/batches.json --thresholds eval/thresholds_demand_signal.json`
- Added a one-command local quality gate runner:
  - `python -m brand_gap_inference.quality_gates`
  - writes `artifacts/quality-gates-latest.json`
  - mirrors the CI gate sequence so handoff checks are easier to reproduce locally
- Latest local quality-gate run:
  - 12 gates passed
  - 0 gates failed
- Regenerated saved demo collection analysis with the new demand layer:
  - `artifacts/data-collection-live-sugar-3`
  - `artifacts/data-collection-live-zero-calories-candy-15-query-fit-v1`
- Current real-output read:
  - sugar stays cautious: no supported gap crossed the threshold
  - zero-calorie candy surfaces supported candidates where demand breadth, adjacent traction, and supply gap align
- Local validation completed:
  - full unit suite: 108 tests passed
  - phase1 eval gate: passed
  - discovery eval: passed
  - taxonomy eval: passed
  - normalization eval: passed
  - product intelligence eval: passed
  - brand positioning eval: passed
  - brand profile eval: passed
  - demand signal eval: passed
  - gap validation eval: passed
  - deep inference eval: passed
- PM implication:
  - gap validation is no longer purely selected-set whitespace plus reviews
  - this is still not true search-volume or conversion data, but it gives us a replayable external-market signal before ranking opportunities

## 2026-04-25 post-collection analysis stack sprint

- Added a bounded post-collection orchestrator in `src/brand_gap_inference/analysis_stack.py`.
- The new runner now executes, in order:
  - `landscape`
  - `brand_positioning`
  - `brand_profiles`
  - `demand_signals`
  - `gap_validation`
  - optional `deep_inference`
- Added a new schema-bound summary contract:
  - `analysis_stack_report`
- Added a replay-safe CLI for saved collections:
  - `python -m brand_gap_inference.analyze_collection --collection-dir <artifact_dir>`
- Extended `collect_data` with an opt-in post-analysis mode:
  - `--post-analysis deterministic`
  - `--post-analysis deep_inference`
- The new analysis stack writes:
  - `analysis_stack_report.json`
  - `analysis_stack_report.md`
  - `analysis_stack_bundle_manifest.json`
- Added regression coverage for:
  - deterministic stack success
  - optional deep inference success
  - optional deep inference failure while preserving deterministic outputs
  - one-command collection plus analysis
- PM implication:
  - the analysis pipeline is now reproducible as one bounded slice instead of a manual chain of separate commands
  - this makes saved collection runs easier to replay, debug, and hand off without losing artifact traceability

## 2026-04-25 CI gate alignment cleanup

- Read the CI/eval-team brief and tightened the workflow to match the current stack.
- Renamed the GitHub Actions workflow from phase-1-only wording to:
  - `analysis-stack-quality-gates`
- Added the missing `discovery_eval` step to the workflow.
- The workflow now covers:
  - phase1 contract gate
  - discovery
  - taxonomy
  - normalization
  - product intelligence
  - brand positioning
  - brand profile
  - gap validation
  - deep inference
- PM implication:
  - CI naming now matches the real product scope
  - discovery is no longer the only threshold-gated layer left out of the workflow
  - this keeps the “every important module must be measurable and gated” principle consistent end to end

## 2026-04-25 deep inference eval gate

- Added a formal eval harness for the schema-bound LLM synthesis layer.
- New runtime module:
  - `src/brand_gap_inference/deep_inference_eval.py`
- New golden fixture and thresholds:
  - `eval/fixtures/deep_inference_golden/batches.json`
  - `eval/thresholds_deep_inference.json`
- New regression test:
  - `tests/test_deep_inference_eval.py`
- The new gate uses a deterministic fixture client so we can measure the deep-inference orchestration layer without making live API calls.
- The deep-inference layer itself now reads the current deterministic stack, not just the older subset:
  - `product_intelligence`
  - `landscape`
  - `brand_positioning`
  - `brand_profiles`
  - `gap_validation`
- The gate now checks:
  - status accuracy
  - brand-profile field accuracy
  - whitespace-opportunity accuracy
  - caveat accuracy
  - repeat-run stability
- PM implication:
  - the GPT-assisted step is no longer the only non-measured layer in the pipeline
  - this keeps the architecture aligned with the original principle that LLM reasoning must also be eval-gated

## 2026-04-25 product-intelligence contamination guard

- Added a merge-layer safety guard in `src/brand_gap_inference/product_intelligence.py` for cross-product detail contamination.
- The guard now compares the dominant content family implied by:
  - product title
  - detail bullets
  - promotional content
- When detail narrative strongly conflicts with the title family, the merger now:
  - preserves hard facts and media assets
  - drops contaminated `description_bullets`
  - drops contaminated `promotional_content`
  - adds an explicit warning for support and debug review
- Live validation on the saved `zero calories candy` run confirmed the fix on `B0G1VDN772`:
  - the bad syrup-style bullets and promo blocks were removed
  - images, title, price, rating, and availability were preserved
  - downstream positioning and profile output now rely on safe evidence only
- Strengthened eval gating:
  - added a new product-intelligence golden batch with candy-vs-syrup contamination
  - added `product_intelligence_sanitization_accuracy` to the eval thresholds
- Validation status:
  - full unit suite passed
  - `product_intelligence_eval` passed
  - `brand_profile_eval` passed
  - `gap_validation_eval` passed
- PM implication:
  - the pipeline is getting better at protecting brand analysis from believable but wrong upstream narrative content
  - this keeps trust rails intact without throwing away useful packaging and marketplace evidence

## 2026-04-25 category-aware candy market map

- Added category-context awareness to the brand-profile and gap-validation layers.
- The brand-profile layer now reads query family context from collection artifacts and can emit category-specific territory names.
- For candy-shaped runs, the market map now uses:
  - `value_multi_pack_candy`
  - `sharing_variety_pack`
  - `premium_indulgence_candy`
  - `mainstream_zero_sugar_candy`
  - `health_forward_alternative`
- Gap validation now follows the same context, so its candidates no longer default to pantry-era territory names on candy sets.
- Added regression coverage:
  - `tests.test_brand_profile` now checks candy-context territory mapping
  - `tests.test_gap_validation` now checks candy-context gap generation
- Validation status:
  - targeted brand-profile tests passed
  - targeted gap-validation tests passed
  - full unit suite passed
  - `brand_profile_eval` passed
  - `gap_validation_eval` passed
- Live `zero calories candy` rerun after these updates:
  - brand-profile report now shows a candy-specific market map instead of pantry/beverage naming
  - top supported gap is now `premium_indulgence_candy`
  - tentative lanes now include:
    - `value_multi_pack_candy x premium_pantry`
    - `sharing_variety_pack x budget_anchor`
    - `mainstream_zero_sugar_candy x budget_anchor`
- PM implication:
  - the pipeline is now better aligned from selection through opportunity naming on candy-shaped searches
  - the next bottleneck is not category naming anymore; it is refining the underlying territory heuristics and adding broader demand grounding

## 2026-04-25 query-fit selector hardening

- Added a deterministic query-fit selection layer ahead of product-detail collection.
- New runtime module:
  - `src/brand_gap_inference/candidate_selection.py`
- The selector now:
  - identifies query family hints such as `candy`, `sweetener`, and `sugar`
  - prefers records with direct category-format evidence
  - filters obvious adjacent-category results into a non-preferred pool
  - preserves provider rank within the preferred pool for stability
  - emits operator-facing `selection_trace` metadata on selected candidates
  - writes `selection_report.json` for audit and debugging
- Added regression coverage in `tests/test_data_collection.py` for:
  - same-category selection over adjacent wellness results
  - selection-report artifact generation
  - no unnecessary backfill when enough preferred candidates exist
- Validation status:
  - `py -m unittest tests.test_data_collection -v` passed
  - full unit suite passed
  - `discovery_eval` passed
  - `product_intelligence_eval` passed
  - `brand_profile_eval` passed
- Live rerun on `zero calories candy` with `15` selected products:
  - detail enrichment again succeeded for `15/15`
  - obvious adjacent results like fruit snacks and the zero-sugar energy drink were removed from the selected set
  - they were replaced by more direct candy products such as Dr. John's hard candy, Zaffi Taffy, Sour Drops, and Russell Stover caramel chocolate candy
- PM implication:
  - the current bottleneck moved from discovery precision into downstream category interpretation
  - the next useful improvement is to tighten brand-profile territory logic for candy-specific sets, so cleaner selection produces a cleaner market map

## 2026-04-25 zero calories candy live run

- Ran a live end-to-end test on keyword `zero calories candy` with `15` selected products.
- The pipeline completed successfully through:
  - discovery
  - product detail enrichment
  - brand positioning
  - brand profiles
  - gap validation
- Collection result:
  - discovery returned `60` valid candidates
  - selected set size was `15`
  - detail enrichment succeeded for `15/15`
  - invalid detail records: `0`
- Pre-run expectation:
  - the set would skew toward sugar-free and zero-sugar candy
  - detail capture would succeed for most or all products
  - health-forward positioning would likely look crowded rather than missing
- Actual result:
  - detail capture fully succeeded
  - health-forward territory did appear crowded
  - but discovery quality was noisier than expected, with adjacent snack and beverage products appearing in the selected set
- Brand/market-map output for the selected set:
  - `health_forward_alternative`: `5`
  - `convenience_beverage_station`: `5`
  - `general_household`: `5`
- Gap-validation output:
  - supported candidate: `premium_pantry_basics`
  - supported candidate: `value_pantry_basics`
- PM implication:
  - the pipeline reliability is now proven on a larger live run
  - the next bottleneck is discovery precision and category-fit filtering, not downstream enrichment or reporting

## 2026-04-25 gap validation layer update

- Added the next post-market-map layer: directional gap validation.
- Added new contracts:
  - `gap_validation_record`
  - `gap_validation_report`
- Added runtime modules:
  - `src/brand_gap_inference/gap_validation.py`
  - `src/brand_gap_inference/build_gap_validation.py`
  - `src/brand_gap_inference/gap_validation_eval.py`
- The new output now scores gap candidates using:
  - territory absence or missing price-lane coverage
  - review/rating traction proxies from the selected set
  - price-band realism from the selected set
- Added threshold-gated eval coverage:
  - `python -m brand_gap_inference.gap_validation_eval --cases eval/fixtures/gap_validation_golden/batches.json --thresholds eval/thresholds_gap_validation.json`
- The gate checks:
  - report status accuracy
  - top-candidate accuracy
  - supported-candidate count accuracy
  - candidate-space accuracy
  - repeat-run stability
- Generated gap-validation artifacts for the saved sugar run under:
  - `artifacts/data-collection-live-sugar-3/gap_validation/`

Validation status:

- `py -m unittest tests.test_gap_validation tests.test_gap_validation_eval -v` passed
- `py -m brand_gap_inference.gap_validation_eval --cases eval/fixtures/gap_validation_golden/batches.json --thresholds eval/thresholds_gap_validation.json` passed

PM implication:

- The pipeline now distinguishes between:
  - directional whitespace from the market map
  - higher-priority gap candidates with basic evidence-based scoring
- This is still not true external-demand validation, but it is the right bridge before adding broader demand sources.

## 2026-04-25 brand profile sprint update

- Completed the next deterministic brand-analysis sprint after brand positioning.
- Added a new combined layer for:
  - visual brand signals
  - brand profile synthesis
  - directional competitor market map output
- Added new contracts:
  - `visual_brand_signals_record`
  - `brand_profile_record`
  - `brand_profile_report`
- Added runtime modules:
  - `src/brand_gap_inference/brand_profile.py`
  - `src/brand_gap_inference/build_brand_profiles.py`
  - `src/brand_gap_inference/brand_profile_eval.py`
- The new output turns existing collection artifacts into:
  - package format and configuration
  - promotional stack and message architecture
  - target audience and value proposition
  - pricing stance and tone of voice
  - territory counts plus underrepresented-space notes for the selected set
- Added threshold-gated eval coverage:
  - `python -m brand_gap_inference.brand_profile_eval --cases eval/fixtures/brand_profile_golden/batches.json --thresholds eval/thresholds_brand_profile.json`
- The gate checks:
  - report status accuracy
  - visual signal accuracy
  - positioning territory accuracy
  - pricing stance accuracy
  - market-map accuracy
  - repeat-run stability
- Generated brand-profile sprint artifacts for the saved sugar run under:
  - `artifacts/data-collection-live-sugar-3/brand_profiles/`

Validation status:

- `py -m unittest tests.test_brand_profile tests.test_brand_profile_eval -v` passed
- `py -m brand_gap_inference.brand_profile_eval --cases eval/fixtures/brand_profile_golden/batches.json --thresholds eval/thresholds_brand_profile.json` passed

PM implication:

- The pipeline can now move from collected brand assets to a structured market map, not just product summaries.
- This is the right bridge before any future whitespace or gap inference work.
- The output is still evidence-first and directional; it should not yet be treated as validated opportunity scoring.

## 2026-04-24 brand positioning layer update

- Added the first deterministic brand-positioning layer on top of product-intelligence records.
- Added new contracts:
  - `brand_positioning_record`
  - `brand_positioning_report`
- Added analyzer module:
  - `src/brand_gap_inference/brand_analysis.py`
- Added CLI:
  - `python -m brand_gap_inference.build_brand_positioning --collection-dir <collection_artifact_dir>`
- Current brand-positioning output includes:
  - normalized brand name
  - price tier within the selected set
  - inferred positioning archetype
  - value / health / convenience signal strength
  - coarse visual strategy based on packaging, promotional content, and video coverage
  - evidence snippets and caveats
- Added threshold-gated eval coverage:
  - `python -m brand_gap_inference.brand_analysis_eval --cases eval/fixtures/brand_positioning_golden/batches.json --thresholds eval/thresholds_brand_positioning.json`
- The gate checks:
  - report status accuracy
  - brand normalization accuracy
  - archetype accuracy
  - signal accuracy
  - market-theme accuracy
  - repeat-run stability
- Generated a replay-safe brand-positioning demo for the existing sugar collection artifacts under:
  - `artifacts/data-collection-live-sugar-3/brand_positioning/`

Validation status:

- `py -m unittest tests.test_brand_analysis tests.test_brand_analysis_eval -v` passed
- `py -m brand_gap_inference.brand_analysis_eval --cases eval/fixtures/brand_positioning_golden/batches.json --thresholds eval/thresholds_brand_positioning.json` passed

PM implication:

- The pipeline can now move from raw competitive collection into early brand-positioning inference using the exact inputs PM highlighted:
  - packaging
  - promotional content
  - description / claims
- This is still an evidence-backed heuristic layer, not full image-semantic brand reasoning yet.

## 2026-04-24 brand-analysis intake fields update

- Promoted brand-analysis inputs into product detail and product-intelligence records.
- Added extraction for:
  - packaging / product gallery images from `product_results.thumbnail` and `product_results.thumbnails`
  - promotional / A+ content images and copy from `product_description`
  - product description bullets from `about_item`
  - video links and video thumbnails from `videos`
- Added product-intelligence carry-forward for:
  - `media_assets`
  - `promotional_content`
  - `description_bullets`
- Updated landscape report competitor table to show media, bullet, and promotional content counts.
- Added replay CLI for product detail extraction:
  - `python -m brand_gap_inference.extract_product_details --snapshot-id <amazon_api_product_snapshot_id> --output-dir <details_output_dir>`
- Regenerated the sugar demo artifacts from stored raw SerpApi product snapshots without spending new API calls.
- Sugar demo brand-analysis coverage:
  - `B0D1L1KSMZ`: 6 gallery images, 2 promotional blocks, 6 bullets, 4 videos
  - `B0FZXW7LVY`: 8 gallery images, 0 promotional blocks, 6 bullets, 2 videos
  - `B09RPPBG15`: 6 gallery images, 0 promotional blocks, 5 bullets, 0 videos

Validation status:

- `py -m unittest tests.test_data_collection tests.test_product_intelligence_eval tests.test_landscape_report -v` passed (14/14)
- `py -m brand_gap_inference.product_intelligence_eval --cases eval/fixtures/product_intelligence_golden/batches.json --thresholds eval/thresholds_product_intelligence.json` passed

PM implication:

- The pipeline now collects the core brand-analysis inputs needed for positioning inference:
  - packaging
  - promotional imagery
  - product description / claims
- The next step can be brand-positioning analysis over these collected fields instead of trying to infer positioning from title, price, and rating alone.

## 2026-04-24 sugar keyword data collection demo

- Ran a live data collection demo for:
  - keyword: `sugar`
  - max selected products: `3`
  - output: `artifacts/data-collection-live-sugar-3/`
- Result:
  - run status: `success`
  - discovery candidates: `60`
  - valid discovery candidates: `60`
  - selected candidates: `3`
  - product detail records collected: `3`
  - valid product detail records: `3`
  - product-intelligence records: `3`
  - product-intelligence issues: `0`
- Selected products:
  - rank 1: `B0D1L1KSMZ` - Amazon Saver, White Sugar, 4 Lb
  - rank 2: `B0FZXW7LVY` - Sweetmo Sugar Packets Variety Pack
  - rank 3: `B09RPPBG15` - Domino Granulated Sugar, 20 oz Canister, Pack of 3
- Generated landscape report:
  - `artifacts/data-collection-live-sugar-3/landscape/landscape_report.md`
  - `artifacts/data-collection-live-sugar-3/landscape/landscape_report.json`
- Caveat:
  - SerpApi product detail records omitted currency for all three products, so price comparisons are useful for demo inspection but not final decision use.
- During this demo, fixed a duplicate-ASIN merge issue:
  - when full discovery output contained later duplicate ASINs, product intelligence could inherit the later rank instead of the selected candidate rank
  - the merger now preserves selected-candidate context while still using full discovery/detail records for provenance
  - added regression coverage for this case

Validation status:

- `py -m unittest tests.test_data_collection -v` passed (10/10)
- `py -m brand_gap_inference.product_intelligence_eval --cases eval/fixtures/product_intelligence_golden/batches.json --thresholds eval/thresholds_product_intelligence.json` passed

## 2026-04-24 product intelligence eval gate update

- Added a threshold-gated eval for the new product-intelligence and landscape layer.
- Added golden fixture:
  - `eval/fixtures/product_intelligence_golden/batches.json`
- Added thresholds:
  - `eval/thresholds_product_intelligence.json`
- Added eval CLI:
  - `python -m brand_gap_inference.product_intelligence_eval --cases eval/fixtures/product_intelligence_golden/batches.json --thresholds eval/thresholds_product_intelligence.json`
- The gate checks:
  - product-intelligence merge status accuracy
  - summary accuracy
  - field provenance accuracy
  - landscape claim pattern accuracy
  - repeat-run stability
- Added unit coverage:
  - `tests/test_product_intelligence_eval.py`

Validation status:

- `py -m unittest tests.test_product_intelligence_eval -v` passed (1/1)
- `py -m brand_gap_inference.product_intelligence_eval --cases eval/fixtures/product_intelligence_golden/batches.json --thresholds eval/thresholds_product_intelligence.json` passed

PM implication:

- The newest transition layers now follow the original project rule: no important module is accepted without measurable thresholds.

## 2026-04-24 lightweight landscape report update

- Added the first deterministic landscape report over product-intelligence records.
- Added contract:
  - `landscape_report`
- Added CLI:
  - `python -m brand_gap_inference.build_landscape_report --collection-dir <collection_artifact_dir>`
- The report currently includes:
  - competitor table
  - price ladder
  - rating ladder
  - review ladder
  - repeated title/claim patterns
  - caveats for missing fields
- Generated a live monk fruit landscape report:
  - `artifacts/data-collection-live-monk-fruit-3/landscape/landscape_report.json`
  - `artifacts/data-collection-live-monk-fruit-3/landscape/landscape_report.md`
- Live report result:
  - products: `3`
  - status: `partial_success`
  - reason for partial status: missing currency for 3 products and missing brand for 2 products
- Repeated title/claim patterns detected in the selected set:
  - `keto`
  - `non gmo`
  - `zero calorie`
  - `low carb`
  - `gluten free`
  - `baking`
  - `coffee`
  - `tea`
  - `sugar substitute`

Validation status:

- `py -m unittest tests.test_landscape_report -v` passed (3/3)
- `py -m unittest discover -s tests -p "test_*.py"` passed (70/70)

PM implication:

- The project can now go from keyword to a stakeholder-readable competitive landscape artifact without depending on fragile raw Amazon HTML as the primary input.
- The output is intentionally conservative: it reports caveats instead of hiding missing provider fields.

## 2026-04-24 product intelligence merge update

- Added the first merged product-intelligence layer on top of collection artifacts.
- Added contracts:
  - `product_intelligence_record`
  - `product_intelligence_batch_report`
- Added deterministic merge behavior:
  - selected discovery candidates define the product set
  - product detail fields are preferred when available
  - discovery fields are used as fallback
  - every merged field records provenance with source, record id, and raw payload URI
- Added merge artifacts:
  - `product_intelligence/product_intelligence_records.json`
  - `product_intelligence/product_intelligence_report.json`
- Added replay-style CLI:
  - `python -m brand_gap_inference.merge_product_intelligence --collection-dir <collection_artifact_dir>`
- Updated `collect_data` so future successful detail-enabled collection runs also emit product-intelligence artifacts automatically.
- Regenerated product-intelligence artifacts for the live monk fruit run:
  - output: `artifacts/data-collection-live-monk-fruit-3/product_intelligence/`
  - merged products: `3`
  - complete products: `3`
  - issue products: `0`

Validation status:

- `py -m unittest tests.test_data_collection -v` passed (9/9)

PM implication:

- The project now has the bridge from raw collection to structured product intelligence.
- The next meaningful product step is a lightweight landscape report over product-intelligence records: competitor table, price/rating/review ladder, claim/title pattern summary, and early whitespace notes.

## 2026-04-24 live data collection run

- Ran the new one-shot collection command against live SerpApi:
  - keyword: `monk fruit sweetener`
  - max selected products: `3`
  - output: `artifacts/data-collection-live-monk-fruit-3/`
- Result:
  - run status: `success`
  - discovery candidates: `48`
  - valid discovery candidates: `48`
  - selected candidates: `3`
  - product detail records collected: `3`
  - valid product detail records: `3`
  - invalid product detail records: `0`
- Selected ASINs:
  - `B08CC1FMJQ`
  - `B077SPTP4Z`
  - `B0CFSL5L2M`
- Detail record warnings were non-fatal:
  - currency missing from product detail response
  - brand missing from two product detail responses
- The run produced:
  - `selected_candidates.json`
  - `data_collection_report.json`
  - `data_collection_bundle_manifest.json`
  - `details/product_detail_records.json`
  - `details/product_detail_report.json`

PM implication:

- The new data collection layer has now been exercised live, not only through fixtures.
- The transition from keyword discovery to selected ASIN detail enrichment is operational.
- The next build step should be a merge/product-intelligence record that combines discovery breadth and product detail enrichment while preserving field-level provenance.

## 2026-04-24 one-shot data collection layer update

- Added a one-command data collection path:
  - `python -m brand_gap_inference.collect_data --keyword "<keyword>" --max-products 5`
- The new flow runs:
  - keyword discovery through the existing SerpApi discovery lane
  - deterministic top-valid-candidate selection
  - optional product-detail collection by ASIN through SerpApi's Amazon Product API
- Added a new raw source family:
  - `amazon_api_product`
- Added `SerpApiProductConnector` for ASIN-based product detail snapshots.
- Product detail collection stores one raw record per selected ASIN under a shared detail snapshot.
- Added deterministic product-detail extraction into validated records with:
  - title
  - brand
  - product URL
  - price
  - currency
  - rating
  - review count
  - availability
  - provider metadata
  - warnings and issues
- Added new contracts:
  - `product_detail_record`
  - `data_collection_report`
- Added collection artifacts:
  - `selected_candidates.json`
  - `details/product_detail_records.json`
  - `details/product_detail_report.json`
  - `data_collection_report.json`
  - `data_collection_bundle_manifest.json`
- Added replay-friendly operation:
  - `--discovery-snapshot-id <snapshot_id>`
  - `--detail-mode none` for offline downstream testing
- Kept the boundary explicit:
  - no change to current MVP replay path
  - no change to browser capture path
  - no change to normalization/taxonomy registration
  - no merged product-intelligence record yet

Validation status:

- `py -m unittest tests.test_data_collection -v` passed (7/7)

PM implication:

- The project now has a practical data collection layer that can move from keyword to candidate list to selected ASIN detail snapshots.
- This completes the next transition step from "discovery only" toward breadth plus selective enrichment while keeping replay and auditability intact.

## 2026-04-24 SERP API discovery ingestion update

- Implemented the first transition slice from URL-first acquisition toward keyword-first discovery.
- Added a new raw source family:
  - `amazon_api_discovery`
- Added `SerpApiDiscoveryConnector` for keyword-based Amazon product discovery.
- Live discovery now stores one raw provider-response snapshot per keyword query instead of trying to fetch and analyze arbitrary Amazon product pages as the backbone.
- The raw discovery payload now stores:
  - `provider`
  - `query`
  - `requested_at`
  - `provider_request_metadata`
  - full `provider_response`
  - `result_count`
- Added a deterministic extraction layer that converts replayable raw discovery snapshots into structured candidate records.
- Added new contracts:
  - `discovery_result_record`
  - `discovery_batch_report`
- Discovery extraction behavior in v1:
  - valid minimum fields are `title` and `product_url`
  - missing `asin`, `price`, `currency`, `rating`, or `review_count` become warnings
  - missing title or valid Amazon product URL makes the candidate invalid
  - ASIN falls back to extraction from Amazon product URLs when provider output omits it
- Added new CLI:
  - `python -m brand_gap_inference.discover_products --keyword "<keyword>"`
  - `python -m brand_gap_inference.discover_products --snapshot-id "<snapshot_id>"`
- Added predictable discovery artifact bundle output:
  - `discovery_records.json`
  - `discovery_report.json`
  - `discovery_bundle_manifest.json`
- Kept the boundary explicit:
  - no change to replay MVP path
  - no change to browser spike path
  - no change to normalization/taxonomy runtime registration
  - no merged product-intelligence record yet
- Added discovery golden eval coverage and thresholds with replay-safe fixtures for:
  - batch run status accuracy
  - summary accuracy
  - record status accuracy
  - required field coverage
  - repeat-run stability

Validation status:

- `py -m unittest discover -s tests -p "test_*.py"` passed (58/58)
- `py -m brand_gap_inference.eval_runner --fixtures-dir eval/fixtures/phase1 --thresholds eval/thresholds.json` passed
- `py -m brand_gap_inference.normalization_eval --cases eval/fixtures/normalization_golden/batches.json --thresholds eval/normalization_thresholds.json` passed
- `py -m brand_gap_inference.taxonomy_eval --cases eval/fixtures/taxonomy_golden/cases.json --thresholds eval/taxonomy_thresholds.json` passed
- `py -m brand_gap_inference.discovery_eval --cases eval/fixtures/discovery_golden/batches.json --thresholds eval/thresholds_discovery.json` passed

PM implication:

- The project now has a second trustworthy acquisition lane that is keyword-first and replayable.
- This is the first concrete repo step toward the transition plan:
  - breadth discovery first
  - selective detail capture later
- The downstream trust rails are preserved while the weakest live-acquisition dependency is being de-emphasized.

## 2026-04-24 browser acquisition spike update

- Implemented a new browser-assisted live Amazon acquisition path without replacing the current HTTP path or replay path.
- Added explicit live CLI selection:
  - `python -m brand_gap_inference.mvp_run --url <amazon_url> --acquisition-mode browser`
  - `python -m brand_gap_inference.amazon_ingest --url <amazon_url> --acquisition-mode browser`
- Added a bundled-runtime Playwright bridge:
  - new Python runner that invokes Node Playwright through the Codex bundled Node runtime
  - new minimal Node capture script that opens one URL, captures rendered HTML, and returns structured diagnostics
- Added `AmazonBrowserProductConnector` while keeping downstream source as `amazon`, so normalization and taxonomy remain unchanged.
- Browser-captured raw payloads now include:
  - `acquisition_method: browser_playwright`
  - `browser_engine: chromium`
  - rendered `html`
  - `capture_diagnostics` with navigation status, ready state, wait strategy, timing, and visible offer-state signals
- Kept trust behavior unchanged after acquisition:
  - no new unsafe price fallbacks
  - same normalization safe-stop behavior when critical price data is still unsafe
  - same artifact contract for success and failure bundles
- Added regression coverage for:
  - browser connector raw-record output
  - browser-mode MVP success bundle
  - browser-mode MVP safe-stop failure bundle
  - browser-captured normalization path through unit tests and normalization golden eval fixtures
- Extended safe primary-price parsing so `priceToPay` / `priceToPay_feature_div` are treated as primary containers, which improves compatibility with rendered browser HTML while keeping scoped price behavior.

Validation status:

- `py -m unittest discover -s tests -p "test_*.py"` passed (52/52)
- `py -m brand_gap_inference.eval_runner --fixtures-dir eval/fixtures/phase1 --thresholds eval/thresholds.json` passed
- `py -m brand_gap_inference.normalization_eval --cases eval/fixtures/normalization_golden/batches.json --thresholds eval/normalization_thresholds.json` passed
- `py -m brand_gap_inference.taxonomy_eval --cases eval/fixtures/taxonomy_golden/cases.json --thresholds eval/taxonomy_thresholds.json` passed
- `py -m brand_gap_inference.mvp_release_check --output-dir artifacts/mvp-release-check-browser-spike` passed

Live browser reality check:

- Browser capture is now operational end to end in this environment once run unsandboxed; raw snapshots were captured successfully with `browser_playwright` metadata and diagnostics.
- Manual browser-mode MVP runs were exercised against:
  - `B098H7XWQ6`
  - `B01LDNBAC4`
  - `B014RVNVKS`
  - `B000EA2D9C`
- Result: all tested live URLs still safe-stopped at normalization with the same core reason:
  - `missing product price: no featured offers available on page; buying-options only`
- A grouped browser fallback run was recorded under:
  - `artifacts/mvp-browser-live-fallback-spike/`
- PM implication:
  - browser acquisition fixed the capture/control gap
  - it did not yet convert the current candidate set into a successful end-to-end live MVP
  - the remaining blocker is now marketplace offer visibility in this session/region, not lack of browser rendering support

## 2026-04-24 sprint directive execution update

- Read and executed the `SPRINT_1WEEK_EXECUTION_PLAN.md` directive against current repo state.
- Locked the MVP command behavior to predictable default output folders by mode:
  - live single run: `artifacts/mvp-live-<timestamp>`
  - replay single run: `artifacts/mvp-replay-<snapshot_id>`
  - live fallback run: `artifacts/mvp-live-fallback-<timestamp>`
  - replay fallback run: `artifacts/mvp-replay-fallback-<timestamp>`
- Added run-level `mvp_bundle_manifest.json` output for single-run success and single-run safe-stop failure bundles.
- Upgraded failure `mvp_report.md` wording for operators and stakeholders:
  - explicit `SAFE STOP` outcome
  - clearer failure reason
  - explicit trust rationale
  - artifact bundle listing
- Added fallback summary artifacts (`fallback_attempts.json` and `fallback_report.md`) for both success and failure paths.
- Added curated failure replay fixture list at `fixtures/mvp/demo_snapshot_failure_ids.txt` for stakeholder trust demos.
- Updated README with copy-paste stakeholder demo commands for:
  - replay success
  - replay safe-stop failure
  - live attempt
  - artifact review
- Added one-command release rehearsal CLI:
  - `python -m brand_gap_inference.mvp_release_check`
  - validates replay success path + replay safe-stop path and emits a pass/fail readiness summary
- Expanded MVP tests to verify:
  - bundle manifest emission
  - safe-stop failure report wording
  - fallback summary artifact emission on success and failure
- Added explicit buying-options-only failure coverage to normalization tests and normalization golden eval fixtures.
- Added safety override so singleton unscoped structured-price fallback is blocked when offer-missing signals are present (`No featured offers` / `See All Buying Options`).
- Added key-field provenance snapshot directly into `mvp_report.md` so non-engineers can review parsing origin without opening raw JSON artifacts.

Release rehearsal status:

- `python -m brand_gap_inference.mvp_release_check --output-dir artifacts/mvp-release-check-directive-2` is passing.
- Replay success path and replay safe-stop path both validated in one run.

## 2026-04-24 option 1 hardening continuation

- Primary container price extraction is now restricted to real HTML container tags and explicitly skips script/style blocks.
- Legacy price-block regex fallback now also skips script/style matches, so script-only widget snippets cannot be treated as primary listing price.
- This closes a trust risk where script metadata strings (for example `divToUpdate` lists or encoded widget HTML) could be mistaken for true primary price containers.
- Added a regression test that simulates script-injected widget price markup on a no-featured-offers page and verifies the run still fails safely.
- Expanded normalization golden eval coverage with `script-widget-price-trap-1` so this safety behavior is now gate-protected.
- Revalidated behavior with replay runs:
  - known good snapshot (`amazon-CLEAN00001-2026-04-24T00-00-00Z`) still succeeds end to end
  - known offer-missing snapshot (`amazon-B098H7XWQ6-2026-04-24T01-23-08Z`) still fails clearly with explicit no-featured-offers context

## 2026-04-24 hardening update

- Primary-price extraction was expanded to parse additional safe markup shapes inside primary price containers (including `a-price-whole` + `a-price-fraction` patterns).
- Safety behavior remains unchanged: the system still avoids unscoped widget/recommendation prices and fails clearly when a safe primary price cannot be established.
- New regression coverage was added for dynamic-style primary container price markup.
- MVP runner now supports candidate URL fallback via `--urls-file`, trying URLs in order and selecting first success.
- Fallback failure output now includes structured attempt history so operators can see which URLs failed and why.

Current operational blocker:

- Live Amazon validation from this environment is currently blocked by network refusal (`WinError 10061`).
- Because of that, we can validate logic, tests, and eval gates locally, but we cannot complete a true live-source reliability measurement run until connectivity is available.

## 2026-04-24 live fallback execution update

- Network access was enabled and verified.
- MVP fallback mode was exercised live against a discovered candidate set (10 Amazon URLs).
- All candidates failed normalization at the same product-supply boundary:
  - `missing product price: no featured offers available on page; buying-options only`
- Structured attempt history artifacts were produced for each failed URL under `artifacts/mvp-live-candidates-run/attempt-*`.

PM-level implication:

- This is now a marketplace/offer-availability constraint in our current region/session context, not just parser coverage.
- Continuing to widen regex coverage will not solve this class of failures reliably because primary featured price is absent.

## 2026-04-24 predictable MVP execution update

- Added MVP replay mode from stored snapshots via `mvp_run --snapshot-id ... --source ...`.
- This provides a reliable, audited demo path without weakening live-source safety policy.
- Live mode remains strict:
  - unsafe pages still fail clearly
  - no fallback to unscoped widget/recommendation prices

PM alignment:

- This matches the memo direction to prioritize predictable MVP reliability over trying to force all live URLs to succeed.

Execution proof:

- Replay-mode MVP run succeeded for snapshot `amazon-CLEAN00001-2026-04-24T00-00-00Z`.
- Output artifacts were generated under `artifacts/mvp-replay-demo/`, including `mvp_report.md`, normalization artifacts, taxonomy artifacts, and `opportunities.json`.

## 2026-04-24 continued progress update

- Added replay fallback mode via `mvp_run --snapshot-ids-file ... --source ...`, so the runner can try multiple audited snapshots and stop at first success.
- Added structured attempt reporting for replay fallback, mirroring live URL fallback behavior.
- Expanded normalization golden eval coverage with a second batch that validates:
  - safe parsing from primary whole/fraction price markup
  - explicit invalid behavior for `no featured offers available` pages with unscoped structured prices
- Expanded taxonomy golden eval coverage with a new non-keto baking sweetener case to strengthen audience-axis expectations.

Current quality status:

- Unit tests are passing.
- `eval_runner`, `normalization_eval`, and `taxonomy_eval` remain passing after these additions.

## What this project is trying to do

This project is meant to turn messy marketplace data into trustworthy business insight.

In simple terms:

1. Pull raw product data from sources like Amazon
2. Save the raw data so we can always audit what happened
3. Clean and standardize the data
4. Classify products into a stable taxonomy
5. Add demand, behavior, and brand signals
6. Generate market opportunities
7. Validate those opportunities before showing them to people
8. Block low-quality output with eval checks

Important reminder:

This is not just a scraper.
This is a data quality and validation system.

If data cleaning, taxonomy, or eval is weak, the whole system becomes unreliable.

## Current project status

The project has now moved beyond early implementation into an early MVP stage.

Good news:

- The project already has strong engineering foundations.
- The team did not jump straight into a dashboard.
- The system already stores raw source data for audit and replay.
- There is already an eval gate in place.
- A first normalization runtime now exists.
- A first taxonomy runtime now exists.
- There are artifact outputs for normalization and taxonomy runs.
- Test coverage has grown beyond the original foundation tests.
- The MVP end-to-end spine now exists from one Amazon URL to one report output.
- The current version has been committed and pushed to `main`.

Current reality:

- The project can now run a narrow MVP workflow end to end.
- It is still not full market intelligence and should not be treated that way.
- The biggest product risk is now live-source reliability, especially safe primary-price extraction on real Amazon pages.
- Right now the repo is best described as "MVP spine shipped, trust rails preserved, live-source robustness still uneven."

## What has been completed so far

### 1. Shared schemas and contracts are in place

The project already has structured schemas for:

- raw source records
- normalized listings
- taxonomy assignments
- evidence
- opportunities
- run manifests
- task envelopes

Why this matters:

- Every module has a clear output shape.
- This reduces chaos later.
- This is the right way to keep LLM and non-LLM parts under control.

### 2. Validation layer exists

The repo includes a validation layer that checks documents against the project schemas.

Why this matters:

- The system can reject broken output early.
- This protects downstream modules from bad inputs.

### 3. Eval gate is working

There is already an eval runner and thresholds file.

The current phase-1 eval checks:

- listing validation
- taxonomy validation
- taxonomy coverage
- opportunity validation
- evidence coverage
- run metadata validation
- task dependency integrity

Current result on 2026-04-22:

- Unit tests passed
- Eval gate passed
- All current tracked metrics were at 1.0

Why this matters:

- This is one of the most important parts of the whole project.
- The meeting note was right: the real system is the eval and validation layer.

### 4. Ingestion foundation exists

The repo already has:

- a source connector interface
- an ingestion service
- filesystem raw storage
- snapshot manifest support
- replay support

Why this matters:

- We can save raw source payloads before any analysis happens.
- We can replay old snapshots for debugging and testing.
- This is necessary for auditability and trust.

### 5. First Amazon connector/probe exists

There is an early Amazon connector that:

- normalizes Amazon product URLs
- extracts ASINs
- fetches product pages
- stores the raw HTML and metadata
- flags robot-check responses

Why this matters:

- It proves the ingestion design is being used on a real source.
- It also already shows that source instability is real.

Observed example:

- One Amazon snapshot was marked as robot-check
- Another snapshot a few minutes later was not

This is exactly the kind of source fragility warned about in the meeting notes.

### 6. Run metadata groundwork exists

The project already has schemas/models for:

- run manifest
- task envelope

Why this matters:

- This is useful groundwork for a future state-machine orchestrator.
- It supports resumability and audit trails later.

### 7. Cleaning and normalization runtime now exists

This is a major step forward from the earlier review.

The repo now has:

- a batch normalization module
- an Amazon-specific listing normalizer
- normalization reports and record-level outputs
- tests for live snapshot normalization
- tests for duplicates, partial failure, and noisy larger batches

Why this matters:

- The project has started moving from raw source capture into usable structured listings.
- This is one of the most critical layers in the whole system.

Current limitation:

- The normalization logic is still early and mostly rule-based.
- It now emits field-level provenance plus operator-facing low-confidence reason codes in normalization artifacts (see `normalization_records.json`).
- It still needs broader fixture coverage across more messy listing patterns and more sources.

### 8. Taxonomy runtime now exists

This is also a meaningful step forward.

The repo now has:

- a taxonomy assigner
- taxonomy reports and record-level outputs
- tests for a live sweetener listing
- artifact generation for taxonomy results

Why this matters:

- The project can now assign multi-axis taxonomy outputs instead of only defining the schema for them.

Current limitation:

- The taxonomy appears heuristic and keyword-based.
- It is now measured against golden labeled fixtures and gated by threshold-based taxonomy evals.
- It is still early and heuristic, so the golden fixture set must expand over time to prevent overfitting.

### 9. MVP spine now exists

This is the biggest new milestone from the acceleration sprint.

The shipped MVP path is now:

- one Amazon URL
- raw snapshot storage
- normalization
- taxonomy
- one gap hypothesis report

Why this matters:

- The project is no longer only proving subsystems in isolation.
- It now has a usable narrow product flow for demo and review.

Current MVP behavior:

- the MVP CLI writes a per-run artifact folder every time
- on successful runs it produces the expected end-to-end output
- on failure it still writes debug-friendly artifact files and a human-readable report

PM view:

This is exactly the right kind of narrow MVP.
It creates learning without pretending the full product is finished.

### 10. Trust rails were preserved during acceleration

This is important.

The sprint did not throw away core trust protections.

The current MVP still keeps:

- raw snapshot storage
- schema validation
- eval-gated outputs
- explicit warnings
- low-confidence reasons

Why this matters:

- The team sped up without removing the minimum protections that keep the product from becoming fake-confidence software.

### 11. Live-source failure handling is better now

This is also meaningful product progress.

What improved:

- Amazon price extraction was hardened to avoid using irrelevant widget or recommended-product prices
- if the system cannot safely find the primary price, it now fails loudly instead of guessing
- the run still emits useful artifacts and a readable stop-condition report

Why this matters:

- This is the right product behavior
- wrong data would be worse than incomplete data

Current live reality check:

- the Lakanto URL sometimes normalizes successfully
- the same flow often fails with "missing product price" because the captured page is out-of-stock or uses dynamic markup

PM interpretation:

This is not a failure of product direction.
It is a real source-instability problem being handled in a responsible way.

## What is not built yet

These are the major missing pieces right now.

### 1. Live Amazon robustness for MVP demo reliability

What is still missing:

- safer and broader primary-price extraction for live Amazon pages
- stronger handling of out-of-stock and dynamic-markup product pages
- either a more reliable demo URL set or stronger live extraction coverage
- confidence that the MVP demo path works consistently in live conditions

Why this matters:

- The MVP exists now, so demo reliability matters immediately.
- The current most visible failure mode is live price extraction.

### 2. Cleaning and normalization quality proof

What is still missing:

- broader messy-data fixture coverage
- stronger correctness measurement
- expansion of the normalization-quality eval gate (more golden dirty batches, more sources, and tighter per-field expectations)
- proof that the layer stays reliable beyond the current tested patterns

Why this matters:

- The system should not reason on raw data.
- According to the project guideline, this layer is still critical path.

### 3. Taxonomy quality measurement

Taxonomy quality measurement now exists, but it needs broader coverage.

What is still missing:

- more golden labeled taxonomy fixtures (more product shapes and tricky ambiguous listings)
- stronger stability checks
- quality measurement against expected classification truth

Why this matters:

- Bad taxonomy means bad "similar vs adjacent" reasoning.
- That would break later opportunity analysis.

### 4. Demand and behavior modules

No demand or behavior signal module is present yet.

What is still missing:

- demand source integration
- trend logic
- behavior signal logic
- source reliability/time-window handling

### 5. Brand analysis module

No brand analysis engine is present yet.

What is still missing:

- structured brand reasoning
- stable schema-bound brand outputs
- repeatability controls

### 6. Opportunity generation and validation engine

The opportunity schema exists, but there is no live generation pipeline yet.

What is still missing:

- opportunity generation from cleaned market data
- validation scoring
- ranking after validation

### 7. State-machine orchestrator

There is groundwork, but no real orchestrator yet.

What is still missing:

- state transitions
- retries
- resumable partial runs
- failure recovery
- run observability

### 8. Drift monitoring and observability

The raw snapshots help, but this layer is not built yet.

What is still missing:

- drift alerts
- source health tracking
- quality trend tracking

### 9. UI

No evidence-first UI exists yet.

This is fine for now.

The meeting note clearly warned against building UI before system reliability.

## Suggested next steps

These next steps are ordered by what matters most for project success.

### Next step 1: Make the MVP demo path more reliable on live Amazon pages

The MVP path now exists, so the highest short-term product priority is reliability of that path.

Goal:

Make the live demo succeed more consistently without falling back into unsafe price guessing.

What to do:

- improve safe primary-price extraction for out-of-stock and dynamic-markup pages
- keep rejecting wrong widget/recommended prices
- add fixtures for known live failure shapes
- if needed, maintain one or more known-good in-stock URLs for demo use while extraction hardening continues

Definition of done:

- the MVP demo path succeeds more consistently on selected live URLs
- failures remain explicit and well explained
- no regression back to unsafe price guessing

### Next step 2: Strengthen the cleaning and normalization layer

The first version exists now, so the priority has changed.
The job is no longer to start this layer from zero.
The job is to make it robust, explainable, and trustworthy.

Goal:

Turn raw source records into normalized listings safely and consistently.

Product requirement from user experience standpoint:

This layer must also pass stress testing.

Why this matters in plain English:

If users upload or ingest messy marketplace data and the system becomes slow, inconsistent, or confusing, they will stop trusting the product before they ever reach the insight stage.

Engineering should choose the best technical path, but the user experience outcome must be protected.

Stress test requirement:

- The cleaning layer must remain reliable when input quality is poor, inconsistent, duplicated, incomplete, or unusually large.
- The user should not receive silently corrupted output.
- The user should not see obviously contradictory normalized results for similar items in the same run.
- The user should not be left guessing whether the system succeeded, partially succeeded, or failed.
- The user should get predictable behavior when data is messy, including clear handling of invalid or low-confidence records.

What success should feel like for the user:

- messy input does not break the workflow
- duplicate-heavy input does not create obviously inflated product counts
- inconsistent units do not produce confusing price or size comparisons
- partial failures do not look like successful analysis
- large batches do not make the system feel random or unstable

What engineering must prove before this layer is considered ready:

- the system behaves consistently under high-noise input
- the system behaves safely under incomplete input
- the system behaves clearly under partial failure
- the system can process larger realistic batches without quality collapsing
- outputs remain traceable enough that support or operators can explain what happened

Important PM guidance:

Do not optimize this layer only for "clean demo data."
It must be robust against real-world ugly data because that is what users will actually bring into the system.

What to build first:

- broader fixture coverage for ugly real-world cases
- stronger normalization correctness checks
- richer record-level trace outputs
- low-confidence and fallback reason capture
- output validation against the normalized listing schema

Definition of done:

- raw Amazon snapshot can be transformed into normalized listing output
- output passes schema validation
- edge cases are covered by tests and fixtures
- support can explain why important listing fields were parsed the way they were

### Next step 3: Add golden fixtures for dirty real-world cases

Do this alongside cleaning work.

Examples to add:

- duplicate listings
- different pack-size formats
- missing fields
- noisy titles
- inconsistent units
- robot-check or partial source responses

Why this matters:

- Cleaning quality must be measured, not assumed.

### Next step 4: Continue improving taxonomy quality

This step has started already.
The next job is not just to keep adding rules.
The next job is to prove taxonomy quality with measured evaluation.

Goal:

Classify listings in a stable multi-axis way.

What to build:

- golden labeled taxonomy fixtures
- threshold-based evals
- stability checks
- quality reporting against expected outputs
- deterministic rules can continue, but they must now be measured

Definition of done:

- normalized listings can be turned into taxonomy assignments
- taxonomy output passes schema checks
- classification is stable across repeated runs on the same fixture
- taxonomy quality is measured against labeled fixtures, not just demonstrated on examples

### Next step 5: Expand evals from structure-only to behavior quality

The eval layer is good, but it needs to grow with the system.

Add checks for:

- cleaning correctness
- dedup accuracy
- taxonomy stability
- taxonomy precision/recall on golden examples
- malformed source handling

Why this matters:

- Passing schema validation alone is not enough.

### Next step 6: Formalize source risk handling for Amazon

Amazon is already showing instability.

What to add:

- explicit source status markers
- retry policy
- fetch outcome categories
- source failure handling tests

Goal:

Do not let unstable source data silently poison downstream analysis.

### Next step 7: Only after that, start demand and behavior signals

Do not start this before cleaning and taxonomy are solid.

When ready, define:

- what demand signals will be used
- how freshness will be handled
- how source reliability will affect confidence

### Next step 8: Then add opportunity generation and validation

Important:

Do not generate market opportunities from raw supply gaps alone.

Opportunity logic should only happen after:

- cleaned data exists
- taxonomy exists
- demand/behavior grounding exists
- validation scoring rules are defined

### Next step 9: Build the orchestrator after module contracts are stable

The orchestrator should come after the core modules are real, not before.

Build when ready:

- stage transitions
- resumable execution
- retries
- observability hooks

## PM evaluation of the engineering team's proposed next steps

Engineering suggested these two next tasks:

1. Improve taxonomy quality with golden labeled fixtures and threshold-based evals
2. Add richer normalization/operator traces such as field-level extraction provenance and low-confidence reasons

### PM verdict

Both tasks are important.
One is essential immediately.
The other is essential for trust and support, but should be treated as the second priority unless it blocks diagnosis work.

### Task 1: Improve taxonomy quality with golden labeled fixtures and threshold-based evals

PM assessment:

This is essential and should be approved as a top-priority task.

Reason:

- The taxonomy runtime now exists, but it is still heuristic.
- Right now it can be "functional" without being truly reliable.
- The project guideline clearly says eval-gated development is mandatory.
- Without labeled fixtures and thresholds, taxonomy can drift or look correct in demos while still being wrong in production.

PM decision:

- Approve
- Treat as critical path
- Do not consider taxonomy production-ready until this exists

### Task 2: Add richer normalization/operator traces such as field-level extraction provenance and low-confidence reasons

PM assessment:

This is also essential, but as a trust and operations requirement rather than the single top technical gate.

Reason:

- Current normalization outputs show warnings, but the traces are still shallow.
- Support and operators will need to explain why brand, price, pack count, category, or availability were inferred a certain way.
- From a user experience standpoint, unexplained parsing behavior destroys trust.
- This also supports faster debugging when quality issues appear in real data.

PM decision:

- Approve
- Prioritize immediately after, or in parallel with, taxonomy eval work
- Do not let this get dropped as a "nice to have"

Important PM nuance:

If engineering can only do one thing first, taxonomy quality measurement comes first because it closes the biggest "looks functional but may be wrong" risk.
If engineering has capacity for parallel work, richer normalization traces should run alongside it because explainability is part of product trust, not just internal tooling.

## Recommended short-term execution plan

For the next working cycle, focus on these three items only:

1. Improve safe primary-price extraction for live Amazon pages without regressing into wrong-widget prices
2. Keep a reliable demo path ready, either through hardened extraction or a known-good live URL set
3. Continue expanding normalization/taxonomy fixture and eval coverage around the newly exposed live failure modes

This is the safest path.

It balances MVP momentum with responsible trust behavior.

## Simple summary

What the team has done well:

- started with contracts
- added eval gating early
- saved raw source data
- avoided building UI too soon
- accelerated into a narrow MVP without dropping trust rails
- handled live-source failure in a responsible way

What must happen next:

- make the MVP demo path more reliable on live source pages
- keep unsafe price guessing blocked
- keep expanding evals and fixtures around real failure modes
- use MVP output to learn which parts need hardening next

Plain English project status:

"The project now has a working MVP train running on the rails. The next job is not to build a bigger train yet. The next job is to make sure it can complete the demo journey reliably without leaving the tracks."

## Minimalist Evidence Dashboard sprint

Status:

- Implemented the next Evidence Workbench stage as a static Minimalist Evidence Dashboard.
- Kept the existing command unchanged: `python -m brand_gap_inference.build_evidence_workbench --collection-dir <collection_dir>`.
- Kept JSON artifacts as the source of truth. The dashboard is a review surface, not a final opportunity dashboard.

What changed:

- Added a top-level dashboard summary that tells PM what the run concluded, whether it is reviewable, what evidence is strong, what evidence is weak, and what PM should do next.
- Added evidence-quality scoring with image coverage, gallery coverage, promo-content coverage, warning totals, and missing-evidence flags.
- Added market-structure comparison between primary territory coverage and multi-axis coverage, so secondary positioning lanes are visible.
- Added a gap-validation panel that clearly explains when there are no supported candidates instead of implying the market has no opportunity.
- Added a product evidence matrix so reviewers can quickly scan rank, brand, ASIN, territory, secondary territories, rating/reviews, media availability, and warning count.
- Kept product cards, review controls, source artifact links, and trust rails below the dashboard summary for deeper review.

Why this matters:

- PM can now understand the outcome without reading raw JSON first.
- Engineering still keeps the trust-first posture because the dashboard is generated from validated artifacts and does not add new opportunity logic.
- The UI now highlights caveats, especially warning-heavy runs, instead of making the result feel more certain than the evidence supports.

Acceptance target for the saved `vegan protein bar` run:

- 15 products
- 15/15 primary images
- 15/15 gallery images
- 15/15 promo content
- 64 warnings, mostly currency-related
- 0 gap candidates
- recommendation `do_not_prioritize_yet`
- 2 primary territories
- 5 multi-axis coverage territories

## Amazon source-failure hardening sprint

Status:

- Added Amazon failure-shape coverage for unavailable and no-featured-offer product-detail responses.
- Improved safe price extraction so detail prices are accepted only when the source exposes a safe primary-offer signal.
- Added safe status handling for unavailable / no-featured-offer shapes.
- Blocked product-intelligence fallback to discovery price when the detail source explicitly says the primary offer is unsafe.
- Added a product-intelligence eval metric for source-failure accuracy.

Plain English behavior:

- If Amazon detail data says the product is unavailable or has no featured offer, the system does not reuse a search-result price just to keep the row complete.
- The product can still carry packaging, promo, title, rating, and review evidence.
- The final product-intelligence record gets a missing-price issue, which prevents downstream users from treating the price lane as reliable.

Fixtures and tests added:

- Unit test for blocking unsafe primary-offer price extraction.
- Unit test for using safe `single_offer` price and stock when product-results fields are incomplete.
- Unit test proving discovery price fallback is blocked when detail offer state is unsafe.
- Golden eval fixture `product_intelligence_amazon_offer_failure_batch_v1`.
- Eval metric `product_intelligence_source_failure_accuracy`.

Saved run regeneration:

- Regenerated `artifacts/mvp-replay-demo`.
- Regenerated `artifacts/mvp-replay-failure-demo`.
- Regenerated `artifacts/mvp-release-check-final`.
- Regenerated saved collection analysis/dashboard artifacts for:
  - `artifacts/data-collection-live-sugar-3`
  - `artifacts/data-collection-live-zero-calories-candy-15-query-fit-v1`
  - `artifacts/data-collection-live-vegan-protein-bar-15-final-mvp`

Current saved `vegan protein bar` dashboard after stricter status extraction:

- 15 products
- 15/15 primary images
- 15/15 gallery images
- 15/15 promo content
- 65 warnings, mostly currency-related
- 0 gap candidates
- recommendation `do_not_prioritize_yet`
- 2 primary territories
- 5 multi-axis coverage territories

Important note:

The warning count moved from 64 to 65 because stricter status extraction now surfaces one missing-availability condition instead of silently treating delivery text as availability. This is the correct trust-first direction.

## Adjacent CPG live validation sprint

Status:

- Added deterministic category context for three adjacent CPG validation lanes:
  - `hydration`
  - `protein_powder`
  - `energy_drink`
- Extended the same model surfaces already used for candy and protein bars:
  - query-fit candidate selection
  - brand-profile market maps
  - secondary territory coverage
  - demand-signal target terms
  - gap-validation target terms
- Added regression coverage for:
  - adjacent CPG query-family detection and adjacent-category filtering
  - adjacent CPG brand-profile territory assignment and secondary coverage
  - adjacent CPG demand-signal terms
  - adjacent CPG gap-validation spaces
  - secondary territory suppression of false missing-territory gaps
- Added golden eval coverage:
  - adjacent CPG demand-signal fixture
  - supported hydration opportunity case
  - crowded protein-powder no-priority case
  - warning-heavy energy-drink case that stays below supported status

Live validation runs completed:

- `artifacts/data-collection-live-electrolyte-powder-15-validation-v1`
  - query family: `hydration`
  - selected products: `15`
  - detail records: `15/15`
  - Evidence Workbench status: `success`
  - recommendation: `validate_with_caution`
  - supported candidates: `1`
  - top candidate: `family variety hydration x premium pantry`
  - warnings: `77`
  - evidence quality: `reviewable_with_caveats`
  - PM read: promising price-lane signal, but not a `validate_now` result because demand support is moderate and caveats are visible
- `artifacts/data-collection-live-protein-powder-15-validation-v1`
  - query family: `protein_powder`
  - selected products: `15`
  - detail records: `15/15`
  - Evidence Workbench status: `success`
  - recommendation: `do_not_prioritize_yet`
  - supported candidates: `0`
  - warnings: `68`
  - evidence quality: `reviewable_with_caveats`
  - PM read: crowded selected set; no supported gap after primary-plus-secondary coverage
- `artifacts/data-collection-live-energy-drink-15-validation-v1`
  - query family: `energy_drink`
  - selected products: `15`
  - detail records: `15/15`
  - Evidence Workbench status: `success`
  - recommendation: `do_not_prioritize_yet`
  - supported candidates: `0`
  - warnings: `69`
  - evidence quality: `reviewable_with_caveats`
  - PM read: no supported priority gap from this selected set

Trust posture:

- No adjacent CPG run produced `validate_now`.
- Warning-heavy runs remain explicitly caveated as `reviewable_with_caveats`.
- Currency warnings remain high enough that price-lane conclusions are directional only.
- Evidence Workbench remains a review UI; JSON artifacts remain the source of truth.
