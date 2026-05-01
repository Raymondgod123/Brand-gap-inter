# Engineering Brief: CI Eval Gate Coverage

Date: 2026-04-25

## Change Made

The GitHub Actions foundation workflow now runs the newer deterministic quality gates in CI:

- product intelligence
- brand positioning
- brand profile
- gap validation
- deep inference

This is a low-conflict architecture fix. It does not change runtime heuristics, schemas, fixtures, or live-source behavior. It only makes CI enforce the eval gates that already exist in the current working tree.

## Why This Was Prioritized

The progress log says the project principle is "no important module is accepted without measurable thresholds." The workflow had fallen behind that principle: CI still covered phase1, taxonomy, and normalization, but not the newer post-collection and synthesis layers.

This matters most for the deep-inference layer, because it now depends on the current deterministic stack:

- product intelligence
- landscape
- brand positioning
- brand profiles
- gap validation

If any upstream artifact contract drifts, CI should catch it before the branch lands.

## Validation Run Locally

Commands run:

```powershell
$env:PYTHONPATH='src'; py -m brand_gap_inference.deep_inference_eval --cases eval/fixtures/deep_inference_golden/batches.json --thresholds eval/thresholds_deep_inference.json
$env:PYTHONPATH='src'; py -m unittest tests.test_deep_inference tests.test_deep_inference_eval
```

Both passed.

## Team Follow-Up

Keep the workflow aligned with every new threshold-gated layer. When a new `*_eval.py` module and thresholds file are added, add the matching CI step in the same PR or working slice.

Recommended next cleanup, separate from this change: consider renaming the workflow from `phase1-foundation` to something broader once the current in-flight work settles, since it now gates the full MVP analysis stack rather than only phase 1.

## 2026-04-28 Follow-Up: Demand Signal Gate Added

The demand-signal layer is now treated as a first-class gated module.

New CI gate:

```powershell
python -m brand_gap_inference.demand_signal_eval --cases eval/fixtures/demand_signal_golden/batches.json --thresholds eval/thresholds_demand_signal.json
```

Why this matters:

- Gap validation now depends on a replayable demand proxy, not only selected-set whitespace and review/rating traction.
- The demand layer produces its own schema-bound artifacts before gap validation consumes them.
- CI now protects this contract so future changes cannot silently remove demand grounding from opportunity ranking.

Local validation completed:

```powershell
$env:PYTHONPATH='src'; py -m unittest discover -s tests -p "test_*.py"
$env:PYTHONPATH='src'; py -m brand_gap_inference.eval_runner --fixtures-dir eval/fixtures/phase1 --thresholds eval/thresholds.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.discovery_eval --cases eval/fixtures/discovery_golden/batches.json --thresholds eval/thresholds_discovery.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.taxonomy_eval --cases eval/fixtures/taxonomy_golden/cases.json --thresholds eval/taxonomy_thresholds.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.normalization_eval --cases eval/fixtures/normalization_golden/batches.json --thresholds eval/normalization_thresholds.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.product_intelligence_eval --cases eval/fixtures/product_intelligence_golden/batches.json --thresholds eval/thresholds_product_intelligence.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.brand_analysis_eval --cases eval/fixtures/brand_positioning_golden/batches.json --thresholds eval/thresholds_brand_positioning.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.brand_profile_eval --cases eval/fixtures/brand_profile_golden/batches.json --thresholds eval/thresholds_brand_profile.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.demand_signal_eval --cases eval/fixtures/demand_signal_golden/batches.json --thresholds eval/thresholds_demand_signal.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.gap_validation_eval --cases eval/fixtures/gap_validation_golden/batches.json --thresholds eval/thresholds_gap_validation.json
$env:PYTHONPATH='src'; py -m brand_gap_inference.deep_inference_eval --cases eval/fixtures/deep_inference_golden/batches.json --thresholds eval/thresholds_deep_inference.json
```

Result: all passed locally.

## 2026-04-28 Follow-Up: Decision Brief Gate Added

The deterministic PM decision-brief layer is now also threshold-gated.

New CI gate:

```powershell
python -m brand_gap_inference.decision_brief_eval --cases eval/fixtures/decision_brief_golden/batches.json --thresholds eval/thresholds_decision_brief.json
```

Why this matters:

- The product now has a handoff artifact between gap scoring and any future UI or LLM synthesis.
- The brief converts evidence into recommendation levels without changing the underlying gap-validation math.
- CI now protects the plain-English recommendation behavior, including whether supported and tentative opportunities are framed correctly.

Team rule reinforced:

- Any module that changes PM-facing interpretation must have fixtures, thresholds, and a CI gate in the same slice.
