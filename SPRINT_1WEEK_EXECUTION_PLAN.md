# 1-Week Sprint Execution Plan

## Completion Status (2026-04-24)

Sprint phase outcome: COMPLETED

Validation outcome:

- unit tests: pass
- eval runner: pass
- normalization eval: pass
- taxonomy eval: pass
- release rehearsal check (`mvp_release_check`): pass

Must Ship status:

- [x] 1. Lock the MVP execution path
- [x] 2. Finalize the demo-safe replay path
- [x] 3. Harden Amazon price extraction only where it is safe
- [x] 4. Expand regression coverage around live failure modes
- [x] 5. Improve operator-facing reports
- [x] 6. Prepare stakeholder-ready demo materials in repo form

Notes:

- Replay success + replay safe-stop behavior is now verifiable in one command:
  - `python -m brand_gap_inference.mvp_release_check --output-dir artifacts/mvp-release-check`
- Failure behavior remains trust-first and explicit (safe stop, no unsafe price guessing).
- Provenance and low-confidence reasoning are visible in both structured artifacts and report output.

## Sprint Goal

Ship a credible MVP result that stakeholders can see and trust:

- one audited product input
- one clean listing
- one taxonomy output
- one evidence-backed gap hypothesis report
- predictable success in replay mode
- clear failure behavior in live mode

## Definition Of Done

By end of week:

- replay-mode MVP run succeeds consistently on approved snapshots
- artifact bundle is complete and readable
- live-mode failures are explicit and operator-friendly
- unit tests pass
- eval gates pass
- no unsafe price guessing
- stakeholder demo can be run without engineering improvisation

## Must Ship

### 1. Lock the MVP execution path

Standardize the command path for:

- replay single snapshot
- replay fallback list
- live URL

Requirements:

- output folders are predictable and easy to explain
- every run produces a complete artifact bundle or a clear failure bundle

### 2. Finalize the demo-safe replay path

Requirements:

- curate a small approved snapshot set
- keep at least 1 guaranteed-success replay example
- keep at least 1 guaranteed-failure replay example that demonstrates safe stop behavior
- make both usable in stakeholder review

### 3. Harden Amazon price extraction only where it is safe

Requirements:

- continue improving primary-price extraction for real container markup
- keep widget/recommendation price rejection strict
- do not add risky fallbacks just to increase success rate

### 4. Expand regression coverage around live failure modes

Add fixtures for:

- no featured offers
- buying-options-only pages
- script/widget price traps
- dynamic primary container price markup

Requirements:

- these behaviors are encoded in tests
- these behaviors are protected by normalization eval coverage

### 5. Improve operator-facing reports

Requirements:

- make `mvp_report.md` easier for non-engineers to read
- failure reports clearly say:
  - what failed
  - why it failed
  - whether the system stopped safely
- provenance and low-confidence reasons remain visible

### 6. Prepare stakeholder-ready demo materials in repo form

Requirements:

- update the README MVP section
- document exact commands for:
  - safe replay demo
  - live attempt
  - artifact review
- keep instructions short and copy-pasteable

## Nice To Have

- replay fallback with a small approved snapshot list
- structured attempt summaries in a more human-readable form
- one extra taxonomy golden fixture for edge-case sweetener audience classification

## Hard Stop / Defer

Do not spend sprint time on:

- UI
- orchestrator/state machine
- demand module
- behavior module
- brand analysis engine
- multi-source expansion
- advanced opportunity ranking

These matter later, but they do not help us finish the current result quickly.

## Suggested Day-by-Day

### Day 1: Freeze scope and stabilize commands

- confirm MVP commands
- confirm artifact structure
- confirm demo success/failure snapshots

### Day 2: Harden safe price extraction

- only primary-container-safe improvements
- no unsafe fallback logic

### Day 3: Add regression fixtures and eval coverage

- encode newest live failure shapes
- keep tests and evals green

### Day 4: Improve reports and stakeholder readability

- polish `mvp_report.md`
- improve failure wording
- improve README MVP usage

### Day 5: Demo rehearsal and release check

- run replay success path
- run replay failure path
- run live attempt if available
- verify tests and evals
- prepare sprint summary

## Success Metric For Stakeholders

At the end of the week, we should be able to say:

- We have a reliable audited MVP demo path.
- The system produces safe output when it can.
- The system fails clearly when live source data is unsafe.
- We did not trade trust for speed.

## Operating Principle

Optimize for reliable output first, broader intelligence second.

The goal of this sprint is to complete Phase MVP Reliability, not the full long-term platform vision.
