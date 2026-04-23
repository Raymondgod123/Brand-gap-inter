# Brand Gap Inference Progress Log

Last updated: 2026-04-22 (reevaluated after normalization + taxonomy implementation)

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

The project is no longer just in a foundation stage.
It has now entered an early implementation stage.

Good news:

- The project already has strong engineering foundations.
- The team did not jump straight into a dashboard.
- The system already stores raw source data for audit and replay.
- There is already an eval gate in place.
- A first normalization runtime now exists.
- A first taxonomy runtime now exists.
- There are artifact outputs for normalization and taxonomy runs.
- Test coverage has grown beyond the original foundation tests.

Current reality:

- The project is now doing early structured analysis, but not yet trustworthy full market intelligence.
- Cleaning and taxonomy are no longer missing entirely, but they are still early-version implementations.
- The biggest risk has shifted from "nothing exists yet" to "the early logic may look functional before it is truly reliable."
- Right now the repo is best described as "core rails plus first-pass normalization and taxonomy are built, but quality proof is still too thin."

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

## What is not built yet

These are the major missing pieces right now.

### 1. Cleaning and normalization quality proof

What is still missing:

- broader messy-data fixture coverage
- stronger correctness measurement
- expansion of the normalization-quality eval gate (more golden dirty batches, more sources, and tighter per-field expectations)
- proof that the layer stays reliable beyond the current tested patterns

Why this matters:

- The system should not reason on raw data.
- According to the project guideline, this layer is critical path.

### 2. Taxonomy quality measurement

Taxonomy quality measurement now exists, but it needs broader coverage.

What is still missing:

- more golden labeled taxonomy fixtures (more product shapes and tricky ambiguous listings)
- stronger stability checks
- quality measurement against expected classification truth

Why this matters:

- Bad taxonomy means bad "similar vs adjacent" reasoning.
- That would break later opportunity analysis.

### 3. Demand and behavior modules

No demand or behavior signal module is present yet.

What is still missing:

- demand source integration
- trend logic
- behavior signal logic
- source reliability/time-window handling

### 4. Brand analysis module

No brand analysis engine is present yet.

What is still missing:

- structured brand reasoning
- stable schema-bound brand outputs
- repeatability controls

### 5. Opportunity generation and validation engine

The opportunity schema exists, but there is no live generation pipeline yet.

What is still missing:

- opportunity generation from cleaned market data
- validation scoring
- ranking after validation

### 6. State-machine orchestrator

There is groundwork, but no real orchestrator yet.

What is still missing:

- state transitions
- retries
- resumable partial runs
- failure recovery
- run observability

### 7. Drift monitoring and observability

The raw snapshots help, but this layer is not built yet.

What is still missing:

- drift alerts
- source health tracking
- quality trend tracking

### 8. UI

No evidence-first UI exists yet.

This is fine for now.

The meeting note clearly warned against building UI before system reliability.

## Suggested next steps

These next steps are ordered by what matters most for project success.

### Next step 1: Strengthen the cleaning and normalization layer

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

### Next step 2: Add golden fixtures for dirty real-world cases

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

### Next step 3: Build the taxonomy engine

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

### Next step 4: Expand evals from structure-only to behavior quality

The eval layer is good, but it needs to grow with the system.

Add checks for:

- cleaning correctness
- dedup accuracy
- taxonomy stability
- taxonomy precision/recall on golden examples
- malformed source handling

Why this matters:

- Passing schema validation alone is not enough.

### Next step 5: Formalize source risk handling for Amazon

Amazon is already showing instability.

What to add:

- explicit source status markers
- retry policy
- fetch outcome categories
- source failure handling tests

Goal:

Do not let unstable source data silently poison downstream analysis.

### Next step 6: Only after that, start demand and behavior signals

Do not start this before cleaning and taxonomy are solid.

When ready, define:

- what demand signals will be used
- how freshness will be handled
- how source reliability will affect confidence

### Next step 7: Then add opportunity generation and validation

Important:

Do not generate market opportunities from raw supply gaps alone.

Opportunity logic should only happen after:

- cleaned data exists
- taxonomy exists
- demand/behavior grounding exists
- validation scoring rules are defined

### Next step 8: Build the orchestrator after module contracts are stable

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

1. Add golden labeled taxonomy fixtures and threshold-based taxonomy evals
2. Add richer normalization traces with field-level provenance and low-confidence reasons
3. Expand dirty-data fixtures so both normalization and taxonomy are tested against uglier real-world inputs

This is the safest path.

It matches the meeting-note guidance and reduces the risk of building a system that looks smart but is actually wrong.

## Simple summary

What the team has done well:

- started with contracts
- added eval gating early
- saved raw source data
- avoided building UI too soon

What must happen next:

- measure taxonomy quality, not just taxonomy structure
- make normalization decisions explainable
- expand evals to measure real quality
- keep building against ugly real-world data, not demo-clean data

Plain English project status:

"The project now has a small working train on the rails, but it still needs instrumentation and safety checks before people should trust the journey."
