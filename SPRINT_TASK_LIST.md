# Brand Gap Inference Sprint Task List

Last updated: 2026-04-22

## Sprint goal

Make the current normalization and taxonomy layers trustworthy enough for the next stage of the project.

In plain English:

- measure taxonomy quality instead of just showing that it runs
- make normalization decisions easier to explain
- test both layers against uglier real-world data

## Sprint priorities

Priority 1:

Taxonomy quality measurement

Priority 2:

Normalization trace and explainability improvements

Priority 3:

Dirty-data fixture expansion for both layers

## Task 1: Golden labeled taxonomy fixtures

Why this matters:

The taxonomy engine exists now, but it is still mostly heuristic.
We need labeled examples so we can measure whether it is actually correct.

Work:

- create a set of golden labeled normalized listings
- include expected taxonomy axes for each listing
- cover more than one product shape, not just the current sweetener example
- include edge cases where classification is likely to be wrong

Suggested examples:

- sweeteners
- hydration products
- protein products
- energy products
- ambiguous listings
- listings with weak category signals

Definition of done:

- a reusable golden fixture set exists in the repo
- each fixture has expected taxonomy output
- the fixture set is readable and easy for future updates

Acceptance criteria:

- engineers can run tests against the labeled fixtures
- the fixture set includes both clean and ambiguous examples
- expected axes are defined clearly enough that another engineer could review them

## Task 2: Threshold-based taxonomy evals

Why this matters:

A classifier that only "runs" is not enough.
We need a quality gate.

Work:

- add taxonomy-specific eval metrics
- define minimum thresholds
- fail the eval when taxonomy quality drops below the agreed threshold
- include coverage and stability checks where helpful

Recommended metrics:

- taxonomy accuracy on golden fixtures
- per-axis accuracy if useful
- confidence distribution checks
- invalid assignment rate
- repeat-run stability on the same inputs

Definition of done:

- taxonomy eval reports quality, not just structure
- thresholds are committed in the repo
- CI or the normal eval workflow can fail when taxonomy quality regresses

Acceptance criteria:

- a bad taxonomy change can fail the eval
- the team can explain what each threshold means
- taxonomy quality is measurable over time

## Task 3: Richer normalization trace output

Why this matters:

Right now normalization can succeed, but support still may not be able to explain why a field was inferred a certain way.
That creates trust problems.

Work:

- add field-level extraction provenance where practical
- record whether a field came from HTML element, page title, URL slug, fallback rule, or default behavior
- record low-confidence reasons and fallback reasons
- include trace output in normalization artifacts without making them unreadable

Important product requirement:

This is not just for engineering convenience.
This is part of user trust and supportability.

Fields that are especially important:

- brand
- price
- pack count
- unit measure
- category path
- availability

Definition of done:

- normalization artifacts show how key fields were derived
- low-confidence or fallback parsing is visible in the output
- support can explain the result of at least one tricky listing without reading source code

Acceptance criteria:

- at least the key fields listed above have provenance or reason data
- fallback behavior is visible, not hidden
- traces stay structured and readable

## Task 4: Operator-facing low-confidence reasons

Why this matters:

Users and support need to know when the system is uncertain.

Work:

- define simple low-confidence reason categories
- record those categories during normalization
- surface them in batch reports or record-level outputs

Example reason types:

- brand inferred from fallback
- missing breadcrumb categories
- size signal missing
- price taken from secondary pattern
- availability unclear

Definition of done:

- low-confidence reasons are captured consistently
- record-level outputs show why a result may be weak

Acceptance criteria:

- weak records are easy to identify from the artifacts
- support can distinguish "parsed cleanly" from "parsed with fallback"

## Task 5: Dirty-data fixture expansion

Why this matters:

The system must survive ugly real-world data, not only happy-path examples.

Work:

- add more fixture examples for noisy normalization cases
- add more fixture examples for taxonomy edge cases
- include partial failures and contradictory signals

Suggested fixture types:

- duplicate-heavy batches
- missing HTML sections
- weak brand signals
- multiple possible size signals
- incomplete breadcrumbs
- robot-check pages
- product titles that look like more than one category

Definition of done:

- fixture coverage is broader than the current live sample and simple synthetic cases
- both normalization and taxonomy tests use uglier examples

Acceptance criteria:

- at least several new edge-case fixtures exist
- tests reference those fixtures
- failure modes are intentional and understandable

## Task 6: Reporting and developer usability

Why this matters:

If engineers cannot quickly understand failures, the eval system will be ignored.

Work:

- make taxonomy eval output readable
- make normalization trace output easy to inspect
- keep artifact naming and folder structure consistent

Definition of done:

- an engineer can run the workflow and understand failures without deep code digging

Acceptance criteria:

- failure output points to the broken listing or record
- reports are structured enough for debugging and future automation

## Suggested task order

1. Golden labeled taxonomy fixtures
2. Threshold-based taxonomy evals
3. Richer normalization trace output
4. Operator-facing low-confidence reasons
5. Dirty-data fixture expansion
6. Reporting and developer usability polish

## Suggested ownership split

Track A:

- taxonomy fixtures
- taxonomy thresholds
- taxonomy eval updates

Track B:

- normalization provenance
- low-confidence reasons
- normalization artifact updates

Track C:

- dirty-data fixtures
- test coverage expansion
- report readability improvements

This work can run partly in parallel if the team coordinates schema and artifact shapes early.

## Sprint success criteria

This sprint is successful if:

- taxonomy quality is measured with labeled fixtures
- taxonomy regressions can fail evaluation
- normalization outputs explain key field derivations
- low-confidence parsing is visible
- uglier real-world cases are represented in tests

## Simple PM summary

At the end of this sprint, the system should not just say:

"Here is the answer."

It should also be able to say:

"Here is how we got the answer, how confident we are, and whether quality slipped."
