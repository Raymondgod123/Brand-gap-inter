# Brand Gap Inference MVP Acceleration Plan

Last updated: 2026-04-23

## PM decision

Yes, the project is now far enough along to stop building only step by step and move into an MVP push.

That said, we should not speed run blindly.

The right move is:

1. Freeze the current working foundation
2. Commit the current version as a checkpoint
3. Define a narrow MVP
4. Build the shortest usable end-to-end product path
5. Reevaluate broken pieces after the MVP is working

Status update:

This plan has now largely been executed.
The narrow MVP path exists, the current version has been committed and pushed, and the main PM focus has shifted to live demo reliability.

In plain English:

We should stop acting like we are still at zero.
We already have enough pieces to assemble a first product.
Now the team should focus on getting a usable result in users' hands fast, while protecting only the most critical trust rails.

## Current progress evaluation

The project is no longer just an architecture exercise.

What already exists:

- raw ingestion foundation
- replayable raw storage
- schema validation
- eval gate foundation
- Amazon connector/probe
- normalization runtime
- taxonomy runtime
- artifact outputs
- tests for ingestion, normalization, taxonomy, and eval flow
- MVP CLI workflow
- failure-path reporting for MVP runs

What this means:

- We now have enough to create a first end-to-end MVP flow.
- The current system is not fully trustworthy yet.
- But it is strong enough to support a focused MVP if we keep scope narrow.

Updated PM reading:

- the MVP flow is now real, not just planned
- the main current product risk is live Amazon source behavior, especially safe price extraction on dynamic or out-of-stock pages

PM view:

If we continue polishing every layer before assembling a product, we may lose momentum.
If we rush without any guardrails, we risk building a convincing but wrong dashboard.

So the correct balance is:

Speed up product assembly, but keep a few non-negotiable trust checks.

## PM recommendation to engineering

### 1. Commit the current version now

This is strongly recommended.

Why:

- the current repo has meaningful progress
- normalization and taxonomy are now real working layers
- this is the right time to create a stable checkpoint before the MVP push

What this commit represents:

- "foundation + first-pass normalization + first-pass taxonomy"

PM note:

Do not treat this as a release-quality commit.
Treat it as a product checkpoint so the team can move faster from a known base.

Status:

Done.
The update report says the work is committed and pushed to `main`.

### 2. Stop expanding scope horizontally

For MVP, do not try to build everything in the original long-term vision.

Do not add yet:

- multi-source ingestion
- full orchestration engine
- demand intelligence from many sources
- deep brand reasoning
- polished UI platform
- broad general-purpose opportunity engine

These can come later.

### 3. Define a very narrow MVP

The MVP should answer one useful user question well enough to demonstrate value.

Recommended MVP statement:

"Given an Amazon product URL, generate a cleaned product record, assign product taxonomy, and produce a simple evidence-backed gap hypothesis with clear caveats."

That is enough for an MVP.

Not enough for final product.
Enough to learn quickly.

## Recommended MVP scope

### MVP input

- one Amazon product URL

### MVP system flow

1. ingest raw product page
2. store raw snapshot
3. normalize listing fields
4. assign taxonomy axes
5. generate a simple gap hypothesis
6. show evidence and warnings

### MVP output

The output should include:

- cleaned listing summary
- taxonomy summary
- confidence and warnings
- evidence used
- one simple "possible opportunity/gap" statement
- clear note that output is decision support, not truth

## What can be simplified for MVP

These are acceptable shortcuts for the first MVP:

- Amazon-only input
- single-product analysis
- heuristic taxonomy
- simple opportunity logic
- file-based artifact output instead of full app UI
- manual review step instead of full automation

These shortcuts are okay because they help us learn faster.

## What cannot be skipped, even for MVP

These are the minimum trust rails we must keep:

### 1. Raw snapshot storage

We must keep this.

Reason:

- without raw evidence, we cannot debug, explain, or audit

### 2. Schema validation

We must keep this.

Reason:

- without structure checks, downstream behavior will become unstable fast

### 3. Clear warning/confidence output

We must keep this.

Reason:

- the MVP must communicate uncertainty
- users should not mistake weak output for strong truth

### 4. Basic eval check for regressions

We must keep this.

Reason:

- even if the eval is still lightweight, we need at least a minimal quality gate

PM rule:

We can simplify quality measurement for MVP.
We should not remove quality measurement entirely.

## Recommended engineering plan

### Phase A: Freeze and checkpoint

Goal:

Create a stable base before acceleration.

Tasks:

- review current repo state
- clean up any obviously broken local artifacts if needed
- commit current version as checkpoint
- tag the commit or clearly name it

Definition of done:

- the team can return to this state safely if the MVP sprint gets messy

### Phase B: Build the shortest end-to-end product flow

Goal:

Get a usable MVP output from one Amazon URL.

Tasks:

- define one command or workflow that runs ingest -> normalize -> taxonomy -> simple gap output
- keep artifact outputs readable
- surface warnings and confidence clearly

Definition of done:

- a teammate can run one workflow and get the MVP result without manual digging across many files

Status:

Done in first-pass MVP form.
The CLI path `python -m brand_gap_inference.mvp_run --url <amazon_url>` now writes a per-run artifacts folder and produces readable output even on failure.

### Phase C: Add a very simple opportunity layer

Goal:

Produce one lightweight but useful product output beyond cleaned data.

Guidance:

- do not overbuild this
- do not pretend it is full intelligence
- keep it evidence-backed and narrow

Possible MVP output shape:

- "This product appears positioned for keto sugar replacement and baking use."
- "Adjacent whitespace may exist in [x] format or [y] audience segment."
- "Confidence is limited because [reasons]."

Definition of done:

- the system produces a simple, readable hypothesis with explicit caveats

Status:

Done in narrow MVP form.
The sprint report confirms the pipeline now reaches one gap hypothesis report.

### Phase D: Thin presentation layer

Goal:

Make the MVP easy to demo and review.

Allowed options:

- markdown report
- json + markdown summary
- simple CLI output
- minimal local page if very fast to build

PM guidance:

Do not build a polished product shell yet.
Just make the output easy to understand.

Status:

Done in a lightweight way through artifact outputs and `mvp_report.md`.

### Phase E: Reevaluate broken pieces after MVP

Goal:

Use the working MVP to find the real weaknesses.

What to review after MVP works:

- normalization failures
- taxonomy misclassifications
- unclear confidence signals
- weak opportunity statements
- source instability pain points

This is the right time to deepen quality work.

Status:

This is the current phase now.
The main exposed weak point is safe live Amazon primary-price extraction.

## Updated engineering priorities

### Immediate priority

Ship a narrow end-to-end MVP.

Status:

Completed.

### Secondary priority

Keep only the minimum trust rails needed to avoid fake confidence.

Status:

Completed and preserved.
The update confirms raw snapshots, schema validation, eval gates, and explicit warnings/low-confidence reasons remain in place.

### Deferred priority

Broader hardening, broader coverage, and platform depth after MVP feedback.

Status:

This is the active work bucket now, starting with live-source hardening instead of broad platform expansion.

## Suggested backlog for the MVP push

### Must have

- checkpoint commit of current system
- one end-to-end workflow from URL to final report
- normalized output
- taxonomy output
- simple evidence-backed gap hypothesis
- confidence/warning section

Status:

Completed.

### Should have

- one markdown summary artifact
- one example demo run checked into artifacts or docs
- basic regression checks for the MVP flow

Status:

Mostly completed.
The markdown report artifact exists and regression checks are reported green.
The remaining practical need is a more reliable live demo path.

### Nice to have

- richer provenance
- stronger taxonomy eval thresholds
- improved fixture breadth

Status:

Partly completed.
Richer provenance and low-confidence reasons are present.
Fixture breadth and hardening still need more work.

Important PM note:

These nice-to-have items are still valuable.
They are just not the first blocking steps if the goal is MVP speed.

## Clear guidance to engineering team

Message to engineering:

"We are moving into MVP mode. Freeze the current progress with a checkpoint commit, then optimize for the shortest path to a usable end-to-end output. Keep raw evidence, schema validation, and visible warnings. Cut anything that does not help us get from one Amazon URL to one understandable gap report quickly."

## Definition of MVP done

The MVP is done when:

- a user can provide one Amazon product URL
- the system stores the raw record
- the system produces a cleaned listing
- the system assigns taxonomy
- the system produces a simple gap hypothesis
- the output includes evidence and caveats
- the workflow is easy enough to run for a demo

Current PM judgment:

The MVP is done in first-pass form.
It is demoable, but the live demo path is not yet fully dependable because some real Amazon pages fail safe on price extraction.

## Next PM focus

The next focus is not to widen the product.
The next focus is to make the current MVP path more dependable.

Top near-term tasks:

- improve safe primary-price extraction for live Amazon pages
- avoid regressing back into wrong-widget prices
- keep the failure-path reporting clean
- maintain one or more reliable demo URLs while hardening continues

## Plain English summary

The project is ready to stop behaving like a research-only build.

We should:

- save the current progress
- narrow the scope hard
- assemble the fastest believable MVP
- learn from real output
- then come back and harden the weak parts

That is still the right strategy.
The team has now reached the "learn from real output and harden the weak parts" stage.
