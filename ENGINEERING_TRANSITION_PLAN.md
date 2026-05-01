# Engineering Transition Plan

Last updated: 2026-04-24

## Purpose

Shift the project from an Amazon-scraper-first implementation path to a product-intelligence-first system while preserving the trust, validation, and artifact foundations already built.

This plan is intended to be concrete and executable.

## Transition Summary

### Old primary path

`Amazon URL -> raw HTML fetch -> normalize -> taxonomy -> report`

### New primary path

`Keyword -> product discovery API -> selected products -> browser/extension capture -> normalize -> taxonomy -> intelligence output`

### Core principle

We are not rebuilding the project.
We are replacing the weakest acquisition layer and keeping the strongest downstream layers.

## What To Keep

These parts should remain part of the core system:

- schema contracts
- validation layer
- eval gates and thresholds
- raw snapshot storage
- replay support
- normalization artifact outputs
- taxonomy artifact outputs
- low-confidence / provenance reporting
- MVP report generation patterns
- safe-stop behavior

Reason:

These are the trust rails. They are still correct under the new architecture.

## What To Retire From Primary Path

These parts should no longer be treated as the default product path:

- raw HTTP Amazon page fetching as primary acquisition
- ongoing regex expansion as the main reliability strategy
- live arbitrary Amazon URL success as the main MVP success metric

These parts may remain temporarily as:

- legacy connectors
- replay support
- fallback/testing tools

Reason:

They are consuming effort in the wrong place and are too fragile to be the system backbone.

## New Target Architecture

### Layer 1: Product Discovery

Input:

- keyword
- optional seed product link

Primary tool:

- SERP API or Apify Amazon product discovery source

Output:

- product list
- minimal structured fields per product

Required fields:

- title
- price
- rating
- review_count
- product_url
- asin if available
- provider metadata

### Layer 2: Selective Product Inspection

Input:

- selected product URLs from discovery layer

Primary MVP tool:

- Chrome extension or controlled browser-capture path

Optional later tool:

- browser agent automation

Output:

- visible product details for top selected products only

Captured fields:

- bullet points
- claims
- visible offer / pricing panel
- packaging screenshot
- image URLs if available
- visible brand cues / positioning signals

### Layer 3: Structured Intake / Normalization

Input:

- API discovery records
- extension/browser capture records

Action:

- map both sources into one normalized product-intelligence schema

Required source marker:

- `source = api | extension | browser_agent | replay`

### Layer 4: Intelligence Engine

Input:

- normalized product records

Generate:

- competitor clusters
- claim patterns
- pricing ladder
- packaging archetypes
- whitespace opportunities
- candidate product ideas

### Layer 5: Output

Deliverable:

- product landscape report
- competitor table
- claim map
- pricing map
- opportunity recommendations

## New Execution Priorities

## Priority 1: Discovery Ingestion

Build first:

- provider connector interface for discovery APIs
- first provider implementation (`SerpApiConnector` or `ApifyDiscoveryConnector`)
- raw response storage for provider payloads
- schema for discovery records
- replay support for provider discovery snapshots

Definition of done:

- keyword query returns structured product candidates
- raw provider response is stored
- results can be replayed without live API dependency

## Priority 2: Detail Capture Path

Build second:

- extension/browser capture contract
- endpoint or ingestion path for captured detail records
- screenshot storage support
- structured detail capture schema

Definition of done:

- one selected product can be captured from visible page state
- captured detail record is stored and replayable
- screenshots/artifacts are linked to the record

## Priority 3: Unified Normalization

Build third:

- normalize API discovery records into partial product records
- normalize extension/browser capture records into enriched product records
- merge records by product identity (`asin` or canonical URL)

Definition of done:

- the system can combine breadth data and depth data into one structured record
- provenance clearly shows which fields came from which source

## Priority 4: Intelligence Output

Build fourth:

- expand report output from one-product gap hypothesis to multi-product landscape summary
- support competitor table, claim patterns, price ladder, and opportunity summary

Definition of done:

- given a selected product set, the system produces a stakeholder-readable landscape report

## Repo Migration Plan

### Step 1: Freeze current Amazon fetch path

Action:

- stop major new feature work on raw HTTP Amazon fetch
- keep only bug fixes and replay support

Repo effect:

- existing Amazon connector remains, but is no longer the strategic center

### Step 2: Introduce source-type split

Add source families:

- `amazon_api_discovery`
- `amazon_browser_capture`
- existing `amazon` replay/raw-fetch path

Action:

- update source connector abstractions if needed
- keep storage and manifest design source-agnostic

### Step 3: Add new schemas

Add contracts for:

- discovery result record
- browser capture detail record
- merged product intelligence record

Action:

- validate these with the same contract system already in repo

### Step 4: Add provider connector

Implement first:

- `ApifyDiscoveryConnector` or `SerpApiDiscoveryConnector`

Action:

- keyword in
- structured product list out
- persist raw provider response snapshot

### Step 5: Add detail capture ingestion

Implement:

- extension/browser capture ingestion path
- screenshot metadata handling
- raw detail snapshot persistence

### Step 6: Build merge + normalization layer

Implement:

- merge product discovery record + detail capture record
- map into normalized intelligence schema

### Step 7: Expand report format

Implement:

- multi-product report generation
- competitor table
- claim clustering
- pricing ladder
- opportunity summary

## Suggested Work Breakdown (Next 2 Weeks)

### Track A: Discovery Integration

Tasks:

- define provider discovery schema
- build provider connector interface
- implement first provider connector
- add replay fixtures for provider response

### Track B: Capture Integration

Tasks:

- define detail capture schema
- define extension/browser capture payload
- store screenshots and visible content artifacts
- replay captured detail records

### Track C: Normalization + Merge

Tasks:

- map discovery fields into base normalized structure
- map capture fields into enriched normalized structure
- merge by ASIN / canonical URL

### Track D: Reporting

Tasks:

- move from single-product MVP report toward product landscape report
- preserve current safe-stop / artifact bundle behavior

## Acceptance Criteria For Pivot Completion

We can say the pivot is successfully implemented when:

- a keyword query returns a structured competitor candidate list
- selected products can be captured in detail from visible page state
- both discovery and capture data are stored and replayable
- downstream normalization still passes schema validation
- taxonomy and reporting still work on the merged data
- stakeholder report generation no longer depends on fragile raw HTML scraping as the main input path

## Explicit Non-Goals During Transition

Do not spend transition time on:

- UI polish
- full orchestrator build
- multi-marketplace expansion
- advanced demand modeling
- large-scale review scraping
- broad anti-bot/proxy infrastructure

Reason:

The transition goal is to replace the input strategy quickly without destabilizing the trust pipeline.

## Immediate Next Actions

1. Approve the pivot and treat raw HTTP Amazon fetch as legacy path
2. Choose first discovery provider:
   - Apify
   - SERP API
3. Define discovery record schema
4. Define browser/extension capture record schema
5. Implement first provider connector
6. Implement first capture ingestion path
7. Add replay fixtures for both
8. Run merged-data MVP report as the next milestone

## Bottom Line

The current repo already has the hard parts of trust:

- contracts
- validation
- replay
- artifacts
- safe-stop behavior

The transition should preserve those strengths and replace only the weakest layer:

- live acquisition

That is the fastest path from scraper effort to actual product intelligence.
