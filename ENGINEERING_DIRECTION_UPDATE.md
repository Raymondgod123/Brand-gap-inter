# Engineering Direction Update

Last updated: 2026-04-24

## Context

Recent discussion focused on whether we should keep investing in the current custom Amazon scraping path, switch to Apify/Keepa-style alternatives, or move to browser automation we control.

This note records the conclusion so engineering has a shared reference point.

## Decision Summary

We are not changing the grand product goal.

We are adjusting the acquisition strategy.

Current conclusion:

- raw HTTP Amazon scraping is proving too unstable as the primary live acquisition method
- browser automation we control is the most promising next acquisition option
- Apify and Keepa should be treated as reference tools, benchmarks, or temporary accelerators, not reverse-engineering targets
- replay-mode MVP remains the reliable demo path until live acquisition improves materially

## What Is Still Aligned With The Grand Goal

The long-term goal remains:

- trustworthy marketplace ingestion
- clean normalization
- stable taxonomy
- validated opportunities
- decision support with trust rails

The following principles remain unchanged:

- store raw source artifacts for audit and replay
- preserve schema validation and eval gating
- fail clearly instead of guessing when critical fields are unsafe
- avoid building fake-confidence output

Changing the fetch layer does not change the product goal.
It is a course correction, not a strategy change.

## Why Raw HTTP Scraping Is No Longer The Preferred Path

Observed issues:

- live Amazon responses differ materially from what a real browser session sees
- featured price visibility depends on session, region, offer state, and dynamic markup
- the same URL can show a clear price in browser while the fetcher sees buying-options-only or incomplete markup
- continuing to widen regex coverage does not solve the underlying session/rendering problem

Conclusion:

- more raw HTTP scraping work is likely to produce diminishing returns
- we should not let the project become an endless scraper-hardening effort

## How Apify / Keepa Should Be Used

We should not attempt to reverse-engineer Keepa or Apify.

That is the wrong trade:

- legally risky
- operationally brittle
- still consumes engineering time on acquisition internals

We can still use them in useful ways:

- as field-reference models
- as output benchmarks
- as temporary comparison tools during acquisition redesign
- as possible vendor alternatives if speed matters more than control

## Preferred Next Acquisition Direction

Preferred direction:

- build a minimal browser-assisted acquisition path that captures what a human-visible page state actually shows

Why this is preferred:

- better match to user-visible truth
- less dependence on brittle raw HTML-only parsing
- more consistent with the observed failure mode
- keeps control inside our own trust and artifact pipeline

Important boundary:

- browser automation is only justified if it quickly improves reliable normalized listing capture
- if it turns into another long scraper engineering program, we are drifting from the product goal

## Boundary Conditions

We are still aligned with the grand plan if:

- acquisition changes are in service of reliable normalized listings
- replay/demo reliability remains strong
- eval coverage keeps expanding around real failure modes
- we do not weaken trust rails to force success

We are drifting if:

- most sprint capacity goes into scraper mechanics without improving MVP trust or completion
- browser automation becomes a large standalone platform effort
- we delay normalization/taxonomy quality work because acquisition never feels "finished"

## Practical Team Guidance

For the next cycle:

1. Keep replay-mode MVP as the official reliable demo path
2. Continue safe-stop behavior for unsafe live pages
3. Explore browser-assisted acquisition as the next live-source strategy
4. Use Apify/Keepa outputs only as benchmarks or temporary accelerators
5. Do not restart broad scraper R&D

## Bottom Line

We are not deviating from the grand goal.

We are correcting the weakest implementation layer.

The project remains a data integrity and evaluation system with an MVP decision-support flow.
The fetch layer should serve that goal, not become the goal.
