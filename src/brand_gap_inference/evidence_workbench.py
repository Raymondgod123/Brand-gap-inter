from __future__ import annotations

from datetime import UTC, datetime
from html import escape
import json
import os
from pathlib import Path

from .contracts import assert_valid


def write_evidence_workbench_artifacts(
    *,
    collection_dir: Path,
    output_dir: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, str]:
    resolved_collection_dir = collection_dir.resolve()
    resolved_output_dir = (output_dir or (resolved_collection_dir / "evidence_workbench")).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_generated_at = generated_at or _utc_now()

    source_artifacts = _source_artifacts(resolved_collection_dir)
    payloads = {
        name: _load_json_optional(path)
        for name, path in source_artifacts.items()
        if path.exists()
    }
    run_id = _infer_run_id(payloads, resolved_collection_dir)
    caveats = _workbench_caveats(payloads)
    review_summary = _build_review_summary(payloads)
    evidence_quality_summary = _build_evidence_quality_summary(review_summary)
    market_structure_summary = _build_market_structure_summary(payloads)
    product_matrix_summary = _build_product_matrix_summary(payloads)
    dashboard_summary = _build_dashboard_summary(
        payloads=payloads,
        review_summary=review_summary,
        evidence_quality_summary=evidence_quality_summary,
        market_structure_summary=market_structure_summary,
    )

    html_path = resolved_output_dir / "index.html"
    manifest_path = resolved_output_dir / "evidence_workbench_manifest.json"
    html_path.write_text(
        render_evidence_workbench_html(
            collection_dir=resolved_collection_dir,
            output_dir=resolved_output_dir,
            run_id=run_id,
            generated_at=resolved_generated_at,
            payloads=payloads,
            review_summary=review_summary,
            dashboard_summary=dashboard_summary,
            evidence_quality_summary=evidence_quality_summary,
            market_structure_summary=market_structure_summary,
            product_matrix_summary=product_matrix_summary,
            source_artifacts=source_artifacts,
        ),
        encoding="utf-8",
    )

    manifest = {
        "run_id": run_id,
        "status": "partial_success" if caveats else "success",
        "generated_at": resolved_generated_at,
        "collection_dir": str(resolved_collection_dir),
        "output_dir": str(resolved_output_dir),
        "entrypoint": str(html_path),
        "review_summary": review_summary,
        "dashboard_summary": dashboard_summary,
        "evidence_quality_summary": evidence_quality_summary,
        "market_structure_summary": market_structure_summary,
        "product_matrix_summary": product_matrix_summary,
        "source_artifacts": {
            name: str(path)
            for name, path in source_artifacts.items()
            if path.exists()
        },
        "artifacts": {
            "evidence_workbench_html": str(html_path),
            "evidence_workbench_manifest": str(manifest_path),
        },
        "caveats": caveats,
    }
    assert_valid("evidence_workbench_manifest", manifest)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return dict(manifest["artifacts"])


def render_evidence_workbench_html(
    *,
    collection_dir: Path,
    output_dir: Path,
    run_id: str,
    generated_at: str,
    payloads: dict[str, object],
    review_summary: dict[str, object],
    dashboard_summary: dict[str, object],
    evidence_quality_summary: dict[str, object],
    market_structure_summary: dict[str, object],
    product_matrix_summary: dict[str, object],
    source_artifacts: dict[str, Path],
) -> str:
    decision = _object_payload(payloads.get("decision_brief_report"))
    gap = _object_payload(payloads.get("gap_validation_report"))
    brand = _object_payload(payloads.get("brand_profile_report"))
    demand = _object_payload(payloads.get("demand_signal_report"))
    analysis = _object_payload(payloads.get("analysis_stack_report"))
    products = _list_payload(payloads.get("product_intelligence_records"))
    profiles = _profile_lookup(brand, payloads.get("brand_profile_records"))

    recommendation = _text(decision.get("recommendation_level")) or "not_available"
    headline = _text(decision.get("headline")) or "No decision brief has been generated yet."
    category = _first_text(decision.get("category_context"), brand.get("category_context"), gap.get("category_context")) or "none"
    status = _first_text(decision.get("status"), analysis.get("status"), gap.get("status")) or "unknown"
    summary = _text(decision.get("executive_summary")) or "Run the deterministic analysis stack to populate the PM decision summary."

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>Minimalist Evidence Dashboard - {_html(run_id)}</title>",
            "<style>",
            _css(),
            "</style>",
            "</head>",
            "<body>",
            '<main class="shell">',
            _hero(
                run_id=run_id,
                generated_at=generated_at,
                status=status,
                recommendation=recommendation,
                category=category,
                headline=headline,
                summary=summary,
            ),
            _section_dashboard_summary(dashboard_summary),
            _section_evidence_quality(evidence_quality_summary),
            _section_market_structure_dashboard(market_structure_summary),
            _section_gap_validation_dashboard(gap),
            _section_product_matrix(product_matrix_summary),
            _section_review_readiness(review_summary),
            _section_decision(decision, gap),
            _section_market_map(brand, demand, gap),
            _section_review_controls(review_summary),
            _section_products(products, profiles),
            _section_trust(decision, analysis, brand, demand, gap, review_summary, source_artifacts, output_dir),
            "</main>",
            "<script>",
            _script(),
            "</script>",
            "</body>",
            "</html>",
        ]
    )


def _hero(
    *,
    run_id: str,
    generated_at: str,
    status: str,
    recommendation: str,
    category: str,
    headline: str,
    summary: str,
) -> str:
    return f"""
<section class="hero">
  <div class="eyebrow">Minimalist Evidence Dashboard / Evidence Workbench v0</div>
  <div class="hero-grid">
    <div>
      <h1>{_html(headline)}</h1>
      <p class="lede">{_html(summary)}</p>
    </div>
    <aside class="run-card">
      <dl>
        <div><dt>Run</dt><dd>{_html(run_id)}</dd></div>
        <div><dt>Status</dt><dd><span class="pill {_status_class(status)}">{_html(status)}</span></dd></div>
        <div><dt>Recommendation</dt><dd><span class="pill recommendation">{_html(recommendation)}</span></dd></div>
        <div><dt>Category</dt><dd>{_html(category)}</dd></div>
        <div><dt>Generated</dt><dd>{_html(generated_at)}</dd></div>
      </dl>
    </aside>
  </div>
</section>
"""


def _section_dashboard_summary(summary: dict[str, object]) -> str:
    return f"""
<section class="dashboard-summary" id="dashboard-summary">
  <div class="panel-heading">
    <span class="section-kicker">Dashboard Summary</span>
    <h2>What did this run conclude?</h2>
  </div>
  <div class="dashboard-grid">
    <article class="summary-card primary">
      <span class="metric-label">Conclusion</span>
      <strong>{_html(summary.get("run_conclusion"))}</strong>
      <p>{_html(summary.get("headline"))}</p>
    </article>
    <article class="summary-card">
      <span class="metric-label">Review Readiness</span>
      <strong>{_html(summary.get("review_readiness"))}</strong>
      <p>Recommendation: {_html(summary.get("recommendation_level"))}</p>
    </article>
    <article class="summary-card">
      <span class="metric-label">Strong Evidence</span>
      {_html_list(_string_list(summary.get("strong_evidence")) or ["No strong evidence signals were found."])}
    </article>
    <article class="summary-card">
      <span class="metric-label">Weak Evidence</span>
      {_html_list(_string_list(summary.get("weak_evidence")) or ["No material weak evidence signals were found."])}
    </article>
    <article class="summary-card next-step">
      <span class="metric-label">PM Next Step</span>
      <strong>{_html(summary.get("pm_next_step"))}</strong>
      <p>Category context: {_html(summary.get("category_context"))}</p>
    </article>
  </div>
</section>
"""


def _section_evidence_quality(summary: dict[str, object]) -> str:
    primary_pct = round(float(summary.get("primary_image_coverage") or 0) * 100)
    gallery_pct = round(float(summary.get("gallery_image_coverage") or 0) * 100)
    promo_pct = round(float(summary.get("promo_content_coverage") or 0) * 100)
    score_pct = round(float(summary.get("quality_score") or 0) * 100)
    top_warnings = _dict_payload(summary.get("top_warning_types"))

    return f"""
<section class="panel analysis-panel" id="evidence-quality">
  <div class="panel-heading">
    <span class="section-kicker">Evidence Quality</span>
    <h2>What evidence is strong or weak?</h2>
  </div>
  <div class="quality-grid">
    <article class="metric-card">
      <span class="metric-label">Quality Label</span>
      <strong>{_html(summary.get("quality_label"))}</strong>
      <p>Score {score_pct}%. This is a review aid, not a launch-readiness score.</p>
    </article>
    {_coverage_bar("Primary images", primary_pct)}
    {_coverage_bar("Gallery images", gallery_pct)}
    {_coverage_bar("Promo content", promo_pct)}
  </div>
  <div class="two-column">
    <div>
      <h3>Warnings</h3>
      <p class="note">{_integer(summary.get("products_with_warnings"))} product(s) have {_integer(summary.get("total_warning_count"))} total warnings.</p>
      {_metric_list(top_warnings, empty="No warning types were found.")}
    </div>
    <div>
      <h3>Missing Evidence Flags</h3>
      {_html_list(_string_list(summary.get("missing_evidence_flags")) or ["No missing-evidence flags were found."])}
    </div>
  </div>
</section>
"""


def _coverage_bar(label: str, percent: int) -> str:
    clamped = max(0, min(100, percent))
    return f"""
<article class="coverage-card">
  <span class="metric-label">{_html(label)}</span>
  <strong>{clamped}%</strong>
  <div class="coverage-bar"><span style="width: {clamped}%"></span></div>
</article>
"""


def _section_market_structure_dashboard(summary: dict[str, object]) -> str:
    crowded = _dict_payload(summary.get("crowded_territories"))
    underrepresented = _string_list(summary.get("underrepresented_spaces"))
    return f"""
<section class="panel analysis-panel" id="market-structure-dashboard">
  <div class="panel-heading">
    <span class="section-kicker">Market Structure</span>
    <h2>What does the selected competitor set cover?</h2>
  </div>
  <div class="decision-grid">
    <article class="metric-card">
      <span class="metric-label">Primary Territories</span>
      <strong>{_integer(summary.get("primary_territory_count"))}</strong>
      <p>{_html(", ".join(_string_list(summary.get("primary_territories"))) or "None mapped.")}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Multi-Axis Coverage</span>
      <strong>{_integer(summary.get("coverage_territory_count"))}</strong>
      <p>{_integer(summary.get("coverage_delta"))} territory/tories appear only when secondary signals are counted.</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Crowded Spaces</span>
      <strong>{len(crowded)}</strong>
      <p>Based on selected-set primary territory concentration.</p>
    </article>
  </div>
  <div class="two-column">
    <div>
      <h3>Primary vs Multi-Axis Coverage</h3>
      {_metric_list(_dict_payload(summary.get("coverage_comparison")), empty="No coverage comparison is available.")}
    </div>
    <div>
      <h3>Underrepresented Spaces</h3>
      {_html_list(underrepresented or ["No underrepresented territory remained after multi-axis coverage review."])}
    </div>
  </div>
</section>
"""


def _section_gap_validation_dashboard(gap: dict[str, object]) -> str:
    supported = _integer(gap.get("supported_candidates"))
    tentative = _integer(gap.get("tentative_candidates"))
    weak = _integer(gap.get("weak_candidates"))
    candidates = _list_payload(gap.get("top_candidates"))
    explanation = (
        "No gap candidate survived the current validation rules. Treat this as a conservative selected-set result, not proof the full market has no opportunity."
        if not candidates
        else "Top candidates below are validation leads. They still need external demand and concept validation before prioritization."
    )

    return f"""
<section class="panel analysis-panel" id="gap-validation-dashboard">
  <div class="panel-heading">
    <span class="section-kicker">Gap Validation</span>
    <h2>Did the run find anything worth validating?</h2>
  </div>
  <div class="decision-grid">
    <article class="metric-card"><span class="metric-label">Supported</span><strong>{supported}</strong><p>Candidate(s) with the strongest current evidence.</p></article>
    <article class="metric-card"><span class="metric-label">Tentative</span><strong>{tentative}</strong><p>Candidate(s) that need more evidence before PM action.</p></article>
    <article class="metric-card"><span class="metric-label">Weak</span><strong>{weak}</strong><p>Candidate(s) below the current validation bar.</p></article>
  </div>
  <div class="callout">
    <h3>{'Top Candidates' if candidates else 'No-Gap Explanation'}</h3>
    <p>{_html(explanation)}</p>
    {_gap_candidate_list(candidates)}
  </div>
</section>
"""


def _section_product_matrix(summary: dict[str, object]) -> str:
    rows = _list_payload(summary.get("rows"))
    if not rows:
        body = '<tr><td colspan="10">No product rows are available.</td></tr>'
    else:
        body = "".join(_product_matrix_row(row) for row in rows)
    return f"""
<section class="panel analysis-panel" id="product-evidence-matrix">
  <div class="panel-heading">
    <span class="section-kicker">Product Evidence Matrix</span>
    <h2>Compact review table</h2>
  </div>
  <div class="matrix-wrap">
    <table class="matrix-table">
      <thead>
        <tr>
          <th>Rank</th>
          <th>Brand / ASIN</th>
          <th>Territory</th>
          <th>Secondary</th>
          <th>Rating</th>
          <th>Reviews</th>
          <th>Primary</th>
          <th>Gallery</th>
          <th>Promo</th>
          <th>Warnings</th>
        </tr>
      </thead>
      <tbody>{body}</tbody>
    </table>
  </div>
</section>
"""


def _product_matrix_row(row: dict[str, object]) -> str:
    return f"""
<tr>
  <td>{_html(row.get("rank"))}</td>
  <td><strong>{_html(row.get("brand"))}</strong><br><a href="{_attr(row.get("product_url"))}">{_html(row.get("asin"))}</a></td>
  <td>{_html(row.get("primary_territory"))}</td>
  <td>{_html(", ".join(_string_list(row.get("secondary_territories"))) or "none")}</td>
  <td>{_html(row.get("rating"))}</td>
  <td>{_html(row.get("review_count"))}</td>
  <td>{'yes' if row.get("has_primary_image") else 'no'}</td>
  <td>{_html(row.get("gallery_image_count"))}</td>
  <td>{_html(row.get("promo_asset_count"))}</td>
  <td>{_html(row.get("warning_count"))}</td>
</tr>
"""


def _section_review_readiness(summary: dict[str, object]) -> str:
    product_count = _integer(summary.get("product_count"))
    primary_images = _integer(summary.get("products_with_primary_image"))
    gallery_images = _integer(summary.get("products_with_gallery_images"))
    promo_content = _integer(summary.get("products_with_promotional_content"))
    warning_products = _integer(summary.get("products_with_warnings"))
    total_warnings = _integer(summary.get("total_warning_count"))
    gap_candidates = _integer(summary.get("gap_candidate_count"))
    supported_candidates = _integer(summary.get("supported_gap_candidate_count"))
    recommendation = _text(summary.get("decision_recommendation")) or "not_available"
    primary_territories = _integer(summary.get("primary_territory_count"))
    coverage_territories = _integer(summary.get("coverage_territory_count"))
    image_coverage = _percent(primary_images, product_count)
    promo_coverage = _percent(promo_content, product_count)

    return f"""
<section class="readiness-panel" id="review-readiness">
  <div class="readiness-heading">
    <span class="section-kicker">Review Readiness</span>
    <h2>Can we use this run?</h2>
  </div>
  <div class="readiness-strip">
    <article>
      <span class="metric-label">Products</span>
      <strong>{product_count}</strong>
      <p>{image_coverage}% primary image coverage, {promo_coverage}% promo-content coverage.</p>
    </article>
    <article>
      <span class="metric-label">Evidence Assets</span>
      <strong>{primary_images}/{gallery_images}</strong>
      <p>Products with primary images / gallery images for visual review.</p>
    </article>
    <article>
      <span class="metric-label">Trust Flags</span>
      <strong>{warning_products}</strong>
      <p>{total_warnings} total product or profile warnings.</p>
    </article>
    <article>
      <span class="metric-label">Gap State</span>
      <strong>{gap_candidates}</strong>
      <p>{supported_candidates} supported; recommendation is {_html(recommendation)}.</p>
    </article>
    <article>
      <span class="metric-label">Territory Coverage</span>
      <strong>{primary_territories}/{coverage_territories}</strong>
      <p>Primary territories / primary plus secondary coverage territories.</p>
    </article>
  </div>
</section>
"""


def _section_decision(decision: dict[str, object], gap: dict[str, object]) -> str:
    top = _object_payload(decision.get("top_opportunity"))
    rationale = _string_list(decision.get("decision_rationale"))
    steps = _string_list(decision.get("recommended_next_steps"))
    blocked = _string_list(decision.get("blocked_reasons"))
    total_candidates = _integer(gap.get("total_candidates"))
    supported = _integer(gap.get("supported_candidates"))

    return f"""
<section class="panel decision-panel" id="decision">
  <div class="panel-heading">
    <span class="section-kicker">PM Decision</span>
    <h2>What should we do with this run?</h2>
  </div>
  <div class="decision-grid">
    <article class="metric-card">
      <span class="metric-label">Top Space</span>
      <strong>{_html(_text(top.get("candidate_space")) or "none")}</strong>
      <p>{_html(_text(top.get("title")) or "No selected opportunity is currently priority-ready.")}</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Gap Candidates</span>
      <strong>{total_candidates}</strong>
      <p>{supported} supported candidate(s) crossed the current threshold.</p>
    </article>
    <article class="metric-card">
      <span class="metric-label">Validation Score</span>
      <strong>{_html(str(top.get("validation_score", 0.0)))}</strong>
      <p>Evidence score from supply gap, traction, demand proxy, and price realism.</p>
    </article>
  </div>
  <div class="two-column">
    <div>
      <h3>Rationale</h3>
      {_html_list(rationale or ["No decision rationale is available yet."])}
    </div>
    <div>
      <h3>Next Steps</h3>
      {_html_list(steps or ["Run the deterministic analysis stack before making a PM call."])}
    </div>
  </div>
  {f'<div class="callout"><h3>Blocked Reasons</h3>{_html_list(blocked)}</div>' if blocked else ''}
</section>
"""


def _section_market_map(brand: dict[str, object], demand: dict[str, object], gap: dict[str, object]) -> str:
    territory_counts = _dict_payload(brand.get("territory_counts"))
    coverage_counts = _dict_payload(brand.get("territory_coverage_counts"))
    demand_signals = _list_payload(demand.get("signals"))
    top_candidates = _list_payload(gap.get("top_candidates"))

    return f"""
<section class="panel" id="market-map">
  <div class="panel-heading">
    <span class="section-kicker">Market Map</span>
    <h2>Where are competitors visibly positioned?</h2>
  </div>
  <div class="two-column">
    <div>
      <h3>Primary Territories</h3>
      {_metric_list(territory_counts, empty="No primary territory counts are available.")}
    </div>
    <div>
      <h3>Multi-Axis Coverage</h3>
      {_metric_list(coverage_counts, suffix=" profiles", empty="No primary plus secondary coverage counts are available.")}
    </div>
  </div>
  <div class="two-column">
    <div>
      <h3>Demand Proxy Signals</h3>
      {_demand_signal_list(demand_signals)}
    </div>
    <div>
      <h3>Gap Validation</h3>
      {_gap_candidate_list(top_candidates)}
    </div>
  </div>
</section>
"""


def _section_review_controls(summary: dict[str, object]) -> str:
    territory_options = _string_list(summary.get("territory_options"))
    options = "".join(
        f'<option value="{_attr(territory)}">{_html(territory)}</option>'
        for territory in territory_options
    )
    return f"""
<section class="panel review-controls" id="review-controls">
  <div class="panel-heading">
    <span class="section-kicker">Review Controls</span>
    <h2>Find the evidence that matters</h2>
  </div>
  <div class="control-grid">
    <label>
      <span>Search title, brand, ASIN, claims</span>
      <input id="product-search" type="search" placeholder="Search products or claims">
    </label>
    <label>
      <span>Territory</span>
      <select id="territory-filter">
        <option value="">All territories</option>
        {options}
      </select>
    </label>
    <label>
      <span>Evidence status</span>
      <select id="evidence-filter">
        <option value="">All products</option>
        <option value="warnings">Products with warnings</option>
        <option value="missing-promo">Missing promo content</option>
        <option value="missing-primary-image">Missing primary image</option>
      </select>
    </label>
    <label>
      <span>Sort</span>
      <select id="product-sort">
        <option value="rank">Discovery rank</option>
        <option value="reviews">Review count high to low</option>
        <option value="warnings">Warnings high to low</option>
        <option value="title">Title A to Z</option>
      </select>
    </label>
  </div>
  <p class="filter-status"><strong id="visible-products-count">{_integer(summary.get("product_count"))}</strong> visible product(s)</p>
</section>
"""


def _section_products(products: list[dict[str, object]], profiles: dict[str, dict[str, object]]) -> str:
    cards = []
    for product in products[:30]:
        asin = _text(product.get("asin")).upper()
        profile = profiles.get(asin, {})
        cards.append(_product_card(product, profile))

    if not cards:
        cards.append(
            '<article class="empty-card"><h3>No product intelligence records found.</h3><p>Run collection with product-detail enrichment before using the workbench for evidence review.</p></article>'
        )

    return f"""
<section class="panel" id="products">
  <div class="panel-heading">
    <span class="section-kicker">Evidence Cards</span>
    <h2>Packaging, promo images, copy, and positioning by product</h2>
  </div>
  <div class="product-grid">
    {''.join(cards)}
  </div>
</section>
"""


def _product_card(product: dict[str, object], profile: dict[str, object]) -> str:
    title = _text(product.get("title")) or "Untitled product"
    brand = _text(product.get("brand")) or _text(profile.get("brand_name")) or "Unknown brand"
    asin = _text(product.get("asin")).upper()
    price = product.get("price")
    currency = _text(product.get("currency"))
    price_display = "price missing" if price in {None, ""} else f"{currency + ' ' if currency else ''}{price}"
    rating = product.get("rating")
    reviews = product.get("review_count")
    territory = _text(profile.get("positioning_territory")) or "unmapped"
    secondary = _string_list(profile.get("secondary_territories"))
    territories = _dedupe([territory, *secondary])
    claims = _string_list(profile.get("primary_claims"))
    bullets = _string_list(product.get("description_bullets"))[:4]
    promo = _list_payload(product.get("promotional_content"))[:3]
    media = _dict_payload(product.get("media_assets"))
    primary_image = _text(media.get("primary_image"))
    gallery_count = len(_string_list(media.get("gallery_images")))
    promo_image_count = len(_string_list(media.get("promotional_images")))
    promo_block_count = len(_list_payload(product.get("promotional_content")))
    discovery_rank = _text(product.get("discovery_rank"))
    sponsored = product.get("sponsored")
    warnings = [*_string_list(product.get("warnings")), *_string_list(product.get("issues")), *_string_list(profile.get("warnings"))]
    warning_count = len(_dedupe(warnings))
    review_count = _integer(reviews)
    rank_value = _integer(discovery_rank) or 999999
    search_text = " ".join([title, brand, asin, territory, *secondary, *claims, *bullets]).lower()
    territory_data = "|".join(territories)

    return f"""
<article class="product-card"
  data-search="{_attr(search_text)}"
  data-territories="{_attr(territory_data)}"
  data-warning-count="{warning_count}"
  data-promo-count="{promo_block_count + promo_image_count}"
  data-has-primary-image="{str(bool(primary_image)).lower()}"
  data-review-count="{review_count}"
  data-rank="{rank_value}">
  {_image_strip(product)}
  <div class="product-body">
    <div class="product-meta">{_html(brand)} <span>{_html(asin)}</span></div>
    <h3>{_html(title)}</h3>
    <div class="facts">
      <span>{_html(price_display)}</span>
      <span>rating {_html(str(rating or "missing"))}</span>
      <span>{_html(str(reviews or "0"))} reviews</span>
      <span>{gallery_count} gallery</span>
      <span>{promo_image_count} promo images</span>
      <span>{promo_block_count} promo blocks</span>
      {f'<span>rank {_html(discovery_rank)}</span>' if discovery_rank else ''}
      {f'<span>sponsored {_html(str(sponsored).lower())}</span>' if isinstance(sponsored, bool) else ''}
    </div>
    <div class="territory">
      <span class="tag primary">{_html(territory)}</span>
      {''.join(f'<span class="tag">{_html(item)}</span>' for item in secondary)}
    </div>
    <details open>
      <summary>Description evidence</summary>
      {_html_list(bullets or ["No description bullets available."])}
    </details>
    <details>
      <summary>Promotional evidence</summary>
      {_promo_list(promo)}
    </details>
    <details>
      <summary>Claims and warnings</summary>
      <div class="mini-columns">
        <div><h4>Claims</h4>{_html_list(claims or ["No primary claims mapped."])}</div>
        <div><h4>Warnings</h4>{_html_list(_dedupe(warnings) or ["No product-level warnings."])}</div>
      </div>
    </details>
  </div>
</article>
"""


def _image_strip(product: dict[str, object]) -> str:
    media = _dict_payload(product.get("media_assets"))
    primary = _text(media.get("primary_image"))
    gallery = _string_list(media.get("gallery_images"))
    promo_images = _string_list(media.get("promotional_images"))
    promo_content = _list_payload(product.get("promotional_content"))
    promo_from_content = [
        _text(item.get("image"))
        for item in promo_content
        if isinstance(item, dict) and _text(item.get("image"))
    ]
    images = _dedupe([item for item in [primary, *gallery, *promo_images, *promo_from_content] if item])[:5]
    if not images:
        return '<div class="image-missing">No image assets captured</div>'
    hero = images[0]
    thumbs = images[1:]
    return f"""
  <div class="image-strip">
    <img class="hero-image" src="{_attr(hero)}" alt="{_attr(_text(product.get("title")) or "Product image")}" loading="lazy">
    <div class="thumb-row">
      {''.join(f'<img src="{_attr(src)}" alt="Supporting product image" loading="lazy">' for src in thumbs)}
    </div>
  </div>
"""


def _section_trust(
    decision: dict[str, object],
    analysis: dict[str, object],
    brand: dict[str, object],
    demand: dict[str, object],
    gap: dict[str, object],
    review_summary: dict[str, object],
    source_artifacts: dict[str, Path],
    output_dir: Path,
) -> str:
    warnings = _dedupe(
        [
            *_string_list(decision.get("quality_warnings")),
            *_string_list(analysis.get("warnings")),
            *_string_list(brand.get("caveats")),
            *_string_list(demand.get("caveats")),
            *_string_list(gap.get("caveats")),
        ]
    )
    links = []
    for label, path in source_artifacts.items():
        if not path.exists():
            continue
        links.append(
            f'<a href="{_attr(_relative_href(path, output_dir))}">{_html(label.replace("_", " "))}</a>'
        )
    warning_breakdown = _dict_payload(review_summary.get("warning_breakdown"))

    return f"""
<section class="panel trust-panel" id="trust">
  <div class="panel-heading">
    <span class="section-kicker">Trust Rails</span>
    <h2>What should the reviewer keep in mind?</h2>
  </div>
  <div class="two-column">
    <div>
      <h3>Warnings and Caveats</h3>
      {_html_list(warnings or ["No caveats were found in the loaded artifacts."])}
    </div>
    <div>
      <h3>Top Warning Types</h3>
      {_metric_list(warning_breakdown, empty="No product or profile warning types were found.")}
      <h3 class="artifact-heading">Source Artifacts</h3>
      <div class="artifact-links">{''.join(links) if links else '<span>No source artifacts found.</span>'}</div>
      <p class="note">This page is a review surface only. The JSON artifacts remain the source of truth.</p>
    </div>
  </div>
</section>
"""


def _script() -> str:
    return """
(() => {
  const cards = Array.from(document.querySelectorAll('.product-card'));
  const grid = document.querySelector('.product-grid');
  const searchInput = document.getElementById('product-search');
  const territoryFilter = document.getElementById('territory-filter');
  const evidenceFilter = document.getElementById('evidence-filter');
  const sortSelect = document.getElementById('product-sort');
  const countNode = document.getElementById('visible-products-count');

  function numberValue(card, name) {
    const value = Number(card.dataset[name] || 0);
    return Number.isFinite(value) ? value : 0;
  }

  function cardMatches(card) {
    const query = (searchInput?.value || '').trim().toLowerCase();
    const territory = territoryFilter?.value || '';
    const evidence = evidenceFilter?.value || '';
    const warningCount = numberValue(card, 'warningCount');
    const promoCount = numberValue(card, 'promoCount');
    const hasPrimaryImage = card.dataset.hasPrimaryImage === 'true';

    if (query && !(card.dataset.search || '').includes(query)) return false;
    if (territory && !(card.dataset.territories || '').split('|').includes(territory)) return false;
    if (evidence === 'warnings' && warningCount <= 0) return false;
    if (evidence === 'missing-promo' && promoCount > 0) return false;
    if (evidence === 'missing-primary-image' && hasPrimaryImage) return false;
    return true;
  }

  function sortCards(visibleCards) {
    const sortMode = sortSelect?.value || 'rank';
    const sorted = [...visibleCards].sort((left, right) => {
      if (sortMode === 'reviews') return numberValue(right, 'reviewCount') - numberValue(left, 'reviewCount');
      if (sortMode === 'warnings') return numberValue(right, 'warningCount') - numberValue(left, 'warningCount');
      if (sortMode === 'title') return (left.querySelector('h3')?.textContent || '').localeCompare(right.querySelector('h3')?.textContent || '');
      return numberValue(left, 'rank') - numberValue(right, 'rank');
    });
    sorted.forEach(card => grid?.appendChild(card));
  }

  function applyFilters() {
    const visibleCards = [];
    cards.forEach(card => {
      const visible = cardMatches(card);
      card.hidden = !visible;
      if (visible) visibleCards.push(card);
    });
    sortCards(visibleCards);
    if (countNode) countNode.textContent = String(visibleCards.length);
  }

  [searchInput, territoryFilter, evidenceFilter, sortSelect].forEach(control => {
    control?.addEventListener('input', applyFilters);
    control?.addEventListener('change', applyFilters);
  });
  applyFilters();
})();
"""


def _css() -> str:
    return """
:root {
  --paper: #f7f0e4;
  --card: #fffaf0;
  --ink: #1f2a24;
  --muted: #6b705f;
  --line: #ded1b8;
  --moss: #466b4c;
  --moss-dark: #294934;
  --clay: #b96d3a;
  --gold: #d5a642;
  --mist: #e8eee3;
  --shadow: 0 24px 80px rgba(31, 42, 36, 0.13);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background:
    radial-gradient(circle at 10% 10%, rgba(213, 166, 66, 0.22), transparent 28rem),
    radial-gradient(circle at 90% 0%, rgba(70, 107, 76, 0.18), transparent 34rem),
    linear-gradient(145deg, #f7f0e4 0%, #edf1e6 100%);
  color: var(--ink);
  font-family: "Aptos", "Segoe UI", sans-serif;
}
.shell { width: min(1180px, calc(100% - 32px)); margin: 0 auto; padding: 36px 0 64px; }
.hero {
  border: 1px solid rgba(70, 107, 76, 0.24);
  border-radius: 34px;
  padding: clamp(24px, 5vw, 52px);
  background: linear-gradient(135deg, rgba(255, 250, 240, 0.9), rgba(232, 238, 227, 0.86));
  box-shadow: var(--shadow);
  overflow: hidden;
  position: relative;
}
.hero:after {
  content: "";
  position: absolute;
  right: -110px;
  top: -110px;
  width: 260px;
  height: 260px;
  border-radius: 999px;
  background: repeating-linear-gradient(45deg, rgba(185, 109, 58, 0.14) 0 8px, transparent 8px 18px);
}
.eyebrow, .section-kicker {
  color: var(--clay);
  font-size: 0.78rem;
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}
.hero-grid { display: grid; grid-template-columns: minmax(0, 1fr) 330px; gap: 30px; align-items: start; position: relative; z-index: 1; }
h1, h2, h3 { font-family: "Georgia", "Cambria", serif; line-height: 1.05; margin: 0; }
h1 { max-width: 820px; font-size: clamp(2.5rem, 6vw, 5.6rem); letter-spacing: -0.06em; }
h2 { font-size: clamp(1.8rem, 3vw, 3.2rem); letter-spacing: -0.04em; }
h3 { font-size: 1.24rem; margin-bottom: 12px; }
h4 { margin: 8px 0; font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
.lede { max-width: 760px; font-size: 1.12rem; line-height: 1.7; color: #354238; }
.run-card, .metric-card, .product-card, .empty-card {
  background: rgba(255, 250, 240, 0.82);
  border: 1px solid var(--line);
  border-radius: 24px;
  box-shadow: 0 10px 36px rgba(31, 42, 36, 0.08);
}
.run-card { padding: 20px; }
dl { margin: 0; display: grid; gap: 14px; }
dt { color: var(--muted); font-size: 0.72rem; letter-spacing: 0.12em; text-transform: uppercase; }
dd { margin: 3px 0 0; font-weight: 800; word-break: break-word; }
.pill, .tag {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 6px 10px;
  font-size: 0.78rem;
  font-weight: 800;
  background: var(--mist);
  color: var(--moss-dark);
}
.status-success { background: #dfeede; color: #24452f; }
.status-partial-success { background: #f4e2b9; color: #725015; }
.status-failed { background: #f3cbc2; color: #7c271d; }
.recommendation { background: #20362a; color: #fdf8ec; }
.panel {
  margin-top: 28px;
  padding: clamp(20px, 4vw, 36px);
  border: 1px solid rgba(31, 42, 36, 0.14);
  border-radius: 30px;
  background: rgba(255, 250, 240, 0.72);
  backdrop-filter: blur(12px);
}
.panel-heading { margin-bottom: 22px; }
.dashboard-summary {
  margin-top: 24px;
  padding: clamp(20px, 4vw, 34px);
  border: 1px solid rgba(31, 42, 36, 0.16);
  border-radius: 30px;
  background: rgba(31, 42, 36, 0.92);
  color: #fffaf0;
  box-shadow: var(--shadow);
}
.dashboard-summary .section-kicker { color: #f2c96d; }
.dashboard-grid {
  display: grid;
  grid-template-columns: 1.4fr 1fr;
  gap: 16px;
}
.summary-card {
  padding: 18px;
  border: 1px solid rgba(255, 250, 240, 0.18);
  border-radius: 22px;
  background: rgba(255, 250, 240, 0.08);
}
.summary-card.primary, .summary-card.next-step { grid-column: span 2; }
.summary-card strong {
  display: block;
  margin: 8px 0;
  font-family: "Georgia", "Cambria", serif;
  font-size: clamp(1.45rem, 3vw, 2.4rem);
  line-height: 1.05;
}
.summary-card p { margin: 0; color: rgba(255, 250, 240, 0.72); line-height: 1.5; }
.summary-card ul.clean-list li {
  background: rgba(255, 250, 240, 0.1);
  border-left-color: #f2c96d;
}
.quality-grid {
  display: grid;
  grid-template-columns: 1.2fr repeat(3, 1fr);
  gap: 16px;
}
.coverage-card {
  padding: 18px;
  border: 1px solid var(--line);
  border-radius: 24px;
  background: rgba(255, 250, 240, 0.82);
}
.coverage-card strong {
  display: block;
  margin: 8px 0;
  font-family: "Georgia", "Cambria", serif;
  font-size: 2rem;
}
.coverage-bar {
  height: 10px;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(70, 107, 76, 0.14);
}
.coverage-bar span {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: linear-gradient(90deg, var(--moss), var(--gold));
}
.matrix-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 20px;
  background: rgba(255, 250, 240, 0.82);
}
.matrix-table {
  width: 100%;
  min-width: 920px;
  border-collapse: collapse;
  font-size: 0.9rem;
}
.matrix-table th, .matrix-table td {
  padding: 12px;
  border-bottom: 1px solid rgba(222, 209, 184, 0.8);
  text-align: left;
  vertical-align: top;
}
.matrix-table th {
  color: var(--clay);
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.matrix-table a { color: var(--moss-dark); font-weight: 800; }
.readiness-panel {
  margin-top: 20px;
}
.readiness-heading {
  margin: 0 0 12px;
}
.readiness-heading h2 {
  font-size: clamp(1.5rem, 2.4vw, 2.5rem);
}
.readiness-strip {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 14px;
}
.readiness-strip article {
  padding: 16px;
  border: 1px solid rgba(70, 107, 76, 0.18);
  border-radius: 20px;
  background: rgba(255, 250, 240, 0.78);
  box-shadow: 0 8px 28px rgba(31, 42, 36, 0.07);
}
.readiness-strip strong {
  display: block;
  margin: 5px 0;
  font-family: "Georgia", "Cambria", serif;
  font-size: 2.15rem;
}
.readiness-strip p {
  margin: 0;
  color: var(--muted);
  line-height: 1.38;
  font-size: 0.9rem;
}
.decision-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }
.metric-card { padding: 18px; }
.metric-card strong { display: block; margin: 7px 0; font-size: 2rem; font-family: "Georgia", "Cambria", serif; }
.metric-card p, .note { color: var(--muted); line-height: 1.5; margin: 0; }
.metric-label { color: var(--clay); font-weight: 800; font-size: 0.74rem; letter-spacing: 0.1em; text-transform: uppercase; }
.control-grid { display: grid; grid-template-columns: 1.4fr 1fr 1fr 1fr; gap: 14px; }
.control-grid label { display: grid; gap: 6px; color: var(--muted); font-weight: 800; font-size: 0.78rem; letter-spacing: 0.06em; text-transform: uppercase; }
.control-grid input, .control-grid select {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(255, 250, 240, 0.96);
  color: var(--ink);
  font: inherit;
  letter-spacing: 0;
  text-transform: none;
  padding: 11px 12px;
}
.filter-status { margin: 14px 0 0; color: var(--muted); }
.two-column { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 22px; margin-top: 22px; }
ul.clean-list { list-style: none; margin: 0; padding: 0; display: grid; gap: 9px; }
ul.clean-list li { padding: 10px 12px; border-left: 4px solid var(--gold); background: rgba(255, 255, 255, 0.46); border-radius: 12px; line-height: 1.45; }
.callout { margin-top: 20px; padding: 16px; border-radius: 18px; background: rgba(185, 109, 58, 0.12); border: 1px solid rgba(185, 109, 58, 0.22); }
.product-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; }
.product-card { overflow: hidden; }
.product-card[hidden] { display: none; }
.image-strip { background: #f2eadb; padding: 12px; }
.hero-image { width: 100%; height: 270px; object-fit: contain; border-radius: 18px; background: white; display: block; }
.thumb-row { display: flex; gap: 8px; margin-top: 8px; overflow-x: auto; }
.thumb-row img { width: 72px; height: 72px; object-fit: cover; border-radius: 14px; background: white; border: 1px solid var(--line); }
.image-missing { display: grid; place-items: center; min-height: 190px; color: var(--muted); background: repeating-linear-gradient(135deg, #f2eadb, #f2eadb 10px, #eadfc9 10px, #eadfc9 20px); }
.product-body { padding: 18px; }
.product-meta { color: var(--muted); font-size: 0.8rem; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase; }
.product-meta span { float: right; color: var(--clay); }
.facts, .territory { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
.facts span { padding: 7px 9px; border-radius: 10px; background: rgba(70, 107, 76, 0.1); font-size: 0.84rem; }
.tag.primary { background: var(--moss); color: #fffaf0; }
details { border-top: 1px solid var(--line); padding-top: 11px; margin-top: 11px; }
summary { cursor: pointer; font-weight: 900; color: var(--moss-dark); }
.mini-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }
.artifact-heading { margin-top: 20px; }
.artifact-links { display: flex; flex-wrap: wrap; gap: 10px; }
.artifact-links a { color: #fffaf0; background: var(--moss-dark); text-decoration: none; padding: 9px 12px; border-radius: 999px; font-weight: 800; }
@media (max-width: 860px) {
  .hero-grid, .two-column, .decision-grid, .product-grid, .mini-columns, .readiness-strip, .control-grid, .dashboard-grid, .quality-grid { grid-template-columns: 1fr; }
  .summary-card.primary, .summary-card.next-step { grid-column: auto; }
  .hero-image { height: 220px; }
}
"""


def _source_artifacts(collection_dir: Path) -> dict[str, Path]:
    return {
        "data_collection_report": collection_dir / "data_collection_report.json",
        "selection_report": collection_dir / "selection_report.json",
        "selected_candidates": collection_dir / "selected_candidates.json",
        "product_intelligence_records": collection_dir / "product_intelligence" / "product_intelligence_records.json",
        "brand_profile_records": collection_dir / "brand_profiles" / "brand_profile_records.json",
        "brand_profile_report": collection_dir / "brand_profiles" / "brand_profile_report.json",
        "demand_signal_report": collection_dir / "demand_signals" / "demand_signal_report.json",
        "gap_validation_report": collection_dir / "gap_validation" / "gap_validation_report.json",
        "decision_brief_report": collection_dir / "decision_brief" / "decision_brief_report.json",
        "analysis_stack_report": collection_dir / "analysis_stack" / "analysis_stack_report.json",
    }


def _build_review_summary(payloads: dict[str, object]) -> dict[str, object]:
    products = _list_payload(payloads.get("product_intelligence_records"))
    decision = _object_payload(payloads.get("decision_brief_report"))
    gap = _object_payload(payloads.get("gap_validation_report"))
    brand = _object_payload(payloads.get("brand_profile_report"))
    profiles = _profile_lookup(brand, payloads.get("brand_profile_records"))

    products_with_primary_image = 0
    products_with_gallery_images = 0
    products_with_promotional_content = 0
    products_with_warnings = 0
    total_warning_count = 0
    warning_breakdown: dict[str, int] = {}
    territory_options: list[str] = []

    for product in products:
        asin = _text(product.get("asin")).upper()
        profile = profiles.get(asin, {})
        media = _dict_payload(product.get("media_assets"))
        if _text(media.get("primary_image")):
            products_with_primary_image += 1
        if _string_list(media.get("gallery_images")):
            products_with_gallery_images += 1
        if _list_payload(product.get("promotional_content")) or _string_list(media.get("promotional_images")):
            products_with_promotional_content += 1
        warnings = _dedupe(
            [
                *_string_list(product.get("warnings")),
                *_string_list(product.get("issues")),
                *_string_list(profile.get("warnings")),
            ]
        )
        if warnings:
            products_with_warnings += 1
            total_warning_count += len(warnings)
            for warning in warnings:
                label = _normalize_warning_label(warning)
                warning_breakdown[label] = warning_breakdown.get(label, 0) + 1
        territory = _text(profile.get("positioning_territory"))
        if territory:
            territory_options.append(territory)
        territory_options.extend(_string_list(profile.get("secondary_territories")))

    product_count = len(products)

    return {
        "product_count": product_count,
        "products_with_primary_image": products_with_primary_image,
        "products_with_gallery_images": products_with_gallery_images,
        "products_with_promotional_content": products_with_promotional_content,
        "products_missing_primary_image": max(0, product_count - products_with_primary_image),
        "products_missing_promotional_content": max(0, product_count - products_with_promotional_content),
        "products_with_warnings": products_with_warnings,
        "total_warning_count": total_warning_count,
        "warning_breakdown": dict(sorted(warning_breakdown.items(), key=lambda item: (-item[1], item[0]))),
        "territory_options": sorted(set(territory_options)),
        "decision_recommendation": _text(decision.get("recommendation_level")),
        "gap_candidate_count": _integer(gap.get("total_candidates")),
        "supported_gap_candidate_count": _integer(gap.get("supported_candidates")),
        "primary_territory_count": len(_dict_payload(brand.get("territory_counts"))),
        "coverage_territory_count": len(_dict_payload(brand.get("territory_coverage_counts"))),
    }


def _build_evidence_quality_summary(review_summary: dict[str, object]) -> dict[str, object]:
    product_count = _integer(review_summary.get("product_count"))
    primary_image_coverage = _ratio(_integer(review_summary.get("products_with_primary_image")), product_count)
    gallery_image_coverage = _ratio(_integer(review_summary.get("products_with_gallery_images")), product_count)
    promo_content_coverage = _ratio(_integer(review_summary.get("products_with_promotional_content")), product_count)
    warning_coverage = _ratio(_integer(review_summary.get("products_with_warnings")), product_count)
    quality_score = round(
        max(
            0.0,
            min(
                1.0,
                (primary_image_coverage * 0.30)
                + (gallery_image_coverage * 0.20)
                + (promo_content_coverage * 0.25)
                + ((1 - warning_coverage) * 0.25),
            ),
        ),
        2,
    )
    if quality_score >= 0.85:
        quality_label = "strong_review_ready"
    elif quality_score >= 0.65:
        quality_label = "reviewable_with_caveats"
    elif quality_score >= 0.45:
        quality_label = "weak_review_ready"
    else:
        quality_label = "not_review_ready"

    warning_breakdown = _dict_payload(review_summary.get("warning_breakdown"))
    missing_flags: list[str] = []
    if _integer(review_summary.get("products_missing_primary_image")):
        missing_flags.append(f"{review_summary.get('products_missing_primary_image')} product(s) missing primary image.")
    if _integer(review_summary.get("products_missing_promotional_content")):
        missing_flags.append(f"{review_summary.get('products_missing_promotional_content')} product(s) missing promo content.")
    if _integer(review_summary.get("total_warning_count")):
        missing_flags.append(f"{review_summary.get('total_warning_count')} total warning(s) need review.")
    if warning_breakdown.get("currency missing"):
        missing_flags.append("Currency warnings are high enough to caveat price-lane conclusions.")
    if warning_breakdown.get("possible detail contamination"):
        missing_flags.append("Possible detail contamination should be inspected before using brand-positioning claims.")

    return {
        "product_count": product_count,
        "primary_image_coverage": primary_image_coverage,
        "gallery_image_coverage": gallery_image_coverage,
        "promo_content_coverage": promo_content_coverage,
        "products_with_warnings": _integer(review_summary.get("products_with_warnings")),
        "total_warning_count": _integer(review_summary.get("total_warning_count")),
        "top_warning_types": _top_warning_types(review_summary),
        "quality_score": quality_score,
        "quality_label": quality_label,
        "missing_evidence_flags": missing_flags,
    }


def _top_warning_types(review_summary: dict[str, object], *, limit: int = 5) -> dict[str, int]:
    warning_breakdown = _dict_payload(review_summary.get("warning_breakdown"))
    sorted_items = sorted(
        ((str(key), _integer(value)) for key, value in warning_breakdown.items()),
        key=lambda item: (-item[1], item[0]),
    )
    return dict(sorted_items[:limit])


def _build_market_structure_summary(payloads: dict[str, object]) -> dict[str, object]:
    brand = _object_payload(payloads.get("brand_profile_report"))
    primary_counts = _dict_payload(brand.get("territory_counts"))
    coverage_counts = _dict_payload(brand.get("territory_coverage_counts"))
    primary_territories = sorted(str(key) for key in primary_counts)
    coverage_territories = sorted(str(key) for key in coverage_counts)
    coverage_comparison = {
        territory: f"primary {primary_counts.get(territory, 0)}, coverage {coverage_counts.get(territory, 0)}"
        for territory in coverage_territories
    }
    crowded = _dict_payload(brand.get("crowded_territories"))
    if not crowded and primary_counts:
        crowded = {
            str(territory): count
            for territory, count in primary_counts.items()
            if _integer(count) >= 3
        }

    return {
        "primary_territory_count": len(primary_territories),
        "coverage_territory_count": len(coverage_territories),
        "primary_territories": primary_territories,
        "coverage_territories": coverage_territories,
        "crowded_territories": crowded,
        "underrepresented_spaces": _string_list(brand.get("underrepresented_spaces")),
        "coverage_delta": max(0, len(coverage_territories) - len(primary_territories)),
        "coverage_comparison": coverage_comparison,
    }


def _build_product_matrix_summary(payloads: dict[str, object]) -> dict[str, object]:
    products = _list_payload(payloads.get("product_intelligence_records"))
    brand = _object_payload(payloads.get("brand_profile_report"))
    profiles = _profile_lookup(brand, payloads.get("brand_profile_records"))
    rows = [_product_matrix_summary_row(product, profiles.get(_text(product.get("asin")).upper(), {})) for product in products]
    rows.sort(key=lambda row: _integer(row.get("rank")) or 999999)
    return {
        "total_rows": len(rows),
        "rows": rows,
    }


def _product_matrix_summary_row(product: dict[str, object], profile: dict[str, object]) -> dict[str, object]:
    media = _dict_payload(product.get("media_assets"))
    promo_asset_count = len(_string_list(media.get("promotional_images"))) + len(_list_payload(product.get("promotional_content")))
    warnings = _dedupe(
        [
            *_string_list(product.get("warnings")),
            *_string_list(product.get("issues")),
            *_string_list(profile.get("warnings")),
        ]
    )
    return {
        "asin": _text(product.get("asin")).upper(),
        "brand": _text(product.get("brand")) or _text(profile.get("brand_name")) or "Unknown brand",
        "title": _text(product.get("title")) or "Untitled product",
        "rank": _integer(product.get("discovery_rank")),
        "primary_territory": _text(profile.get("positioning_territory")) or "unmapped",
        "secondary_territories": _string_list(profile.get("secondary_territories")),
        "rating": product.get("rating"),
        "review_count": _integer(product.get("review_count")),
        "has_primary_image": bool(_text(media.get("primary_image"))),
        "gallery_image_count": len(_string_list(media.get("gallery_images"))),
        "promo_asset_count": promo_asset_count,
        "warning_count": len(warnings),
        "product_url": _text(product.get("product_url")),
    }


def _build_dashboard_summary(
    *,
    payloads: dict[str, object],
    review_summary: dict[str, object],
    evidence_quality_summary: dict[str, object],
    market_structure_summary: dict[str, object],
) -> dict[str, object]:
    decision = _object_payload(payloads.get("decision_brief_report"))
    gap = _object_payload(payloads.get("gap_validation_report"))
    brand = _object_payload(payloads.get("brand_profile_report"))
    recommendation = _text(decision.get("recommendation_level")) or "not_available"
    supported_candidates = _integer(review_summary.get("supported_gap_candidate_count"))
    total_candidates = _integer(review_summary.get("gap_candidate_count"))
    category = _first_text(decision.get("category_context"), brand.get("category_context"), gap.get("category_context")) or "none"
    headline = _text(decision.get("headline")) or "No decision brief has been generated yet."

    if supported_candidates:
        run_conclusion = f"{supported_candidates} supported gap candidate(s) found for validation."
    elif total_candidates:
        run_conclusion = f"{total_candidates} gap candidate(s) found, but none reached supported status."
    elif recommendation == "do_not_prioritize_yet":
        run_conclusion = "No priority gap found in this selected set."
    else:
        run_conclusion = "Run conclusion is not decision-ready yet."

    quality_label = _text(evidence_quality_summary.get("quality_label"))
    if quality_label == "strong_review_ready":
        review_readiness = "Ready for PM evidence review."
    elif quality_label == "reviewable_with_caveats":
        review_readiness = "Reviewable, with caveats visible."
    elif quality_label == "weak_review_ready":
        review_readiness = "Weak review readiness; use for debugging only."
    else:
        review_readiness = "Not ready for PM decision review."

    strong_evidence: list[str] = []
    weak_evidence: list[str] = []
    product_count = _integer(review_summary.get("product_count"))
    if product_count >= 10:
        strong_evidence.append(f"{product_count} selected products provide a useful comparison set.")
    if evidence_quality_summary.get("primary_image_coverage") == 1.0:
        strong_evidence.append("Primary image coverage is complete.")
    if evidence_quality_summary.get("gallery_image_coverage") == 1.0:
        strong_evidence.append("Gallery image coverage is complete.")
    if evidence_quality_summary.get("promo_content_coverage") == 1.0:
        strong_evidence.append("Promotional content coverage is complete.")
    if _integer(market_structure_summary.get("coverage_delta")) > 0:
        strong_evidence.append("Multi-axis territory coverage catches secondary positioning lanes.")

    missing_flags = _string_list(evidence_quality_summary.get("missing_evidence_flags"))
    weak_evidence.extend(missing_flags)
    if not weak_evidence and not supported_candidates:
        weak_evidence.append("No supported gap candidate was found; treat the output as a conservative baseline.")

    blocked_reasons = _string_list(decision.get("blocked_reasons"))
    next_steps = _string_list(decision.get("recommended_next_steps"))
    if supported_candidates:
        pm_next_step = "Move the supported candidate(s) into controlled validation, not launch."
    elif recommendation == "do_not_prioritize_yet":
        pm_next_step = next_steps[0] if next_steps else "Use this as a baseline and test a sharper subcategory query."
    elif blocked_reasons:
        pm_next_step = "Resolve blocked reasons before prioritizing the run."
    else:
        pm_next_step = next_steps[0] if next_steps else "Rerun with stronger evidence before making a PM call."

    return {
        "run_conclusion": run_conclusion,
        "review_readiness": review_readiness,
        "strong_evidence": strong_evidence,
        "weak_evidence": weak_evidence,
        "pm_next_step": pm_next_step,
        "recommendation_level": recommendation,
        "category_context": category,
        "headline": headline,
    }


def _workbench_caveats(payloads: dict[str, object]) -> list[str]:
    caveats: list[str] = []
    for required in ("product_intelligence_records", "decision_brief_report"):
        if required not in payloads:
            caveats.append(f"{required} was not found, so the workbench is incomplete.")
    return caveats


def _infer_run_id(payloads: dict[str, object], collection_dir: Path) -> str:
    for payload_name in ("decision_brief_report", "analysis_stack_report", "data_collection_report", "brand_profile_report"):
        payload = _object_payload(payloads.get(payload_name))
        run_id = _text(payload.get("run_id"))
        if run_id:
            return run_id
    return collection_dir.name


def _profile_lookup(brand_report: dict[str, object], brand_records_payload: object) -> dict[str, dict[str, object]]:
    records = _list_payload(brand_report.get("profiles"))
    if not records:
        records = _list_payload(brand_records_payload)
    return {
        _text(record.get("asin")).upper(): record
        for record in records
        if _text(record.get("asin"))
    }


def _demand_signal_list(signals: list[dict[str, object]]) -> str:
    if not signals:
        return _html_list(["No demand signals are available."])
    items = []
    for signal in signals[:8]:
        territory = _text(signal.get("target_territory")) or "unknown territory"
        score = signal.get("demand_score", "missing")
        count = signal.get("matching_discovery_count", signal.get("candidate_count", "unknown"))
        items.append(f"{territory}: demand score {score}, matches {count}")
    return _html_list(items)


def _gap_candidate_list(candidates: list[dict[str, object]]) -> str:
    if not candidates:
        return _html_list(["No candidate gaps were generated after coverage review."])
    items = []
    for candidate in candidates[:5]:
        space = _text(candidate.get("candidate_space")) or "unknown space"
        status = _text(candidate.get("status")) or "unknown"
        score = candidate.get("validation_score", "missing")
        items.append(f"{space}: {status}, validation score {score}")
    return _html_list(items)


def _promo_list(items: list[dict[str, object]]) -> str:
    if not items:
        return _html_list(["No promotional content blocks available."])
    labels = []
    for item in items:
        title = _text(item.get("title")) or _text(item.get("headline")) or "Untitled promotional block"
        labels.append(title)
    return _html_list(labels)


def _metric_list(values: dict[str, object], *, suffix: str = "", empty: str) -> str:
    if not values:
        return _html_list([empty])
    return _html_list([f"{key}: {value}{suffix}" for key, value in values.items()])


def _html_list(items: list[str]) -> str:
    return '<ul class="clean-list">' + "".join(f"<li>{_html(item)}</li>" for item in items) + "</ul>"


def _load_json_optional(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _object_payload(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _dict_payload(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _list_payload(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _dedupe(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def _integer(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _percent(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return round((numerator / denominator) * 100)


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 2)


def _normalize_warning_label(warning: str) -> str:
    normalized = warning.lower()
    if "currency" in normalized:
        return "currency missing"
    if "promotional" in normalized or "promo" in normalized:
        return "promotional content missing"
    if "gallery" in normalized or "image" in normalized:
        return "image evidence missing"
    if "contaminated" in normalized or "content family" in normalized or "narrative content dropped" in normalized:
        return "possible detail contamination"
    return warning[:80]


def _first_text(*values: object) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _html(value: object) -> str:
    return escape(str(value), quote=False)


def _attr(value: object) -> str:
    return escape(str(value), quote=True)


def _status_class(status: str) -> str:
    normalized = status.replace("_", "-").lower()
    if normalized in {"success", "partial-success", "failed"}:
        return f"status-{normalized}"
    return "status-unknown"


def _relative_href(path: Path, output_dir: Path) -> str:
    return Path(os.path.relpath(path, output_dir)).as_posix()


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
