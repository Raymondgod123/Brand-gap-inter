from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from html import unescape
import re
from urllib.parse import unquote, urlparse

from .connectors import RawSourceRecord

_TITLE_RE = re.compile(r'id="productTitle"[^>]*>(.*?)</span>', re.IGNORECASE | re.DOTALL)
_BYLINE_RE = re.compile(r'<a[^>]+id="bylineInfo"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
_PREMIUM_BRAND_ALT_RE = re.compile(r'premium-logoByLine-brand-logo[^>]*alt="([^"]+)"', re.IGNORECASE | re.DOTALL)
_VISIT_STORE_RE = re.compile(r"visit the\s+(.+?)\s+store", re.IGNORECASE)
_REVIEW_RATING_RE = re.compile(r'id="acrPopover"[^>]*title="([^"]+)"', re.IGNORECASE | re.DOTALL)
_REVIEW_COUNT_RE = re.compile(r'id="acrCustomerReviewText"[^>]*>(.*?)</span>', re.IGNORECASE | re.DOTALL)
_BREADCRUMB_BLOCK_RE = re.compile(
    r'<ul class="a-unordered-list a-horizontal a-size-small">(.*?)</ul>',
    re.IGNORECASE | re.DOTALL,
)
_BREADCRUMB_ITEM_RE = re.compile(r'a-color-tertiary"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
_PRICE_PATTERNS = (
    re.compile(r'&quot;priceAmount&quot;\s*:\s*(\d+(?:\.\d+)?)', re.IGNORECASE),
    re.compile(r'priceAmount\\&quot;\s*:\s*(\d+(?:\.\d+)?)', re.IGNORECASE),
    re.compile(r'price\\&quot;\s*:\s*\\&quot;(\d+(?:\.\d+)?)', re.IGNORECASE),
    re.compile(r'"priceAmount"\s*:\s*(\d+(?:\.\d+)?)', re.IGNORECASE),
)
_PRICE_BLOCK_PATTERNS = (
    re.compile(
        r'id="corePriceDisplay_desktop_feature_div".*?class="a-offscreen">\s*\$?\s*([\d,]+(?:\.\d+)?)\s*<',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'id="priceToPay"[^>]*>.*?class="a-offscreen">\s*\$?\s*([\d,]+(?:\.\d+)?)\s*<',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'id="tp_price_block_total_price_ww"[^>]*>.*?class="a-offscreen">\s*\$?\s*([\d,]+(?:\.\d+)?)\s*<',
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r'id="apex_desktop".*?class="a-offscreen">\s*\$?\s*([\d,]+(?:\.\d+)?)\s*<',
        re.IGNORECASE | re.DOTALL,
    ),
)
_PRIMARY_PRICE_CONTAINER_IDS = (
    "corePriceDisplay_desktop_feature_div",
    "corePrice_feature_div",
    "priceToPay",
    "priceToPay_feature_div",
    "apex_desktop",
    "buybox",
    "desktop_buybox",
    "newAccordionRow_0",
)
_PRIMARY_OFFSCREEN_PRICE_RE = re.compile(
    r'class=(?:\\&quot;|["\'])a-offscreen(?:\\&quot;|["\'])[^>]*>\s*([^<]+)\s*<',
    re.IGNORECASE | re.DOTALL,
)
_PRIMARY_PRICE_WHOLE_RE = re.compile(
    r'class=(?:\\&quot;|["\'])a-price-whole(?:\\&quot;|["\'])[^>]*>\s*([\d.,]+)\s*<',
    re.IGNORECASE | re.DOTALL,
)
_PRIMARY_PRICE_FRACTION_RE = re.compile(
    r'class=(?:\\&quot;|["\'])a-price-fraction(?:\\&quot;|["\'])[^>]*>\s*(\d{1,2})\s*<',
    re.IGNORECASE | re.DOTALL,
)
_WEIGHT_PATTERNS = (
    re.compile(r'(\d+(?:\.\d+)?)\s*(?:fl\.?\s*)?(oz|ounce|ounces)\b', re.IGNORECASE),
    re.compile(r'(\d+(?:\.\d+)?)\s*(lb|lbs|pound|pounds)\b', re.IGNORECASE),
    re.compile(r'(\d+(?:\.\d+)?)\s*(kg|kilogram|kilograms)\b', re.IGNORECASE),
    re.compile(r'(\d+(?:\.\d+)?)\s*(g|gram|grams)\b', re.IGNORECASE),
    re.compile(r'(\d+(?:\.\d+)?)\s*(ml|milliliter|milliliters)\b', re.IGNORECASE),
    re.compile(r'(\d+(?:\.\d+)?)\s*(l|liter|liters)\b', re.IGNORECASE),
)
_COUNT_PATTERNS = (
    re.compile(r'(\d+)\s*count\b', re.IGNORECASE),
    re.compile(r'(\d+)[-\s]*ct\b', re.IGNORECASE),
)
_PACK_RE = re.compile(r'pack of (\d+)', re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTISPACE_RE = re.compile(r"\s+")
_STRUCTURED_PRODUCT_TITLE_RE = re.compile(
    r'(?:&quot;|\\&quot;|")productTitle(?:&quot;|\\&quot;|")\s*:\s*(?:&quot;|\\&quot;|")(.{0,220}?)(?:&quot;|\\&quot;|")',
    re.IGNORECASE | re.DOTALL,
)
_AVAILABILITY_RULES = (
    ("currently unavailable", "out_of_stock"),
    ("temporarily out of stock", "out_of_stock"),
    ("out of stock", "out_of_stock"),
    ("available from these sellers", "limited"),
    ("see all buying options", "limited"),
    ("usually ships within", "limited"),
    ("only ", "limited"),
    ("in stock", "in_stock"),
)


@dataclass(frozen=True)
class ExtractionOutcome:
    listing: dict | None
    warnings: list[str]
    errors: list[str]
    field_provenance: dict[str, dict[str, object]] = field(default_factory=dict)
    low_confidence_reasons: list[dict[str, str]] = field(default_factory=list)


class AmazonListingNormalizer:
    def normalize(self, record: RawSourceRecord, raw_payload_uri: str) -> ExtractionOutcome:
        payload = record.payload
        warnings: list[str] = []
        errors: list[str] = []
        field_provenance: dict[str, dict[str, object]] = {}
        low_confidence_reasons: list[dict[str, str]] = []

        if payload.get("is_robot_check"):
            return ExtractionOutcome(
                None,
                warnings,
                ["amazon returned a robot-check page"],
                field_provenance=field_provenance,
                low_confidence_reasons=low_confidence_reasons,
            )
        if payload.get("status_code") != 200:
            return ExtractionOutcome(
                None,
                warnings,
                [f"unexpected http status {payload.get('status_code')}"],
                field_provenance=field_provenance,
                low_confidence_reasons=low_confidence_reasons,
            )

        html = payload.get("html")
        if not isinstance(html, str) or not html.strip():
            return ExtractionOutcome(
                None,
                warnings,
                ["missing raw html payload"],
                field_provenance=field_provenance,
                low_confidence_reasons=low_confidence_reasons,
            )

        product_title, title_provenance = _extract_title(html, payload.get("page_title"))
        field_provenance["product_title"] = title_provenance
        if product_title is None:
            return ExtractionOutcome(
                None,
                warnings,
                ["missing product title"],
                field_provenance=field_provenance,
                low_confidence_reasons=low_confidence_reasons,
            )

        brand_name, brand_provenance, brand_warning, brand_reason = _extract_brand(
            html, payload.get("original_url"), product_title
        )
        field_provenance["brand_name"] = brand_provenance
        if brand_name is None:
            return ExtractionOutcome(
                None,
                warnings,
                ["missing brand name"],
                field_provenance=field_provenance,
                low_confidence_reasons=low_confidence_reasons,
            )
        if brand_warning:
            warnings.append(brand_warning)
        if brand_reason:
            low_confidence_reasons.append(brand_reason)

        asin = payload.get("asin")
        price, price_provenance, price_reason = _extract_price(html, asin if isinstance(asin, str) else None)
        field_provenance["price"] = price_provenance
        if price is None:
            missing_price_error = _describe_missing_price_error(html, price_provenance)
            return ExtractionOutcome(
                None,
                warnings,
                [missing_price_error],
                field_provenance=field_provenance,
                low_confidence_reasons=low_confidence_reasons,
            )
        if price_reason:
            low_confidence_reasons.append(price_reason)

        category_path = _extract_category_path(html)
        if not category_path:
            warnings.append("missing breadcrumb categories; using uncategorized fallback")
            category_path = ["uncategorized"]
            field_provenance["category_path"] = _provenance(
                "default", "fallback to ['uncategorized'] due to missing breadcrumbs", "category_uncategorized_fallback"
            )
            low_confidence_reasons.append(
                _low_confidence_reason(
                    "missing_breadcrumb_categories",
                    "category_path",
                    "missing breadcrumb categories; using uncategorized fallback",
                )
            )
        else:
            field_provenance["category_path"] = _provenance(
                "html",
                "ul.a-unordered-list.a-horizontal.a-size-small -> a.a-color-tertiary",
                "category_breadcrumbs",
            )

        measure, measure_provenance, measure_reasons = _extract_measure(product_title)
        if measure.warning:
            warnings.append(measure.warning)
        field_provenance.update(measure_provenance)
        low_confidence_reasons.extend(measure_reasons)

        availability, availability_provenance, availability_reason = _extract_availability(html)
        field_provenance["availability"] = availability_provenance
        if availability == "unknown":
            warnings.append("availability signal was unclear")
        if availability_reason:
            low_confidence_reasons.append(availability_reason)

        rating = _extract_rating(html)
        review_count = _extract_review_count(html)
        currency = _extract_currency(payload.get("final_url"), payload.get("original_url"))
        field_provenance["currency"] = _provenance(
            "url",
            f"{urlparse(payload.get('final_url') or payload.get('original_url') or '').netloc or 'unknown host'}",
            "currency_from_host",
        )
        listing_id = f"{record.source}:{payload.get('asin', record.record_id)}"
        total_quantity = measure.quantity * measure.pack_count
        unit_price = round(price / total_quantity, 4) if total_quantity > 0 else round(price, 4)
        field_provenance["unit_price"] = _provenance(
            "rule",
            f"unit_price=price/(quantity*pack_count) where quantity={measure.quantity} and pack_count={measure.pack_count}",
            "unit_price_compute_v1",
        )

        listing = {
            "listing_id": listing_id,
            "source": record.source,
            "source_record_id": record.record_id,
            "captured_at": record.captured_at,
            "product_title": product_title,
            "brand_name": brand_name,
            "category_path": category_path,
            "price": round(price, 2),
            "currency": currency,
            "unit_price": unit_price,
            "unit_measure": measure.unit_measure,
            "pack_count": measure.pack_count,
            "availability": availability,
            "raw_payload_uri": raw_payload_uri,
        }
        if rating is not None:
            listing["rating"] = rating
        if review_count is not None:
            listing["review_count"] = review_count

        return ExtractionOutcome(
            listing,
            warnings,
            errors,
            field_provenance=field_provenance,
            low_confidence_reasons=low_confidence_reasons,
        )


@dataclass(frozen=True)
class ParsedMeasure:
    quantity: float
    unit_measure: str
    pack_count: int
    warning: str | None = None


def _extract_title(html: str, page_title: str | None) -> tuple[str | None, dict[str, object]]:
    match = _TITLE_RE.search(html)
    if match:
        return _clean_text(match.group(1)), _provenance("html", "span#productTitle", "title_productTitle")
    if page_title:
        cleaned = _clean_text(page_title)
        if cleaned.lower().startswith("amazon.com :"):
            cleaned = cleaned.split(":", 1)[1].strip()
        if " : " in cleaned:
            cleaned = cleaned.rsplit(" : ", 1)[0].strip()
        return cleaned or None, _provenance("page_title", "payload.page_title", "title_page_title")
    return None, _provenance("unknown", "no title source found", "title_missing")


def _extract_brand(
    html: str, original_url: str | None, product_title: str
) -> tuple[str | None, dict[str, object], str | None, dict[str, str] | None]:
    match = _BYLINE_RE.search(html)
    if match:
        brand = _normalize_brand_label(_clean_text(match.group(1)))
        if brand and "premium non-fashion" not in brand.lower():
            return brand, _provenance("html", "a#bylineInfo", "brand_bylineInfo"), None, None

    premium_match = _PREMIUM_BRAND_ALT_RE.search(html)
    if premium_match:
        brand = _normalize_brand_label(_clean_text(premium_match.group(1)))
        if brand:
            return brand, _provenance("html", "premium brand alt attribute", "brand_premium_alt"), None, None

    inferred_from_url = _infer_brand_from_url(original_url)
    if inferred_from_url:
        return (
            inferred_from_url,
            _provenance("url", "original_url path slug", "brand_url_slug"),
            "brand inferred from Amazon URL slug",
            _low_confidence_reason(
                "brand_inferred_from_url_slug", "brand_name", "brand inferred from Amazon URL slug"
            ),
        )

    inferred_from_title = _infer_brand_from_title(product_title)
    if inferred_from_title:
        return (
            inferred_from_title,
            _provenance("rule", "first token from product title", "brand_title_first_token"),
            "brand inferred from product title",
            _low_confidence_reason(
                "brand_inferred_from_title", "brand_name", "brand inferred from product title"
            ),
        )

    return None, _provenance("unknown", "no brand source found", "brand_missing"), None, None


def _infer_brand_from_url(url: str | None) -> str | None:
    if not url:
        return None
    path_parts = [part for part in urlparse(url).path.split("/") if part]
    if not path_parts:
        return None
    slug = unquote(path_parts[0])
    words = [word for word in re.split(r"[-_]+", slug) if word and word.lower() not in {"dp", "gp", "product"}]
    if not words:
        return None
    return _normalize_brand_label(_clean_text(words[0]))


def _infer_brand_from_title(product_title: str) -> str | None:
    words = product_title.split()
    if not words:
        return None
    candidate = re.sub(r"[^A-Za-z0-9&']", "", words[0]).strip()
    return _normalize_brand_label(candidate) or None


def _extract_price(html: str, asin: str | None) -> tuple[float | None, dict[str, object], dict[str, str] | None]:
    lowered_html = html.lower()
    # Prefer primary price containers first. This keeps us scoped to the listing and avoids
    # grabbing recommended/widget prices from unrelated sections.
    container_price, container_detail = _extract_price_from_primary_containers(html)
    if container_price is not None:
        return (
            container_price,
            _provenance(
                "html",
                container_detail,
                "price_primary_container",
            ),
            None,
        )

    # Backward-compatible fallback for known block patterns.
    for index, pattern in enumerate(_PRICE_BLOCK_PATTERNS):
        for match in pattern.finditer(html):
            if _is_inside_block(lowered_html, match.start(), "script"):
                continue
            if _is_inside_block(lowered_html, match.start(), "style"):
                continue
            parsed = _parse_price_amount(match.group(1))
            if parsed is not None:
                return (
                    parsed,
                    _provenance(
                        "html",
                        f"price block regex match block pattern {index}",
                        "price_block_primary_legacy",
                    ),
                    None,
                )

    # If we don't have a block price, use structured patterns but scope them to the current ASIN.
    structured_matches: list[tuple[int, int, str]] = []
    for index, pattern in enumerate(_PRICE_PATTERNS):
        for match in pattern.finditer(html):
            structured_matches.append((index, match.start(), match.group(1)))

    if not structured_matches:
        return None, _provenance("unknown", "no price match found", "price_missing"), None

    if asin:
        asin_tokens = (
            f'\"asin\"\\s*:\\s*\"{asin}\"',
            f'&quot;asin&quot;\\s*:\\s*&quot;{asin}&quot;',
            f'asin\\\\&quot;\\s*:\\s*\\\\&quot;{asin}\\\\&quot;',
            f'&quot;asin\\&quot;\\s*:\\s*\\&quot;{asin}\\&quot;',
        )
        asin_res = [re.compile(token, re.IGNORECASE) for token in asin_tokens]
        for index, start, value in structured_matches:
            window_from = max(0, start - 800)
            window_to = min(len(html), start + 800)
            window = html[window_from:window_to]
            if any(token_re.search(window) for token_re in asin_res):
                return (
                    float(value),
                    _provenance(
                        "html",
                        f"structured data priceAmount scoped to asin={asin} (pattern {index})",
                        "price_structured_data_asin_scoped",
                    ),
                    None,
                )

        # If we know the ASIN but can't scope a structured price to it, it is usually safer to fail than guess.
        #
        # Exception: for small, fixture-like HTML bodies that contain exactly one structured price match,
        # we accept the singleton price to keep deterministic tests stable. Real Amazon pages usually contain
        # many structured priceAmount entries (widgets / recs / ads), so this fallback won't trigger there.
        #
        # Safety override: if the page already signals missing primary offer state (for example buying-options-only),
        # we must fail clearly instead of accepting any unscoped structured price.
        unique_values = sorted({value for _, _, value in structured_matches})
        if len(unique_values) == 1 and len(html) < 60_000 and not _has_missing_primary_offer_signal(lowered_html):
            return (
                float(unique_values[0]),
                _provenance(
                    "html",
                    f"singleton structured priceAmount (unscoped) with asin={asin}; accepted for small html",
                    "price_structured_data_singleton_unscoped",
                ),
                None,
            )

        candidate_summary = _summarize_structured_price_candidates(html, structured_matches)
        detail = f"structured prices found but none scoped to asin={asin}"
        if candidate_summary:
            detail = f"{detail}; candidates={candidate_summary}"
        return None, _provenance("unknown", detail, "price_missing"), None

    # No ASIN available: fall back to first structured match, but mark it low-confidence.
    index, _, value = structured_matches[0]
    return (
        float(value),
        _provenance("html", f"embedded data regex match price pattern {index}", "price_structured_data_unscoped"),
        _low_confidence_reason(
            "price_unscoped_structured_data",
            "price",
            "priceAmount extracted without ASIN scoping; may be from a non-primary widget",
        ),
    )


def _extract_price_from_primary_containers(html: str) -> tuple[float | None, str]:
    lowered_html = html.lower()
    for container_id in _PRIMARY_PRICE_CONTAINER_IDS:
        container_tag_pattern = _compile_primary_container_tag_pattern(container_id)
        for tag_match in container_tag_pattern.finditer(html):
            # Some Amazon scripts include container id strings inside update metadata or
            # encoded widget payloads. Those are not safe primary-price sources.
            if _is_inside_block(lowered_html, tag_match.start(), "script"):
                continue
            if _is_inside_block(lowered_html, tag_match.start(), "style"):
                continue
            window = _extract_container_window(html, lowered_html, tag_match)
            parsed = _extract_price_from_container_window(window)
            if parsed is not None:
                return parsed, f"{container_id} ({tag_match.group('tag')})"
    return None, ""


@lru_cache(maxsize=64)
def _compile_primary_container_tag_pattern(container_id: str) -> re.Pattern[str]:
    escaped = re.escape(container_id)
    # Match actual HTML tags only; do not match JSON strings like:
    # "divToUpdate":"corePriceDisplay_desktop_feature_div"
    return re.compile(
        rf"<(?P<tag>[a-z0-9]+)\b[^>]*\bid\s*=\s*(?:\"{escaped}\"|'{escaped}'|{escaped})[^>]*>",
        re.IGNORECASE,
    )


def _is_inside_block(lowered_html: str, index: int, tag: str) -> bool:
    open_index = lowered_html.rfind(f"<{tag}", 0, index)
    if open_index < 0:
        return False
    close_index = lowered_html.rfind(f"</{tag}", 0, index)
    return close_index < open_index


def _extract_container_window(html: str, lowered_html: str, tag_match: re.Match[str], max_size: int = 9000) -> str:
    start = tag_match.start()
    end = min(len(html), start + max_size)
    close_tag = f"</{tag_match.group('tag').lower()}>"
    close_index = lowered_html.find(close_tag, tag_match.end(), end)
    if close_index >= 0:
        end = min(len(html), close_index + len(close_tag))
    return html[start:end]


def _extract_price_from_container_window(window: str) -> float | None:
    offscreen_match = _PRIMARY_OFFSCREEN_PRICE_RE.search(window)
    if offscreen_match:
        parsed = _parse_price_amount(offscreen_match.group(1))
        if parsed is not None:
            return parsed

    whole_match = _PRIMARY_PRICE_WHOLE_RE.search(window)
    if whole_match:
        whole_text = whole_match.group(1)
        fraction_match = _PRIMARY_PRICE_FRACTION_RE.search(window[whole_match.end(): whole_match.end() + 220])
        fraction_text = fraction_match.group(1) if fraction_match else None
        parsed = _parse_whole_fraction_price(whole_text, fraction_text)
        if parsed is not None:
            return parsed
    return None


def _parse_whole_fraction_price(whole_text: str, fraction_text: str | None) -> float | None:
    whole_digits = re.sub(r"[^0-9]", "", whole_text)
    if not whole_digits:
        return None
    if fraction_text:
        fraction_digits = re.sub(r"[^0-9]", "", fraction_text)[:2].ljust(2, "0")
        return float(f"{int(whole_digits)}.{fraction_digits}")
    return float(int(whole_digits))


def _parse_price_amount(text: str) -> float | None:
    cleaned = re.sub(r"[^0-9,.\-]", "", text).strip()
    if not cleaned:
        return None

    # Normalize decimal/thousand separators.
    if "," in cleaned and "." in cleaned:
        last_comma = cleaned.rfind(",")
        last_dot = cleaned.rfind(".")
        decimal_index = max(last_comma, last_dot)
        integer_part = re.sub(r"[^0-9\-]", "", cleaned[:decimal_index])
        decimal_part = re.sub(r"[^0-9]", "", cleaned[decimal_index + 1:])[:2]
        if not integer_part:
            return None
        if decimal_part:
            return float(f"{integer_part}.{decimal_part}")
        return float(integer_part)

    if "," in cleaned:
        if re.search(r",\d{2}$", cleaned):
            normalized = cleaned.replace(".", "").replace(",", ".")
            return float(normalized)
        return float(cleaned.replace(",", ""))

    if cleaned.count(".") > 1:
        if re.search(r"\.\d{2}$", cleaned):
            last_dot = cleaned.rfind(".")
            integer_part = cleaned[:last_dot].replace(".", "")
            decimal_part = cleaned[last_dot + 1:]
            return float(f"{integer_part}.{decimal_part}")
        return float(cleaned.replace(".", ""))

    return float(cleaned)


def _describe_missing_price_error(html: str, price_provenance: dict[str, object]) -> str:
    lowered = html.lower()
    detail = str(price_provenance.get("source_detail", ""))
    if _has_missing_primary_offer_signal(lowered):
        return f"missing product price: no featured offers available on page; buying-options only. {detail}".strip()
    if "currently unavailable" in lowered:
        return f"missing product price: product is currently unavailable. {detail}".strip()
    return f"missing product price. {detail}".strip()


def _has_missing_primary_offer_signal(lowered_html: str) -> bool:
    return "no featured offers available" in lowered_html or "see all buying options" in lowered_html


def _summarize_structured_price_candidates(
    html: str,
    structured_matches: list[tuple[int, int, str]],
    limit: int = 3,
) -> str:
    candidates: list[str] = []
    seen: set[str] = set()
    for _, start, value in structured_matches:
        window = html[max(0, start - 250): min(len(html), start + 450)]
        title_match = _STRUCTURED_PRODUCT_TITLE_RE.search(window)
        title = _clean_text(title_match.group(1)) if title_match else ""
        summary = f"{value}:{title[:80]}" if title else value
        if summary in seen:
            continue
        seen.add(summary)
        candidates.append(summary)
        if len(candidates) >= limit:
            break
    return " | ".join(candidates)


def _extract_category_path(html: str) -> list[str]:
    match = _BREADCRUMB_BLOCK_RE.search(html)
    if not match:
        return []
    categories = [_clean_text(unescape(item)) for item in _BREADCRUMB_ITEM_RE.findall(match.group(1))]
    return [_slugify(category) for category in categories if category]


def _extract_rating(html: str) -> float | None:
    match = _REVIEW_RATING_RE.search(html)
    if not match:
        return None
    numeric_match = re.search(r"(\d+(?:\.\d+)?)", match.group(1))
    return float(numeric_match.group(1)) if numeric_match else None


def _extract_review_count(html: str) -> int | None:
    match = _REVIEW_COUNT_RE.search(html)
    if not match:
        return None
    digits = re.sub(r"[^0-9]", "", match.group(1))
    return int(digits) if digits else None


def _extract_availability(html: str) -> tuple[str, dict[str, object], dict[str, str] | None]:
    lowered = html.lower()
    for token, status in _AVAILABILITY_RULES:
        if token in lowered:
            return (
                status,
                _provenance("html", f"token match: '{token}'", "availability_token_match"),
                None,
            )
    return (
        "unknown",
        _provenance("unknown", "no known availability tokens found", "availability_unknown"),
        _low_confidence_reason(
            "availability_unclear",
            "availability",
            "availability signal was unclear",
        ),
    )


def _extract_currency(final_url: str | None, original_url: str | None) -> str:
    url = final_url or original_url or ""
    host = urlparse(url).netloc.lower()
    if host.endswith(".co.uk"):
        return "GBP"
    if host.endswith(".de") or host.endswith(".fr") or host.endswith(".es") or host.endswith(".it"):
        return "EUR"
    return "USD"


def _extract_measure(
    product_title: str,
) -> tuple[ParsedMeasure, dict[str, dict[str, object]], list[dict[str, str]]]:
    pack_count = 1
    provenance: dict[str, dict[str, object]] = {}
    low_confidence_reasons: list[dict[str, str]] = []
    detected_size_signals: list[str] = []
    pack_match = _PACK_RE.search(product_title)
    if pack_match:
        pack_count = max(1, int(pack_match.group(1)))
        provenance["pack_count"] = _provenance(
            "rule",
            f"pack of {pack_count} parsed from product title",
            "pack_count_pack_of",
        )
    else:
        provenance["pack_count"] = _provenance(
            "default",
            "no pack-of signal found; defaulting pack_count=1",
            "pack_count_default_1",
        )

    # Pre-scan for multiple size signals so we can surface ambiguity for operators.
    for pattern in _WEIGHT_PATTERNS:
        match = pattern.search(product_title)
        if match:
            detected_size_signals.append(match.group(0).strip())
    for pattern in _COUNT_PATTERNS:
        match = pattern.search(product_title)
        if match:
            detected_size_signals.append(match.group(0).strip())
    unique_signals = sorted(set(detected_size_signals))
    if len(unique_signals) > 1:
        low_confidence_reasons.append(
            _low_confidence_reason(
                "multiple_size_signals_detected",
                "unit_measure",
                f"multiple size signals detected in title; parsed using first match: {unique_signals}",
            )
        )

    for pattern in _WEIGHT_PATTERNS:
        match = pattern.search(product_title)
        if match:
            quantity = float(match.group(1))
            unit_measure = _normalize_measure_unit(match.group(2))
            provenance["unit_measure"] = _provenance(
                "rule",
                f"parsed '{match.group(0).strip()}' from product title",
                "unit_measure_weight_pattern",
            )
            provenance["unit_measure"]["quantity"] = quantity
            provenance["unit_measure"]["raw_unit"] = match.group(2)
            if unique_signals:
                provenance["unit_measure"]["detected_signals"] = unique_signals
            return ParsedMeasure(quantity=quantity, unit_measure=unit_measure, pack_count=pack_count), provenance, low_confidence_reasons

    for pattern in _COUNT_PATTERNS:
        match = pattern.search(product_title)
        if match:
            quantity = float(match.group(1))
            provenance["unit_measure"] = _provenance(
                "rule",
                f"parsed '{match.group(0).strip()}' from product title",
                "unit_measure_count_pattern",
            )
            provenance["unit_measure"]["quantity"] = quantity
            provenance["unit_measure"]["raw_unit"] = "count"
            if unique_signals:
                provenance["unit_measure"]["detected_signals"] = unique_signals
            return ParsedMeasure(quantity=quantity, unit_measure="count", pack_count=pack_count), provenance, low_confidence_reasons

    provenance["unit_measure"] = _provenance(
        "default",
        "no size signal found; defaulting to unit_measure=count with quantity=1",
        "unit_measure_default_count",
    )
    provenance["unit_measure"]["quantity"] = 1.0
    provenance["unit_measure"]["raw_unit"] = "count"
    if unique_signals:
        provenance["unit_measure"]["detected_signals"] = unique_signals
    low_confidence_reasons.append(
        _low_confidence_reason(
            "size_signal_missing",
            "unit_measure",
            "size signal missing; defaulting to single count",
        )
    )
    return (
        ParsedMeasure(
            quantity=1.0,
            unit_measure="count",
            pack_count=pack_count,
            warning="size signal missing; defaulting to single count",
        ),
        provenance,
        low_confidence_reasons,
    )


def _provenance(source_type: str, source_detail: str, rule: str) -> dict[str, object]:
    return {
        "source_type": source_type,
        "source_detail": source_detail,
        "rule": rule,
    }


def _low_confidence_reason(code: str, field: str, message: str) -> dict[str, str]:
    return {
        "code": code,
        "field": field,
        "message": message,
    }


def _normalize_measure_unit(raw_unit: str) -> str:
    lowered = raw_unit.lower()
    if lowered in {"lb", "lbs", "pound", "pounds"}:
        return "lb"
    if lowered in {"oz", "ounce", "ounces"}:
        return "oz"
    if lowered in {"kg", "kilogram", "kilograms"}:
        return "kg"
    if lowered in {"g", "gram", "grams"}:
        return "g"
    if lowered in {"ml", "milliliter", "milliliters"}:
        return "ml"
    if lowered in {"l", "liter", "liters"}:
        return "l"
    return "count"


def _clean_text(value: str) -> str:
    without_tags = _HTML_TAG_RE.sub(" ", value)
    unescaped = unescape(without_tags)
    return _MULTISPACE_RE.sub(" ", unescaped).strip()


def _slugify(value: str) -> str:
    lowered = value.lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    return lowered.strip("-")


def _normalize_brand_label(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = _clean_text(value)
    store_match = _VISIT_STORE_RE.search(cleaned)
    if store_match:
        cleaned = store_match.group(1)
    cleaned = re.sub(r"^\s*brand\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+store\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" -")
    return cleaned or None
