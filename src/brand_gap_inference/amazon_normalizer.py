from __future__ import annotations

from dataclasses import dataclass, field
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

        price, price_provenance, price_reason = _extract_price(html)
        field_provenance["price"] = price_provenance
        if price is None:
            return ExtractionOutcome(
                None,
                warnings,
                ["missing product price"],
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


def _extract_price(html: str) -> tuple[float | None, dict[str, object], dict[str, str] | None]:
    for index, pattern in enumerate(_PRICE_PATTERNS):
        match = pattern.search(html)
        if match:
            return (
                float(match.group(1)),
                _provenance("html", f"embedded data regex match price pattern {index}", "price_structured_data"),
                None,
            )
    for index, pattern in enumerate(_PRICE_BLOCK_PATTERNS):
        match = pattern.search(html)
        if match:
            return (
                float(match.group(1).replace(",", "")),
                _provenance(
                    "html",
                    f"price block regex match block pattern {index}",
                    "price_block_fallback",
                ),
                _low_confidence_reason(
                    "price_secondary_pattern",
                    "price",
                    "price extracted from a secondary html block pattern",
                ),
            )
    return None, _provenance("unknown", "no price match found", "price_missing"), None


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
