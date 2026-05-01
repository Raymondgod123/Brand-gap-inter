from __future__ import annotations

from dataclasses import dataclass
import math
import re

SELECTOR_VERSION = "query_fit_v1"
TOKEN_RE = re.compile(r"[a-z0-9]+")

STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "for",
        "of",
        "or",
        "the",
        "to",
        "with",
    }
)

QUERY_MODIFIERS = frozenset(
    {
        "free",
        "low",
        "no",
        "sugar",
        "sugarfree",
        "sugar-free",
        "zero",
        "calorie",
        "calories",
        "healthy",
    }
)

QUERY_FAMILY_RULES: dict[str, dict[str, frozenset[str]]] = {
    "hydration": {
        "triggers": frozenset({"hydration", "hydrate", "electrolyte", "electrolytes"}),
        "include": frozenset(
            {
                "hydration",
                "hydrate",
                "electrolyte",
                "electrolytes",
                "powder",
                "powders",
                "stick",
                "sticks",
                "packet",
                "packets",
                "tablet",
                "tablets",
                "sodium",
                "potassium",
                "magnesium",
                "zero",
                "sugar",
                "sports",
                "workout",
                "rapid",
                "recovery",
            }
        ),
        "exclude": frozenset(
            {
                "protein",
                "bar",
                "bars",
                "candy",
                "candies",
                "coffee",
                "tea",
                "energy",
                "caffeine",
                "preworkout",
            }
        ),
    },
    "protein_powder": {
        "triggers": frozenset({"protein", "powder", "powders", "shake", "shakes"}),
        "include": frozenset(
            {
                "protein",
                "proteins",
                "powder",
                "powders",
                "shake",
                "shakes",
                "whey",
                "plant",
                "vegan",
                "pea",
                "casein",
                "isolate",
                "collagen",
                "meal",
                "replacement",
                "muscle",
                "recovery",
                "vanilla",
                "chocolate",
                "unflavored",
                "bulk",
                "servings",
            }
        ),
        "exclude": frozenset(
            {
                "bar",
                "bars",
                "candy",
                "candies",
                "gummy",
                "gummies",
                "drink",
                "drinks",
                "energy",
                "electrolyte",
                "electrolytes",
                "hydration",
                "tablet",
                "tablets",
            }
        ),
    },
    "energy_drink": {
        "triggers": frozenset({"energy", "drink", "drinks"}),
        "include": frozenset(
            {
                "energy",
                "drink",
                "drinks",
                "caffeine",
                "zero",
                "sugar",
                "sparkling",
                "can",
                "cans",
                "variety",
                "focus",
                "performance",
                "preworkout",
                "workout",
                "clean",
                "natural",
            }
        ),
        "exclude": frozenset(
            {
                "candy",
                "candies",
                "sweetener",
                "sweeteners",
                "protein",
                "powder",
                "powders",
                "bar",
                "bars",
                "coffee",
                "tea",
                "hydration",
                "electrolyte",
                "electrolytes",
                "tablet",
                "tablets",
            }
        ),
    },
    "protein_bar": {
        "triggers": frozenset({"protein", "bar", "bars"}),
        "include": frozenset(
            {
                "bar",
                "bars",
                "protein",
                "proteins",
                "vegan",
                "plant",
                "based",
                "plantbased",
                "snack",
                "snacks",
                "fiber",
                "gluten",
                "dairy",
                "soy",
                "keto",
                "low",
                "sugar",
                "meal",
                "replacement",
            }
        ),
        "exclude": frozenset(
            {
                "powder",
                "powders",
                "shake",
                "shakes",
                "drink",
                "drinks",
                "beverage",
                "cookies",
                "cereal",
                "granola",
            }
        ),
    },
    "candy": {
        "triggers": frozenset({"candy", "candies"}),
        "include": frozenset(
            {
                "candy",
                "candies",
                "gummy",
                "gummies",
                "licorice",
                "lollipop",
                "lollipops",
                "hard",
                "chewy",
                "caramel",
                "caramels",
                "chocolate",
                "mint",
                "mints",
                "drops",
                "pops",
                "patties",
                "twists",
            }
        ),
        "exclude": frozenset(
            {
                "drink",
                "drinks",
                "energy",
                "beverage",
                "tea",
                "coffee",
                "syrup",
                "powder",
                "powders",
                "extract",
                "sweetener",
                "sweeteners",
                "snack",
                "snacks",
            }
        ),
    },
    "sweetener": {
        "triggers": frozenset({"sweetener", "sweeteners"}),
        "include": frozenset(
            {
                "sweetener",
                "sweeteners",
                "monk",
                "fruit",
                "stevia",
                "erythritol",
                "allulose",
                "sugar",
                "substitute",
                "packet",
                "packets",
                "liquid",
                "powder",
            }
        ),
        "exclude": frozenset(
            {
                "candy",
                "candies",
                "gummy",
                "gummies",
                "lollipop",
                "lollipops",
                "licorice",
                "chocolate",
                "mint",
                "caramel",
                "caramels",
            }
        ),
    },
    "sugar": {
        "triggers": frozenset({"sugar"}),
        "include": frozenset(
            {
                "sugar",
                "granulated",
                "brown",
                "cane",
                "powdered",
                "confectioners",
                "packet",
                "packets",
                "sweetener",
            }
        ),
        "exclude": frozenset(
            {
                "drink",
                "drinks",
                "energy",
                "tea",
                "coffee",
                "snack",
                "snacks",
                "candy",
                "candies",
                "gummy",
                "gummies",
            }
        ),
    },
}


@dataclass(frozen=True)
class QuerySelectionContext:
    query: str
    query_terms: frozenset[str]
    focus_terms: frozenset[str]
    family_name: str | None
    family_include_terms: frozenset[str]
    family_exclude_terms: frozenset[str]


@dataclass(frozen=True)
class CandidateSelectionDecision:
    record: dict[str, object]
    fit_score: float
    is_preferred: bool
    focus_hits: tuple[str, ...]
    include_hits: tuple[str, ...]
    exclude_hits: tuple[str, ...]
    query_overlap: tuple[str, ...]
    reasons: tuple[str, ...]

    def to_candidate_dict(self, *, context: QuerySelectionContext, selection_bucket: str) -> dict[str, object]:
        return {
            "discovery_id": self.record["discovery_id"],
            "rank": self.record["rank"],
            "title": self.record.get("title"),
            "asin": self.record.get("asin"),
            "product_url": self.record.get("product_url"),
            "price": self.record.get("price"),
            "currency": self.record.get("currency"),
            "rating": self.record.get("rating"),
            "review_count": self.record.get("review_count"),
            "sponsored": self.record.get("sponsored"),
            "selection_trace": {
                "selector_version": SELECTOR_VERSION,
                "query_family": context.family_name,
                "fit_score": round(self.fit_score, 3),
                "selection_bucket": selection_bucket,
                "focus_terms": sorted(context.focus_terms),
                "focus_hits": list(self.focus_hits),
                "include_hits": list(self.include_hits),
                "exclude_hits": list(self.exclude_hits),
                "query_overlap": list(self.query_overlap),
                "reasons": list(self.reasons),
            },
        }

    def to_report_dict(self, *, context: QuerySelectionContext, selection_bucket: str, selected: bool) -> dict[str, object]:
        return {
            "discovery_id": self.record["discovery_id"],
            "rank": self.record["rank"],
            "title": self.record.get("title"),
            "asin": self.record.get("asin"),
            "selected": selected,
            "selection_bucket": selection_bucket,
            "fit_score": round(self.fit_score, 3),
            "query_family": context.family_name,
            "focus_hits": list(self.focus_hits),
            "include_hits": list(self.include_hits),
            "exclude_hits": list(self.exclude_hits),
            "query_overlap": list(self.query_overlap),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class CandidateSelectionResult:
    context: QuerySelectionContext
    selected_candidates: list[dict[str, object]]
    selected_decisions: list[CandidateSelectionDecision]
    all_decisions: list[CandidateSelectionDecision]
    filtered_out_count: int
    backfill_count: int

    def to_report_dict(self) -> dict[str, object]:
        selected_ids = {candidate["discovery_id"] for candidate in self.selected_candidates}
        selection_buckets = {
            candidate["discovery_id"]: candidate["selection_trace"]["selection_bucket"]
            for candidate in self.selected_candidates
        }
        return {
            "selector_version": SELECTOR_VERSION,
            "query": self.context.query,
            "query_terms": sorted(self.context.query_terms),
            "focus_terms": sorted(self.context.focus_terms),
            "query_family": self.context.family_name,
            "preferred_pool_count": sum(1 for decision in self.all_decisions if decision.is_preferred),
            "filtered_out_count": self.filtered_out_count,
            "selected_count": len(self.selected_candidates),
            "backfill_count": self.backfill_count,
            "records": [
                decision.to_report_dict(
                    context=self.context,
                    selection_bucket=selection_buckets.get(decision.record["discovery_id"], "not_selected"),
                    selected=decision.record["discovery_id"] in selected_ids,
                )
                for decision in self.all_decisions
            ],
        }


def select_candidates(
    records: list[dict[str, object]],
    *,
    query: str,
    max_products: int,
) -> CandidateSelectionResult:
    context = build_query_selection_context(query)
    decisions = [
        _evaluate_candidate(record, context=context)
        for record in records
        if record.get("status") == "valid"
    ]
    ranked_decisions = sorted(decisions, key=_decision_sort_key)
    preferred_decisions = [decision for decision in ranked_decisions if decision.is_preferred]
    filtered_decisions = [decision for decision in ranked_decisions if not decision.is_preferred]

    selected_decisions = preferred_decisions[:max_products]
    backfill_count = 0
    if len(selected_decisions) < max_products:
        needed = max_products - len(selected_decisions)
        selected_decisions.extend(filtered_decisions[:needed])
        backfill_count = min(needed, len(filtered_decisions))

    selected_candidates = [
        decision.to_candidate_dict(
            context=context,
            selection_bucket="preferred" if decision.is_preferred else "backfill_filtered",
        )
        for decision in selected_decisions
    ]

    return CandidateSelectionResult(
        context=context,
        selected_candidates=selected_candidates,
        selected_decisions=selected_decisions,
        all_decisions=ranked_decisions,
        filtered_out_count=len(filtered_decisions),
        backfill_count=backfill_count,
    )


def build_query_selection_context(query: str) -> QuerySelectionContext:
    query_terms = frozenset(_tokenize(query))
    focus_terms = frozenset(term for term in query_terms if term not in QUERY_MODIFIERS and term not in STOPWORDS)

    family_name = None
    family_include_terms = frozenset()
    family_exclude_terms = frozenset()
    best_score: tuple[int, int, int] = (0, 0, 0)
    for index, (name, rule) in enumerate(QUERY_FAMILY_RULES.items()):
        trigger_hits = query_terms.intersection(rule["triggers"])
        if not trigger_hits:
            continue
        include_hits = query_terms.intersection(rule["include"])
        score = (len(trigger_hits), len(include_hits), -index)
        if score > best_score:
            family_name = name
            family_include_terms = rule["include"]
            family_exclude_terms = rule["exclude"]
            best_score = score

    return QuerySelectionContext(
        query=query,
        query_terms=query_terms,
        focus_terms=focus_terms,
        family_name=family_name,
        family_include_terms=family_include_terms,
        family_exclude_terms=family_exclude_terms,
    )


def _evaluate_candidate(record: dict[str, object], *, context: QuerySelectionContext) -> CandidateSelectionDecision:
    title = str(record.get("title") or "")
    title_tokens = frozenset(_tokenize(title))
    focus_hits = tuple(sorted(title_tokens.intersection(context.focus_terms)))
    raw_include_hits = title_tokens.intersection(context.family_include_terms)
    include_hits = tuple(sorted(raw_include_hits.difference(focus_hits)))
    exclude_hits = tuple(sorted(title_tokens.intersection(context.family_exclude_terms)))
    query_overlap = tuple(sorted(title_tokens.intersection(context.query_terms)))

    rank = _coerce_positive_int(record.get("rank")) or 999999
    rating = _coerce_optional_float(record.get("rating"))
    review_count = _coerce_positive_int(record.get("review_count")) or 0
    sponsored = bool(record.get("sponsored"))

    rank_bonus = max(0.0, 1.0 - ((rank - 1) / 40.0))
    quality_bonus = 0.0
    if rating is not None:
        quality_bonus += min(max(rating, 0.0) / 5.0, 1.0) * 0.2
    if review_count > 0:
        quality_bonus += min(math.log10(review_count + 1) / 5.0, 1.0) * 0.2

    penalty = 0.0
    if exclude_hits:
        penalty = 0.75 * len(exclude_hits) if (focus_hits or include_hits) else 3.0 * len(exclude_hits)
    if sponsored:
        penalty += 0.1

    fit_score = (len(focus_hits) * 3.0) + (len(include_hits) * 0.5) + (len(query_overlap) * 0.5) + rank_bonus + quality_bonus - penalty

    reasons: list[str] = []
    if focus_hits:
        reasons.append(f"matches focus terms: {', '.join(focus_hits)}")
    if include_hits:
        reasons.append(f"matches category terms: {', '.join(include_hits)}")
    if exclude_hits:
        reasons.append(f"adjacent-category terms detected: {', '.join(exclude_hits)}")
    if sponsored:
        reasons.append("provider marked this result as sponsored")
    if not reasons:
        reasons.append("no strong query-fit signals detected; rank-only fallback")

    is_preferred = True
    if context.family_name is not None:
        missing_focus_terms = context.focus_terms.difference(focus_hits)
        if exclude_hits and (not include_hits or missing_focus_terms):
            is_preferred = False
            reasons.append("filtered from preferred pool because adjacent-category terms outweighed direct category evidence")
        elif context.focus_terms and not focus_hits and not include_hits:
            is_preferred = False
            reasons.append("filtered from preferred pool because no category or focus-term match was found")

    return CandidateSelectionDecision(
        record=record,
        fit_score=fit_score,
        is_preferred=is_preferred,
        focus_hits=focus_hits,
        include_hits=include_hits,
        exclude_hits=exclude_hits,
        query_overlap=query_overlap,
        reasons=tuple(reasons),
    )


def _decision_sort_key(decision: CandidateSelectionDecision) -> tuple[float, int, str]:
    return (
        0.0 if decision.is_preferred else 1.0,
        _coerce_positive_int(decision.record.get("rank")) or 999999,
        -decision.fit_score,
        str(decision.record.get("discovery_id") or ""),
    )


def _tokenize(value: str) -> list[str]:
    return TOKEN_RE.findall(value.lower())


def _coerce_optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_positive_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        integer = int(value)
        return integer if integer > 0 else None
    text = str(value).strip()
    if not text.isdigit():
        return None
    integer = int(text)
    return integer if integer > 0 else None
