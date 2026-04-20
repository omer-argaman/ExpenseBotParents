"""
parser.py — Expense message parser.

Splits a message into (keyword_phrase, amount, note) using the position of the
first number as the dividing point, then matches the keyword phrase against the
category map.

Possible parse statuses:
    matched         — exact keyword match, amount found. Ready to log.
    ask_amount      — exact keyword match, but no number in message. Ask user for amount.
    fuzzy_confirm   — no exact match. Best fuzzy guess returned. Ask user to confirm.
    reversed        — number came first; keyword found after it. Ready to log.
    no_match        — nothing matched, even fuzzy. Ask user to try again.
"""

import re
from dataclasses import dataclass, field
from typing import Optional

from fuzzywuzzy import process as fuzz_process
from parsing.category_map import CATEGORY_MAP

FUZZY_THRESHOLD = 65  # minimum score (0-100) to offer a fuzzy suggestion


# ---------------------------------------------------------------------------
# Result object
# ---------------------------------------------------------------------------

@dataclass
class ParseResult:
    status: str                        # see module docstring for possible values
    original_text: str
    category: Optional[str] = None     # canonical category key (matches sheet)
    amount: Optional[float] = None
    note: str = ""
    suggestion: Optional[str] = None   # fuzzy suggestion category, if any
    suggestion_score: int = 0          # confidence of fuzzy suggestion (0-100)
    error: str = ""                    # human-readable explanation of what went wrong


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_keyword_index() -> dict[str, str]:
    """Return a flat dict of lowercase_keyword -> category for fast lookup."""
    index = {}
    for category, keywords in CATEGORY_MAP.items():
        for kw in keywords:
            index[kw.lower()] = category
    return index


KEYWORD_INDEX = _build_keyword_index()


def _exact_match(phrase: str) -> Optional[str]:
    """Return category if phrase exactly matches a keyword (case-insensitive)."""
    return KEYWORD_INDEX.get(phrase.lower().strip())


def _fuzzy_match(phrase: str) -> Optional[tuple[str, str, int]]:
    """
    Return (category, matched_keyword, score) for the best fuzzy match,
    or None if nothing meets the threshold.
    """
    if not phrase.strip():
        return None

    all_keywords = list(KEYWORD_INDEX.keys())
    result = fuzz_process.extractOne(phrase.lower().strip(), all_keywords)

    if result is None:
        return None

    matched_keyword, score = result
    if score >= FUZZY_THRESHOLD:
        return KEYWORD_INDEX[matched_keyword], matched_keyword, score

    return None


def _extract_number(text: str) -> Optional[re.Match]:
    """Return the first regex match object for a number in text, or None."""
    return re.search(r'(-?\d+(?:\.\d+)?)', text)


# ---------------------------------------------------------------------------
# Main parse function
# ---------------------------------------------------------------------------

def parse(text: str) -> ParseResult:
    """
    Parse a raw expense message and return a ParseResult.

    Parsing strategy:
      1. Find the first number in the text.
      2. Everything before it  → keyword phrase
         The number itself     → amount
         Everything after it   → note
      3. If no number found    → whole text is the keyword phrase, amount unknown.
      4. If number is first    → keyword phrase is taken from text AFTER the number.
    """
    number_match = _extract_number(text)

    # ------------------------------------------------------------------
    # Case A: Normal format — "keyword [keyword ...] number [note]"
    # ------------------------------------------------------------------
    if number_match and number_match.start() > 0:
        keyword_phrase = text[:number_match.start()].strip()
        amount         = float(number_match.group(1))
        note           = text[number_match.end():].strip()

        category = _exact_match(keyword_phrase)
        if category:
            return ParseResult(
                status="matched",
                original_text=text,
                category=category,
                amount=amount,
                note=note,
            )

        fuzzy = _fuzzy_match(keyword_phrase)
        if fuzzy:
            category, matched_kw, score = fuzzy
            return ParseResult(
                status="fuzzy_confirm",
                original_text=text,
                category=None,
                amount=amount,
                note=note,
                suggestion=category,
                suggestion_score=score,
                error=f"'{keyword_phrase}' didn't match any category exactly. "
                      f"Closest match: '{category}' (via keyword '{matched_kw}', score {score}).",
            )

        return ParseResult(
            status="no_match",
            original_text=text,
            error=f"'{keyword_phrase}' didn't match any category, even approximately.",
        )

    # ------------------------------------------------------------------
    # Case B: No number — "keyword [keyword ...]" only
    # ------------------------------------------------------------------
    if not number_match:
        keyword_phrase = text.strip()

        category = _exact_match(keyword_phrase)
        if category:
            return ParseResult(
                status="ask_amount",
                original_text=text,
                category=category,
                amount=None,
                error=f"Found category '{category}' but no amount. Need to ask user.",
            )

        fuzzy = _fuzzy_match(keyword_phrase)
        if fuzzy:
            category, matched_kw, score = fuzzy
            return ParseResult(
                status="fuzzy_confirm",
                original_text=text,
                category=None,
                amount=None,
                suggestion=category,
                suggestion_score=score,
                error=f"'{keyword_phrase}' didn't match exactly. "
                      f"Closest: '{category}' (via '{matched_kw}', score {score}). "
                      f"Also need to ask for amount.",
            )

        return ParseResult(
            status="no_match",
            original_text=text,
            error=f"No number found and '{keyword_phrase}' matched nothing.",
        )

    # ------------------------------------------------------------------
    # Case C: Number is first — "number keyword [keyword ...]"
    # ------------------------------------------------------------------
    amount         = float(number_match.group(1))
    keyword_phrase = text[number_match.end():].strip()

    category = _exact_match(keyword_phrase)
    if category:
        return ParseResult(
            status="reversed",
            original_text=text,
            category=category,
            amount=amount,
            note="",
            error="Number came before the category — assumed no note.",
        )

    fuzzy = _fuzzy_match(keyword_phrase)
    if fuzzy:
        category, matched_kw, score = fuzzy
        return ParseResult(
            status="fuzzy_confirm",
            original_text=text,
            category=None,
            amount=amount,
            note="",
            suggestion=category,
            suggestion_score=score,
            error=f"Number came first and '{keyword_phrase}' didn't match exactly. "
                  f"Closest: '{category}' (via '{matched_kw}', score {score}).",
        )

    return ParseResult(
        status="no_match",
        original_text=text,
        error=f"Number came first but '{keyword_phrase}' matched nothing.",
    )
