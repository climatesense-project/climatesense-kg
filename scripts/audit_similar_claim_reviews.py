#!/usr/bin/env python3
"""Find near-duplicate claim reviews attached to the same claim in N-Triples RDF."""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Sequence
import csv
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
import re
import sys
import unicodedata
from urllib.parse import urlsplit

from rdflib.plugins.parsers.ntriples import W3CNTriplesParser
from rdflib.term import Identifier

SCHEMA_AUTHOR = "http://schema.org/author"
SCHEMA_DATE_PUBLISHED = "http://schema.org/datePublished"
SCHEMA_ITEM_REVIEWED = "http://schema.org/itemReviewed"
SCHEMA_TEXT = "http://schema.org/text"
SCHEMA_URL = "http://schema.org/url"

WORD_PATTERN = re.compile(r"\w+", flags=re.UNICODE)
REPORT_COLUMNS = (
    "similarity",
    "exact_normalized_text",
    "same_domain",
    "same_author",
    "claim_uri",
    "claim_text",
    "left_review_uri",
    "left_url",
    "left_author",
    "left_date_published",
    "left_word_count",
    "left_excerpt",
    "right_review_uri",
    "right_url",
    "right_author",
    "right_date_published",
    "right_word_count",
    "right_excerpt",
)


@dataclass
class ReviewRecord:
    """RDF properties needed for one claim review."""

    claim_uri: str | None = None
    texts: list[str] = field(default_factory=list)
    urls: set[str] = field(default_factory=set)
    authors: set[str] = field(default_factory=set)
    dates_published: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class TextFeatures:
    """Normalized representation used by the similarity calculation."""

    source_text: str
    normalized_text: str
    word_count: int
    shingles: frozenset[tuple[str, ...]]


@dataclass(frozen=True)
class AuditCounts:
    """Counts describing the candidate search and its results."""

    total_reviews: int
    reviews_with_text: int
    claims_with_text_reviews: int
    claims_with_multiple_text_reviews: int
    candidate_pairs: int
    eligible_pairs: int
    matches: int
    exact_matches: int
    same_domain_matches: int
    same_author_matches: int


class AuditSink:
    """Collect only duplicate-audit fields while streaming N-Triples."""

    def __init__(self) -> None:
        self.reviews: dict[str, ReviewRecord] = {}
        self.claim_texts: dict[str, str] = {}

    def triple(
        self,
        subject_node: Identifier,
        predicate_node: Identifier,
        object_node: Identifier,
    ) -> None:
        """Receive one parsed triple from rdflib's streaming parser."""

        subject = str(subject_node)
        predicate = str(predicate_node)

        if "/claim-review/" in subject:
            if predicate not in {
                SCHEMA_AUTHOR,
                SCHEMA_DATE_PUBLISHED,
                SCHEMA_ITEM_REVIEWED,
                SCHEMA_TEXT,
                SCHEMA_URL,
            }:
                return

            review = self.reviews.setdefault(subject, ReviewRecord())
            value = str(object_node)
            if predicate == SCHEMA_ITEM_REVIEWED:
                review.claim_uri = value
            elif predicate == SCHEMA_TEXT:
                review.texts.append(value)
            elif predicate == SCHEMA_URL:
                review.urls.add(value)
            elif predicate == SCHEMA_AUTHOR:
                review.authors.add(value)
            elif predicate == SCHEMA_DATE_PUBLISHED:
                review.dates_published.add(value)
            return

        if "/claim/" in subject and predicate == SCHEMA_TEXT:
            self.claim_texts[subject] = str(object_node)


def normalize_text(text: str) -> str:
    """Normalize text for robust comparison across extraction variants."""

    normalized_unicode = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(WORD_PATTERN.findall(normalized_unicode))


def build_features(text: str, shingle_size: int) -> TextFeatures:
    """Build normalized word shingles for one review body."""

    normalized = normalize_text(text)
    words = normalized.split()
    shingles = frozenset(
        tuple(words[index : index + shingle_size])
        for index in range(len(words) - shingle_size + 1)
    )
    return TextFeatures(
        source_text=text,
        normalized_text=normalized,
        word_count=len(words),
        shingles=shingles,
    )


def shingle_containment(left: TextFeatures, right: TextFeatures) -> float:
    """Return the share of the smaller shingle set present in the larger one."""

    smaller_size = min(len(left.shingles), len(right.shingles))
    if smaller_size == 0:
        return 0.0
    return len(left.shingles & right.shingles) / smaller_size


def preferred_text(record: ReviewRecord) -> str:
    """Choose the most complete body if an RDF review has several text literals."""

    return max(record.texts, key=len)


def first_or_empty(values: set[str]) -> str:
    """Return a deterministic representative for an optional RDF property."""

    return min(values, default="")


def joined(values: set[str]) -> str:
    """Return deterministic pipe-separated values for a multi-valued property."""

    return " | ".join(sorted(values))


def normalized_domain(url: str) -> str:
    """Return a hostname suitable for grouping www and non-www URLs."""

    hostname = (urlsplit(url).hostname or "").casefold()
    return hostname.removeprefix("www.")


def excerpt(text: str, length: int = 240) -> str:
    """Return a single-line excerpt for quick manual review."""

    compact = " ".join(text.split())
    if len(compact) <= length:
        return compact
    return compact[: length - 1].rstrip() + "…"


def build_report_row(
    *,
    score: float,
    claim_uri: str,
    claim_text: str,
    left_uri: str,
    left_record: ReviewRecord,
    left_features: TextFeatures,
    right_uri: str,
    right_record: ReviewRecord,
    right_features: TextFeatures,
) -> dict[str, str]:
    """Create one serializable report row."""

    left_url = first_or_empty(left_record.urls)
    right_url = first_or_empty(right_record.urls)
    return {
        "similarity": f"{score:.6f}",
        "exact_normalized_text": str(
            left_features.normalized_text == right_features.normalized_text
        ).lower(),
        "same_domain": str(
            bool(normalized_domain(left_url))
            and normalized_domain(left_url) == normalized_domain(right_url)
        ).lower(),
        "same_author": str(
            bool(left_record.authors) and left_record.authors == right_record.authors
        ).lower(),
        "claim_uri": claim_uri,
        "claim_text": claim_text,
        "left_review_uri": left_uri,
        "left_url": left_url,
        "left_author": joined(left_record.authors),
        "left_date_published": joined(left_record.dates_published),
        "left_word_count": str(left_features.word_count),
        "left_excerpt": excerpt(left_features.source_text),
        "right_review_uri": right_uri,
        "right_url": right_url,
        "right_author": joined(right_record.authors),
        "right_date_published": joined(right_record.dates_published),
        "right_word_count": str(right_features.word_count),
        "right_excerpt": excerpt(right_features.source_text),
    }


def audit_reviews(
    sink: AuditSink,
    *,
    threshold: float,
    min_words: int,
    shingle_size: int,
) -> tuple[list[dict[str, str]], AuditCounts]:
    """Find similar review pairs within exact-claim candidate blocks."""

    reviews_by_claim: dict[str, list[str]] = defaultdict(list)
    for review_uri, review in sink.reviews.items():
        if review.claim_uri and review.texts:
            reviews_by_claim[review.claim_uri].append(review_uri)

    multiple_review_claims = {
        claim_uri: sorted(review_uris)
        for claim_uri, review_uris in reviews_by_claim.items()
        if len(review_uris) > 1
    }

    feature_cache: dict[str, TextFeatures] = {}
    report_rows: list[dict[str, str]] = []
    candidate_pairs = 0
    eligible_pairs = 0
    exact_matches = 0

    def features_for(review_uri: str, record: ReviewRecord) -> TextFeatures:
        cached = feature_cache.get(review_uri)
        if cached is not None:
            return cached
        features = build_features(preferred_text(record), shingle_size)
        feature_cache[review_uri] = features
        return features

    for claim_uri, review_uris in multiple_review_claims.items():
        for left_uri, right_uri in combinations(review_uris, 2):
            candidate_pairs += 1
            left_record = sink.reviews[left_uri]
            right_record = sink.reviews[right_uri]
            left_features = features_for(left_uri, left_record)
            right_features = features_for(right_uri, right_record)

            if min(left_features.word_count, right_features.word_count) < min_words:
                continue

            eligible_pairs += 1
            score = shingle_containment(left_features, right_features)
            if score < threshold:
                continue

            if left_features.normalized_text == right_features.normalized_text:
                exact_matches += 1
            report_rows.append(
                build_report_row(
                    score=score,
                    claim_uri=claim_uri,
                    claim_text=sink.claim_texts.get(claim_uri, ""),
                    left_uri=left_uri,
                    left_record=left_record,
                    left_features=left_features,
                    right_uri=right_uri,
                    right_record=right_record,
                    right_features=right_features,
                )
            )

    report_rows.sort(
        key=lambda row: (
            -float(row["similarity"]),
            row["claim_uri"],
            row["left_review_uri"],
            row["right_review_uri"],
        )
    )
    same_domain_matches = sum(row["same_domain"] == "true" for row in report_rows)
    same_author_matches = sum(row["same_author"] == "true" for row in report_rows)
    counts = AuditCounts(
        total_reviews=len(sink.reviews),
        reviews_with_text=sum(bool(review.texts) for review in sink.reviews.values()),
        claims_with_text_reviews=len(reviews_by_claim),
        claims_with_multiple_text_reviews=len(multiple_review_claims),
        candidate_pairs=candidate_pairs,
        eligible_pairs=eligible_pairs,
        matches=len(report_rows),
        exact_matches=exact_matches,
        same_domain_matches=same_domain_matches,
        same_author_matches=same_author_matches,
    )
    return report_rows, counts


def write_report(report_rows: list[dict[str, str]], output_path: Path) -> None:
    """Write the manual-review CSV."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(report_rows)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(
        description=(
            "Audit N-Triples snapshots for similar claim-review bodies attached "
            "to the same exact claim."
        )
    )
    parser.add_argument(
        "rdf_files",
        nargs="+",
        type=Path,
        help="One or more N-Triples files belonging to the same snapshot",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("similar-claim-reviews.csv"),
        help="CSV report path (default: similar-claim-reviews.csv)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.9,
        help="Minimum shingle-containment score from 0 to 1 (default: 0.9)",
    )
    parser.add_argument(
        "--min-words",
        type=int,
        default=50,
        help="Minimum word count of the shorter review (default: 50)",
    )
    parser.add_argument(
        "--shingle-size",
        type=int,
        default=5,
        help="Number of consecutive words per shingle (default: 5)",
    )
    args = parser.parse_args(argv)

    if not 0 <= args.threshold <= 1:
        parser.error("--threshold must be between 0 and 1")
    if args.min_words < 1:
        parser.error("--min-words must be at least 1")
    if args.shingle_size < 1:
        parser.error("--shingle-size must be at least 1")
    missing_files = [path for path in args.rdf_files if not path.is_file()]
    if missing_files:
        parser.error(
            "RDF file not found: " + ", ".join(str(path) for path in missing_files)
        )

    return args


def main(argv: Sequence[str] | None = None) -> int:
    """Run the duplicate audit and write its CSV report."""

    args = parse_args(argv)
    sink = AuditSink()
    for rdf_path in args.rdf_files:
        print(f"Reading {rdf_path}...", file=sys.stderr)
        with rdf_path.open(encoding="utf-8") as stream:
            W3CNTriplesParser(sink=sink).parse(stream)

    report_rows, counts = audit_reviews(
        sink,
        threshold=args.threshold,
        min_words=args.min_words,
        shingle_size=args.shingle_size,
    )
    write_report(report_rows, args.output)

    print(f"Claim reviews: {counts.total_reviews:,}")
    print(f"Claim reviews with text: {counts.reviews_with_text:,}")
    print(f"Claims with review text: {counts.claims_with_text_reviews:,}")
    print(
        "Claims with multiple text-bearing reviews: "
        f"{counts.claims_with_multiple_text_reviews:,}"
    )
    print(f"Candidate pairs within those claims: {counts.candidate_pairs:,}")
    print(f"Pairs meeting the word-count minimum: {counts.eligible_pairs:,}")
    print(f"Pairs at or above {args.threshold:.3f}: {counts.matches:,}")
    print(f"Normalized exact-text matches: {counts.exact_matches:,}")
    print(f"Matches from the same domain: {counts.same_domain_matches:,}")
    print(f"Matches with the same author set: {counts.same_author_matches:,}")
    print(f"CSV report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
