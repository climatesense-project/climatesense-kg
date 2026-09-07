"""Plan exact identity reconciliation using compact metadata, without I/O."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4


@dataclass(frozen=True, slots=True)
class Document:
    id: UUID
    created_at: datetime


@dataclass(frozen=True, slots=True)
class Review:
    id: UUID
    document_id: UUID
    claim_uri: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class Observation:
    record_key: str
    claim_uri: str
    review_id: UUID | None
    urls: tuple[str, ...]
    text_hash: str | None


@dataclass(frozen=True)
class Merge:
    retired_id: UUID
    survivor_id: UUID
    evidence: dict[str, object]


@dataclass(frozen=True)
class ReconciliationPlan:
    documents: dict[UUID, UUID]
    reviews: dict[UUID, UUID]
    # Existing surviving and newly allocated reviews, keyed by review UUID.
    review_documents: dict[UUID, tuple[UUID, str]]
    # record key -> (canonical document UUID, canonical review UUID)
    assignments: dict[str, tuple[UUID, UUID]]
    new_documents: frozenset[UUID]
    new_reviews: frozenset[UUID]
    document_merges: tuple[Merge, ...]
    review_merges: tuple[Merge, ...]


class IdentityPlanner:
    """One organization's connected components of exact identity evidence."""

    def __init__(
        self,
        organization_uri: str,
        documents: list[Document],
        reviews: list[Review],
    ) -> None:
        self.organization_uri = organization_uri
        self.documents = {document.id: document for document in documents}
        self.reviews = {review.id: review for review in reviews}
        self.observations: dict[str, Observation] = {}
        self.parents: dict[str, str] = {}
        self.sizes: dict[str, int] = {}
        self.keys: dict[tuple[str, str], str] = {}
        self.links: list[dict[str, str]] = []
        for document in documents:
            self._add(f"d:{document.id}")
        for review in reviews:
            if review.document_id not in self.documents:
                raise RuntimeError(
                    f"Review {review.id} references a missing or foreign document "
                    f"{review.document_id} in {organization_uri}"
                )

    def _add(self, node: str) -> None:
        self.parents[node] = node
        self.sizes[node] = 1

    def _root(self, node: str) -> str:
        root = node
        while self.parents[root] != root:
            root = self.parents[root]
        while self.parents[node] != root:
            parent = self.parents[node]
            self.parents[node] = root
            node = parent
        return root

    def _join(self, left: str, right: str, kind: str, value: str) -> None:
        a, b = self._root(left), self._root(right)
        if a == b:
            return
        if (self.sizes[a], a) < (self.sizes[b], b):
            a, b = b, a
        self.parents[b] = a
        self.sizes[a] += self.sizes[b]
        self.links.append({"left": left, "right": right, "kind": kind, "value": value})

    def add_existing_key(self, kind: str, value: str, document_id: UUID) -> None:
        if document_id not in self.documents:
            raise RuntimeError(
                f"{kind} {value} references a missing or foreign document {document_id}"
            )
        self._key(kind, value, f"d:{document_id}")

    def _key(self, kind: str, value: str, node: str) -> None:
        previous = self.keys.setdefault((kind, value), node)
        self._join(node, previous, kind, value)

    def observe(self, observation: Observation) -> None:
        if observation.record_key in self.observations:
            raise RuntimeError(f"Duplicate observation {observation.record_key}")
        node = f"o:{observation.record_key}"
        self._add(node)
        self.observations[observation.record_key] = observation
        if observation.review_id is not None:
            review = self.reviews.get(observation.review_id)
            if review is None:
                raise RuntimeError(
                    f"Observation {observation.record_key} references a missing or "
                    f"foreign review {observation.review_id}"
                )
            self._join(node, f"d:{review.document_id}", "review", str(review.id))
        for url in observation.urls:
            self._key("url", url, node)
        if observation.text_hash:
            self._key("text_hash", observation.text_hash, node)

    def plan(self) -> ReconciliationPlan:
        members: dict[str, list[Document]] = defaultdict(list)
        for document in self.documents.values():
            members[self._root(f"d:{document.id}")].append(document)
        survivors: dict[str, UUID] = {}
        new_documents: set[UUID] = set()
        for node in sorted(self.parents):
            root = self._root(node)
            if root in survivors:
                continue
            if members[root]:
                survivors[root] = min(
                    members[root],
                    key=lambda document: (document.created_at, document.id),
                ).id
            else:
                survivors[root] = uuid4()
                new_documents.add(survivors[root])

        evidence: dict[str, list[dict[str, str]]] = defaultdict(list)
        for link in self.links:
            root = self._root(link["left"])
            if len(members[root]) > 1:
                evidence[root].append(link)
        documents = {
            document_id: survivors[self._root(f"d:{document_id}")]
            for document_id in self.documents
        }
        document_merges = tuple(
            Merge(
                old,
                new,
                {
                    "organization_uri": self.organization_uri,
                    "links": evidence[self._root(f"d:{old}")],
                },
            )
            for old, new in sorted(documents.items())
            if old != new
        )

        # A corrected source claim retains its original review identity anchor.
        # Only reviews with the same stored logical claim key are consolidated.
        by_claim: dict[tuple[UUID, str], UUID] = {}
        reviews: dict[UUID, UUID] = {}
        review_documents: dict[UUID, tuple[UUID, str]] = {}
        for review in sorted(
            self.reviews.values(), key=lambda item: (item.created_at, item.id)
        ):
            key = (documents[review.document_id], review.claim_uri)
            survivor = by_claim.setdefault(key, review.id)
            reviews[review.id] = survivor
            review_documents[survivor] = key
        review_merges = tuple(
            Merge(
                old,
                new,
                {
                    "document_id": str(review_documents[new][0]),
                    "claim_uri": review_documents[new][1],
                },
            )
            for old, new in sorted(reviews.items())
            if old != new
        )
        assignments: dict[str, tuple[UUID, UUID]] = {}
        new_reviews: set[UUID] = set()
        for record_key, observation in sorted(self.observations.items()):
            document_id = survivors[self._root(f"o:{record_key}")]
            if observation.review_id is not None:
                review_id = reviews[observation.review_id]
            else:
                key = (document_id, observation.claim_uri)
                if key not in by_claim:
                    by_claim[key] = uuid4()
                    new_reviews.add(by_claim[key])
                    review_documents[by_claim[key]] = key
                review_id = by_claim[key]
            assignments[record_key] = (document_id, review_id)
        return ReconciliationPlan(
            documents=documents,
            reviews=reviews,
            review_documents=review_documents,
            assignments=assignments,
            new_documents=frozenset(new_documents),
            new_reviews=frozenset(new_reviews),
            document_merges=document_merges,
            review_merges=review_merges,
        )
