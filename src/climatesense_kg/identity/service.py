"""Direct exact identity resolution against authoritative observations."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..utils.progress import ProgressLogger
from ..utils.text_processing import normalize_document_url
from .fingerprints import fingerprint_text

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IdentitySummary:
    observations: int
    documents_created: int
    reviews_created: int


@dataclass(frozen=True)
class _Observation:
    record_key: str
    organization_uri: str
    claim_uri: str
    existing_review_id: UUID | None
    urls: tuple[tuple[int, str, str], ...]
    preferred_url: str
    preferred_url_rank: int
    content: str | None
    normalized_text_hash: str | None
    word_count: int


@dataclass
class _Document:
    id: UUID
    organization_uri: str
    preferred_url: str
    preferred_url_rank: int
    content: str | None
    normalized_text_hash: str | None
    word_count: int
    new: bool = False

    def absorb(self, observation: _Observation) -> None:
        candidate_url = (observation.preferred_url_rank, observation.preferred_url)
        current_url = (self.preferred_url_rank, self.preferred_url)
        if candidate_url < current_url:
            self.preferred_url_rank, self.preferred_url = candidate_url
        candidate_content = (
            observation.word_count,
            observation.normalized_text_hash or "",
        )
        current_content = (self.word_count, self.normalized_text_hash or "")
        if observation.content is not None and (
            candidate_content[0] > current_content[0]
            or (
                candidate_content[0] == current_content[0]
                and candidate_content[1]
                and (
                    not current_content[1] or candidate_content[1] < current_content[1]
                )
            )
        ):
            self.content = observation.content
            self.normalized_text_hash = observation.normalized_text_hash
            self.word_count = observation.word_count


@dataclass(frozen=True)
class _Review:
    id: UUID
    document_id: UUID
    organization_uri: str
    claim_uri: str
    new: bool = False


class IdentityService:
    """Assign persistent documents and claim reviews using explicit exact rules."""

    def __init__(
        self,
        pool: ConnectionPool,
        *,
        batch_size: int = 500,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Identity batch size must be positive")
        if progress_interval_seconds < 0:
            raise ValueError("Identity progress interval must be non-negative")
        self.pool = pool
        self.batch_size = batch_size
        self.progress_interval_seconds = progress_interval_seconds

    def run(self) -> IdentitySummary:
        total = self._count()
        processed = 0
        created_documents = 0
        created_reviews = 0
        progress = ProgressLogger(
            logger,
            "Identity resolution",
            total,
            interval_seconds=self.progress_interval_seconds,
        )
        cursor_name = f"identity_{int(time.time() * 1_000_000)}"
        with self.pool.connection() as connection:
            with (
                connection.transaction(),
                connection.cursor(name=cursor_name, row_factory=dict_row) as cursor,
            ):
                cursor.execute(
                    """
                    SELECT observation.record_key,
                           observation.organization_uri,
                           observation.claim_uri,
                           observation.claim_review_id,
                           observation.observed_url,
                           observation.payload #>> '{document,source_text}'
                               AS source_text,
                           extraction.final_url,
                           extraction.canonical_url,
                           extraction.content AS extracted_text,
                           extraction.normalized_text_hash,
                           extraction.word_count
                    FROM source_observations AS observation
                    LEFT JOIN document_extractions AS extraction
                      ON extraction.document_key = observation.document_key
                     AND extraction.status = 'success'
                    WHERE observation.active
                    ORDER BY observation.organization_uri,
                             observation.document_key,
                             observation.claim_uri,
                             observation.record_key
                    """
                )
                while rows := cursor.fetchmany(self.batch_size):
                    observations = [self._observation(row) for row in rows]
                    batch = self._resolve_batch(observations)
                    processed += len(observations)
                    created_documents += batch.documents_created
                    created_reviews += batch.reviews_created
                    progress.update(
                        processed,
                        {
                            "documents_created": created_documents,
                            "reviews_created": created_reviews,
                        },
                    )
        progress.update(
            processed,
            {
                "documents_created": created_documents,
                "reviews_created": created_reviews,
            },
            force=True,
        )
        return IdentitySummary(processed, created_documents, created_reviews)

    def _count(self) -> int:
        with self.pool.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM source_observations WHERE active")
            row = cursor.fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _observation(row: dict[str, object]) -> _Observation:
        extracted = row["extracted_text"]
        source = row["source_text"]
        content = extracted if isinstance(extracted, str) else source
        fingerprint = fingerprint_text(content if isinstance(content, str) else None)
        text_hash = row["normalized_text_hash"] or fingerprint.normalized_text_hash
        word_count = (
            int(row["word_count"])
            if row["word_count"] is not None
            else fingerprint.word_count
        )
        raw_urls = (
            (0, row["canonical_url"]),
            (1, row["final_url"]),
            (2, row["observed_url"]),
        )
        urls: list[tuple[int, str, str]] = []
        seen: set[str] = set()
        for rank, raw_url in raw_urls:
            if not isinstance(raw_url, str):
                continue
            normalized = normalize_document_url(raw_url) or raw_url
            if normalized in seen:
                continue
            seen.add(normalized)
            urls.append((rank, normalized, raw_url))
        preferred_rank, _normalized, preferred_url = min(urls)
        review_id = row["claim_review_id"]
        return _Observation(
            record_key=str(row["record_key"]),
            organization_uri=str(row["organization_uri"]),
            claim_uri=str(row["claim_uri"]),
            existing_review_id=(review_id if isinstance(review_id, UUID) else None),
            urls=tuple(urls),
            preferred_url=preferred_url,
            preferred_url_rank=preferred_rank,
            content=content if isinstance(content, str) else None,
            normalized_text_hash=str(text_hash) if text_hash else None,
            word_count=word_count,
        )

    def _resolve_batch(self, observations: list[_Observation]) -> IdentitySummary:
        alias_keys = {
            (observation.organization_uri, normalized)
            for observation in observations
            for _rank, normalized, _url in observation.urls
        }
        hash_keys = {
            (observation.organization_uri, observation.normalized_text_hash)
            for observation in observations
            if observation.normalized_text_hash
        }
        existing_review_ids = {
            observation.existing_review_id
            for observation in observations
            if observation.existing_review_id is not None
        }
        reviews = self._load_reviews(existing_review_ids)
        aliases = self._load_aliases(alias_keys)
        hashes = self._load_hashes(hash_keys)
        document_ids = (
            {review.document_id for review in reviews.values()}
            | {document_id for values in aliases.values() for document_id in values}
            | {document_id for values in hashes.values() for document_id in values}
        )
        documents = self._load_documents(document_ids)
        selected_documents: dict[str, UUID] = {}
        new_aliases: dict[tuple[str, str], tuple[str, UUID]] = {}
        new_text_hashes: dict[tuple[str, str], UUID] = {}

        for observation in observations:
            if observation.existing_review_id is not None:
                review = reviews.get(observation.existing_review_id)
                if review is None:
                    raise RuntimeError(
                        f"Observation {observation.record_key} references a missing "
                        f"claim review {observation.existing_review_id}"
                    )
                document_id = review.document_id
            else:
                candidates: set[UUID] = set()
                for _rank, normalized, _url in observation.urls:
                    candidates.update(
                        aliases.get((observation.organization_uri, normalized), set())
                    )
                if observation.normalized_text_hash:
                    candidates.update(
                        hashes.get(
                            (
                                observation.organization_uri,
                                observation.normalized_text_hash,
                            ),
                            set(),
                        )
                    )
                if len(candidates) > 1:
                    ids = ", ".join(sorted(str(value) for value in candidates))
                    raise RuntimeError(
                        "Conflicting exact document identity evidence for "
                        f"{observation.record_key}: {ids}"
                    )
                if candidates:
                    document_id = next(iter(candidates))
                else:
                    document_id = uuid4()
                    documents[document_id] = _Document(
                        id=document_id,
                        organization_uri=observation.organization_uri,
                        preferred_url=observation.preferred_url,
                        preferred_url_rank=observation.preferred_url_rank,
                        content=None,
                        normalized_text_hash=None,
                        word_count=0,
                        new=True,
                    )
            document = documents[document_id]
            if document.organization_uri != observation.organization_uri:
                raise RuntimeError(
                    f"Document {document_id} belongs to another organization"
                )
            document.absorb(observation)
            selected_documents[observation.record_key] = document_id
            for _rank, normalized, url in observation.urls:
                key = (observation.organization_uri, normalized)
                known = aliases.get(key, set())
                if known and known != {document_id}:
                    raise RuntimeError(
                        f"URL alias {normalized} identifies conflicting documents"
                    )
                aliases[key] = {document_id}
                if not known:
                    new_aliases[key] = (url, document_id)
            if observation.normalized_text_hash:
                key = (
                    observation.organization_uri,
                    observation.normalized_text_hash,
                )
                known = hashes.get(key, set())
                if known and document_id not in known:
                    raise RuntimeError(
                        "Exact document text identifies conflicting persistent "
                        f"documents for observation {observation.record_key}"
                    )
                if not known:
                    new_text_hashes[key] = document_id
                hashes.setdefault(key, set()).add(document_id)

        document_claims = {
            (selected_documents[item.record_key], item.claim_uri)
            for item in observations
            if item.existing_review_id is None
        }
        reviews_by_document_claim = self._load_document_claim_reviews(document_claims)
        assignments: dict[str, UUID] = {}
        new_reviews: dict[UUID, _Review] = {}
        for observation in observations:
            if observation.existing_review_id is not None:
                assignments[observation.record_key] = observation.existing_review_id
                continue
            key = (selected_documents[observation.record_key], observation.claim_uri)
            review = reviews_by_document_claim.get(key)
            if review is None:
                review = _Review(
                    id=uuid4(),
                    document_id=key[0],
                    organization_uri=observation.organization_uri,
                    claim_uri=observation.claim_uri,
                    new=True,
                )
                reviews_by_document_claim[key] = review
                new_reviews[review.id] = review
            assignments[observation.record_key] = review.id

        self._commit(
            documents,
            new_aliases,
            new_text_hashes,
            new_reviews,
            assignments,
        )
        return IdentitySummary(
            observations=len(observations),
            documents_created=sum(document.new for document in documents.values()),
            reviews_created=len(new_reviews),
        )

    def _load_reviews(self, ids: set[UUID]) -> dict[UUID, _Review]:
        if not ids:
            return {}
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    SELECT id, document_id, organization_uri, claim_uri
                    FROM claim_reviews
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (list(ids),),
                )
                rows = cursor.fetchall()
        return {
            row["id"]: _Review(
                row["id"],
                row["document_id"],
                row["organization_uri"],
                row["claim_uri"],
            )
            for row in rows
        }

    def _load_aliases(
        self,
        keys: set[tuple[str, str]],
    ) -> dict[tuple[str, str], set[UUID]]:
        if not keys:
            return {}
        organizations, urls = zip(*keys, strict=True)
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    WITH requested AS (
                        SELECT * FROM UNNEST(%s::text[], %s::text[])
                            AS key(organization_uri, normalized_url)
                    )
                    SELECT url.organization_uri, url.normalized_url, url.document_id
                    FROM document_urls AS url
                    JOIN requested USING (organization_uri, normalized_url)
                    """,
                    (list(organizations), list(urls)),
                )
                rows = cursor.fetchall()
        result: dict[tuple[str, str], set[UUID]] = {}
        for row in rows:
            result.setdefault(
                (row["organization_uri"], row["normalized_url"]), set()
            ).add(row["document_id"])
        return result

    def _load_hashes(
        self,
        keys: set[tuple[str, str]],
    ) -> dict[tuple[str, str], set[UUID]]:
        if not keys:
            return {}
        organizations, hashes = zip(*keys, strict=True)
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    WITH requested AS (
                        SELECT * FROM UNNEST(%s::text[], %s::text[])
                            AS key(organization_uri, normalized_text_hash)
                    )
                    SELECT hash.organization_uri,
                           hash.normalized_text_hash,
                           hash.document_id
                    FROM document_text_hashes AS hash
                    JOIN requested USING (
                        organization_uri, normalized_text_hash
                    )
                    """,
                    (list(organizations), list(hashes)),
                )
                rows = cursor.fetchall()
        result: dict[tuple[str, str], set[UUID]] = {}
        for row in rows:
            result.setdefault(
                (row["organization_uri"], row["normalized_text_hash"]), set()
            ).add(row["document_id"])
        return result

    def _load_documents(self, ids: set[UUID]) -> dict[UUID, _Document]:
        if not ids:
            return {}
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    SELECT id, organization_uri, preferred_url,
                           preferred_url_rank, content,
                           normalized_text_hash, word_count
                    FROM documents
                    WHERE id = ANY(%s::uuid[])
                    """,
                    (list(ids),),
                )
                rows = cursor.fetchall()
        return {
            row["id"]: _Document(
                id=row["id"],
                organization_uri=row["organization_uri"],
                preferred_url=row["preferred_url"],
                preferred_url_rank=row["preferred_url_rank"],
                content=row["content"],
                normalized_text_hash=row["normalized_text_hash"],
                word_count=row["word_count"],
            )
            for row in rows
        }

    def _load_document_claim_reviews(
        self,
        keys: set[tuple[UUID, str]],
    ) -> dict[tuple[UUID, str], _Review]:
        if not keys:
            return {}
        documents, claims = zip(*keys, strict=True)
        with self.pool.connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    WITH requested AS (
                        SELECT * FROM UNNEST(%s::uuid[], %s::text[])
                            AS key(document_id, claim_uri)
                    )
                    SELECT review.id, review.document_id,
                           review.organization_uri, review.claim_uri
                    FROM claim_reviews AS review
                    JOIN requested USING (document_id, claim_uri)
                    """,
                    (list(documents), list(claims)),
                )
                rows = cursor.fetchall()
        return {
            (row["document_id"], row["claim_uri"]): _Review(
                row["id"],
                row["document_id"],
                row["organization_uri"],
                row["claim_uri"],
            )
            for row in rows
        }

    def _commit(
        self,
        documents: dict[UUID, _Document],
        aliases: dict[tuple[str, str], tuple[str, UUID]],
        text_hashes: dict[tuple[str, str], UUID],
        reviews: dict[UUID, _Review],
        assignments: dict[str, UUID],
    ) -> None:
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO documents (
                        id, organization_uri, preferred_url, preferred_url_rank,
                        content, normalized_text_hash, word_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET preferred_url = EXCLUDED.preferred_url,
                        preferred_url_rank = EXCLUDED.preferred_url_rank,
                        content = EXCLUDED.content,
                        normalized_text_hash = EXCLUDED.normalized_text_hash,
                        word_count = EXCLUDED.word_count,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    [
                        (
                            document.id,
                            document.organization_uri,
                            document.preferred_url,
                            document.preferred_url_rank,
                            document.content,
                            document.normalized_text_hash,
                            document.word_count,
                        )
                        for document in documents.values()
                    ],
                )
                if aliases:
                    cursor.executemany(
                        """
                        INSERT INTO document_urls (
                            organization_uri, normalized_url, url, document_id
                        )
                        VALUES (%s, %s, %s, %s)
                        """,
                        [
                            (organization, normalized, url, document_id)
                            for (organization, normalized), (
                                url,
                                document_id,
                            ) in aliases.items()
                        ],
                    )
                if text_hashes:
                    cursor.executemany(
                        """
                        INSERT INTO document_text_hashes (
                            organization_uri, normalized_text_hash, document_id
                        )
                        VALUES (%s, %s, %s)
                        """,
                        [
                            (organization, text_hash, document_id)
                            for (organization, text_hash), document_id in (
                                text_hashes.items()
                            )
                        ],
                    )
                if reviews:
                    cursor.executemany(
                        """
                        INSERT INTO claim_reviews (
                            id, document_id, organization_uri, claim_uri
                        )
                        VALUES (%s, %s, %s, %s)
                        """,
                        [
                            (
                                review.id,
                                review.document_id,
                                review.organization_uri,
                                review.claim_uri,
                            )
                            for review in reviews.values()
                        ],
                    )
                cursor.executemany(
                    """
                    UPDATE source_observations
                    SET claim_review_id = %s
                    WHERE record_key = %s
                    """,
                    [
                        (review_id, record_key)
                        for record_key, review_id in assignments.items()
                    ],
                )
