"""Streaming, source-atomic ingestion into authoritative observations."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import logging
import time
from uuid import UUID

from psycopg import Cursor
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .config import PipelineConfig
from .config.organizations import OrganizationCatalog
from .data_manager import DataManager
from .domain import SourceReviewRecord, source_record_to_payload
from .utils.memory import format_process_rss
from .utils.text_processing import normalize_document_url

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionSummary:
    """Bounded summary of sources installed during one run."""

    observations: int
    successful_sources: tuple[str, ...]
    failed_sources: tuple[str, ...]


class IngestionService:
    """Install each normalized source snapshot in one transaction."""

    def __init__(
        self,
        pool: ConnectionPool,
        data_manager: DataManager,
        organizations: OrganizationCatalog,
        *,
        batch_size: int = 500,
        progress_interval_seconds: float = 10.0,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("Ingestion batch size must be positive")
        if progress_interval_seconds < 0:
            raise ValueError("Progress interval must be non-negative")
        self.pool = pool
        self.data_manager = data_manager
        self.organizations = organizations
        self.batch_size = batch_size
        self.progress_interval_seconds = progress_interval_seconds

    def run(
        self,
        run_id: UUID,
        config: PipelineConfig,
        *,
        cached_only: bool = False,
    ) -> IngestionSummary:
        successful: list[str] = []
        failed: list[str] = []
        total = 0
        for source in config.data_sources:
            if not source.enabled:
                continue
            try:
                count = self.install_source(
                    run_id,
                    source.name,
                    self.data_manager.get_data(source, skip_download=cached_only),
                )
            except Exception as exc:
                logger.error("Ingestion failed for %s: %s", source.name, exc)
                failed.append(source.name)
                continue
            successful.append(source.name)
            total += count
        return IngestionSummary(
            observations=total,
            successful_sources=tuple(successful),
            failed_sources=tuple(failed),
        )

    def install_source(
        self,
        run_id: UUID,
        source_name: str,
        records: Iterable[SourceReviewRecord],
    ) -> int:
        """Install one complete source snapshot or leave its old state untouched."""

        started = time.monotonic()
        last_logged = started
        count = 0
        pending: list[tuple[object, ...]] = []
        with self.pool.connection() as connection:
            with connection.transaction(), connection.cursor() as cursor:
                for record in records:
                    if record.source.source_name != source_name:
                        raise ValueError(
                            f"Source {source_name!r} yielded a record owned by "
                            f"{record.source.source_name!r}"
                        )
                    organization = self.organizations.resolve(record.organization)
                    if organization is None:
                        raise ValueError(
                            "Organization is missing from the curated catalog: "
                            f"{record.organization.name!r} "
                            f"({record.organization.website})"
                        )
                    document_key = (
                        normalize_document_url(record.document.observed_url)
                        or record.document.observed_url
                    )
                    pending.append(
                        (
                            record.source.record_key,
                            record.source.source_name,
                            record.source.source_type,
                            record.source.native_id,
                            organization.uri,
                            record.claim.uri,
                            record.claim.text,
                            record.document.observed_url,
                            document_key,
                            record.payload_hash,
                            Jsonb(source_record_to_payload(record)),
                            run_id,
                        )
                    )
                    if len(pending) >= self.batch_size:
                        self._upsert(cursor, pending)
                        count += len(pending)
                        pending.clear()
                        now = time.monotonic()
                        if now - last_logged >= self.progress_interval_seconds:
                            elapsed = now - started
                            logger.info(
                                "Ingestion %s: %d observations; rate=%.2f/s; RSS=%s",
                                source_name,
                                count,
                                count / elapsed if elapsed > 0 else 0,
                                format_process_rss(),
                            )
                            last_logged = now
                if pending:
                    self._upsert(cursor, pending)
                    count += len(pending)
                if count == 0:
                    raise ValueError(
                        f"Source {source_name!r} contained no observations"
                    )
                cursor.execute(
                    """
                    UPDATE source_observations
                    SET active = FALSE
                    WHERE source_name = %s
                      AND active
                      AND last_seen_run_id <> %s
                    """,
                    (source_name, run_id),
                )
        elapsed = time.monotonic() - started
        logger.info(
            "Ingestion finished: %s installed %d observations; rate=%.2f/s; RSS=%s",
            source_name,
            count,
            count / elapsed if elapsed > 0 else 0,
            format_process_rss(),
        )
        return count

    @staticmethod
    def _upsert(cursor: Cursor[object], rows: list[tuple[object, ...]]) -> None:
        cursor.executemany(
            """
            INSERT INTO source_observations (
                record_key,
                source_name,
                source_type,
                native_id,
                organization_uri,
                claim_uri,
                claim_text,
                observed_url,
                document_key,
                payload_hash,
                payload,
                last_seen_run_id
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (record_key) DO UPDATE
            SET source_name = EXCLUDED.source_name,
                source_type = EXCLUDED.source_type,
                native_id = EXCLUDED.native_id,
                organization_uri = EXCLUDED.organization_uri,
                claim_uri = EXCLUDED.claim_uri,
                claim_text = EXCLUDED.claim_text,
                observed_url = EXCLUDED.observed_url,
                document_key = EXCLUDED.document_key,
                payload_hash = EXCLUDED.payload_hash,
                payload = EXCLUDED.payload,
                active = TRUE,
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                last_seen_at = CURRENT_TIMESTAMP
            """,
            rows,
        )
