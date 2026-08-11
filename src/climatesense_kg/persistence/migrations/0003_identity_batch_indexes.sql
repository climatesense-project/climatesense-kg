-- Supporting indexes for set-based identity evidence loading.
CREATE INDEX review_documents_org_preferred_url
    ON review_documents (organization_uri, preferred_url);

CREATE INDEX review_documents_org_final_url
    ON review_documents (organization_uri, final_url)
    WHERE final_url IS NOT NULL;

CREATE INDEX source_review_records_claim_review
    ON source_review_records (claim_review_id);

CREATE INDEX source_review_records_document
    ON source_review_records (document_id);

CREATE INDEX source_review_records_claim
    ON source_review_records (claim_uri, claim_review_id);

CREATE INDEX source_review_records_observed_url
    ON source_review_records (observed_url);

CREATE INDEX source_review_records_final_url
    ON source_review_records (final_url)
    WHERE final_url IS NOT NULL;

CREATE INDEX source_review_records_canonical_url
    ON source_review_records (canonical_url)
    WHERE canonical_url IS NOT NULL;
