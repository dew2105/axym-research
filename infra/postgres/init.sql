CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS medicaid_claims (
    billing_provider_npi    VARCHAR(10) NOT NULL,
    servicing_provider_npi  VARCHAR(10) NOT NULL,
    hcpcs_code              VARCHAR(10) NOT NULL,
    claim_from_month        DATE NOT NULL,
    total_unique_beneficiaries INTEGER,
    total_claims            INTEGER,
    total_paid              NUMERIC(15, 2)
);

-- Indexes built during ingestion (included in timing)
CREATE INDEX IF NOT EXISTS idx_billing_npi ON medicaid_claims(billing_provider_npi);
CREATE INDEX IF NOT EXISTS idx_servicing_npi ON medicaid_claims(servicing_provider_npi);
CREATE INDEX IF NOT EXISTS idx_hcpcs ON medicaid_claims(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_claim_month ON medicaid_claims(claim_from_month);
