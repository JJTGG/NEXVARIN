
-- NEXVARIN: initial control-plane schema
--
-- Apply this migration transactionally using the chosen migration runner.
--
-- Security contract:
-- Never persist credentials, access tokens, or secret-bearing request bodies
-- in request_payload, payload, metadata, details, or error_summary.
--
-- Ownership, authorization, idempotency replay behavior, operation transitions,
-- and worker reconciliation must also be enforced by the application layer.

CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    owner_subject TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT projects_slug_format CHECK (
        slug ~ '^[a-z0-9][a-z0-9-]{0,62}$'
        AND slug !~ '-$'
    ),
    CONSTRAINT projects_name_not_blank CHECK (
        length(btrim(name)) > 0
    ),
    CONSTRAINT projects_owner_not_blank CHECK (
        length(btrim(owner_subject)) > 0
    ),
    CONSTRAINT projects_status_valid CHECK (
        status IN ('active', 'archived')
    )
);

CREATE TABLE repositories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    provider TEXT NOT NULL,
    provider_repository_id TEXT,
    canonical_url TEXT NOT NULL,
    display_name TEXT NOT NULL,
    default_branch TEXT NOT NULL DEFAULT 'main',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT repositories_project_id_unique
        UNIQUE (project_id, id),

    CONSTRAINT repositories_project_fk
        FOREIGN KEY (project_id)
        REFERENCES projects (id)
        ON DELETE RESTRICT,

    CONSTRAINT repositories_provider_valid CHECK (
        provider IN ('github', 'gitlab', 'generic_git')
    ),
    CONSTRAINT repositories_url_not_blank CHECK (
        length(btrim(canonical_url)) > 0
    ),
    CONSTRAINT repositories_name_not_blank CHECK (
        length(btrim(display_name)) > 0
    ),
    CONSTRAINT repositories_branch_not_blank CHECK (
        length(btrim(default_branch)) > 0
    )
);

CREATE UNIQUE INDEX repositories_provider_identity_unique
    ON repositories (project_id, provider, provider_repository_id)
    WHERE provider_repository_id IS NOT NULL;

CREATE INDEX repositories_active_by_project_idx
    ON repositories (project_id, created_at DESC)
    WHERE is_active = TRUE;


CREATE TABLE operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    repository_id UUID,
    parent_operation_id UUID,

    operation_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',

    requested_by TEXT NOT NULL,
    idempotency_scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    source_revision TEXT,

    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,

    provider_operation_ref TEXT,
    error_code TEXT,
    error_summary TEXT,

    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT operations_project_id_unique
        UNIQUE (project_id, id),

    CONSTRAINT operations_project_repository_id_unique
        UNIQUE (project_id, repository_id, id),

    CONSTRAINT operations_project_fk
        FOREIGN KEY (project_id)
        REFERENCES projects (id)
        ON DELETE RESTRICT,

    CONSTRAINT operations_repository_fk
        FOREIGN KEY (project_id, repository_id)
        REFERENCES repositories (project_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT operations_parent_fk
        FOREIGN KEY (project_id, parent_operation_id)
        REFERENCES operations (project_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT operations_type_valid CHECK (
        operation_type IN ('build', 'deploy', 'cleanup', 'reconcile')
    ),

    CONSTRAINT operations_status_valid CHECK (
        status IN (
            'queued',
            'running',
            'retry_wait',
            'succeeded',
            'failed',
            'cancel_requested',
            'cancelled',
            'needs_reconciliation'
        )
    ),

    CONSTRAINT operations_requester_not_blank CHECK (
        length(btrim(requested_by)) > 0
    ),

    CONSTRAINT operations_idempotency_scope_not_blank CHECK (
        length(btrim(idempotency_scope)) > 0
        AND length(idempotency_scope) <= 255
    ),

    CONSTRAINT operations_idempotency_key_not_blank CHECK (
        length(btrim(idempotency_key)) > 0
        AND length(idempotency_key) <= 255
    ),

    CONSTRAINT operations_fingerprint_sha256 CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    ),

    CONSTRAINT operations_payload_object CHECK (
        jsonb_typeof(request_payload) = 'object'
    ),

    CONSTRAINT operations_source_revision_valid CHECK (
        source_revision IS NULL
        OR (
            source_revision ~ '^[0-9a-f]+$'
            AND length(source_revision) IN (40, 64)
        )
    ),

    CONSTRAINT operations_build_requires_revision CHECK (
        operation_type <> 'build'
        OR source_revision IS NOT NULL
    ),

    CONSTRAINT operations_attempt_limits CHECK (
        attempt_count >= 0
        AND max_attempts > 0
        AND attempt_count <= max_attempts
    ),

    CONSTRAINT operations_lease_pair CHECK (
        (lease_owner IS NULL) = (lease_expires_at IS NULL)
    ),

    CONSTRAINT operations_running_requires_lease CHECK (
        status NOT IN ('running', 'cancel_requested')
        OR (
            lease_owner IS NOT NULL
            AND lease_expires_at IS NOT NULL
        )
    ),

    CONSTRAINT operations_terminal_timestamp CHECK (
        (status IN ('succeeded', 'failed', 'cancelled'))
        = (finished_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX operations_idempotency_unique
    ON operations (
        project_id,
        requested_by,
        idempotency_scope,
        idempotency_key
    );

CREATE INDEX operations_project_history_idx
    ON operations (project_id, created_at DESC);

CREATE INDEX operations_due_queue_idx
    ON operations (next_attempt_at, created_at)
    WHERE status IN ('queued', 'retry_wait');

CREATE INDEX operations_reconciliation_idx
    ON operations (created_at)
    WHERE status = 'needs_reconciliation';

CREATE INDEX operations_expired_lease_idx
    ON operations (lease_expires_at)
    WHERE status IN ('running', 'cancel_requested')
      AND lease_expires_at IS NOT NULL;


CREATE TABLE operation_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    operation_id UUID NOT NULL,

    event_type TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT,
    actor_subject TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT operation_events_operation_fk
        FOREIGN KEY (project_id, operation_id)
        REFERENCES operations (project_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT operation_events_details_object CHECK (
        jsonb_typeof(details) = 'object'
    ),

    CONSTRAINT operation_events_type_not_blank CHECK (
        length(btrim(event_type)) > 0
    )
);

CREATE INDEX operation_events_history_idx
    ON operation_events (project_id, operation_id, created_at, id);


CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    operation_id UUID NOT NULL,

    event_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,

    delivered_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    last_error_code TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT outbox_operation_fk
        FOREIGN KEY (project_id, operation_id)
        REFERENCES operations (project_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT outbox_event_type_not_blank CHECK (
        length(btrim(event_type)) > 0
    ),

    CONSTRAINT outbox_dedupe_key_not_blank CHECK (
        length(btrim(dedupe_key)) > 0
    ),

    CONSTRAINT outbox_payload_object CHECK (
        jsonb_typeof(payload) = 'object'
    ),

    CONSTRAINT outbox_attempt_count_nonnegative CHECK (
        attempt_count >= 0
    ),

    CONSTRAINT outbox_lease_pair CHECK (
        (lease_owner IS NULL) = (lease_expires_at IS NULL)
    ),

    CONSTRAINT outbox_delivered_releases_lease CHECK (
        delivered_at IS NULL
        OR (lease_owner IS NULL AND lease_expires_at IS NULL)
    ),

    CONSTRAINT outbox_not_delivered_and_dead_lettered CHECK (
        delivered_at IS NULL OR dead_lettered_at IS NULL
    ),

    CONSTRAINT outbox_dead_letter_releases_lease CHECK (
        dead_lettered_at IS NULL
        OR (
            delivered_at IS NULL
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
        )
    )
);

CREATE INDEX outbox_pending_delivery_idx
    ON outbox_events (available_at, created_at)
    WHERE delivered_at IS NULL
      AND dead_lettered_at IS NULL;


CREATE TABLE artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    repository_id UUID NOT NULL,
    build_operation_id UUID NOT NULL,

    source_revision TEXT NOT NULL,
    artifact_kind TEXT NOT NULL DEFAULT 'oci_image',
    artifact_reference TEXT NOT NULL,
    content_digest TEXT NOT NULL,

    verification_status TEXT NOT NULL DEFAULT 'pending',
    verified_at TIMESTAMPTZ,

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT artifacts_project_repository_id_unique
        UNIQUE (project_id, repository_id, id),

    CONSTRAINT artifacts_build_operation_fk
        FOREIGN KEY (project_id, repository_id, build_operation_id)
        REFERENCES operations (project_id, repository_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT artifacts_source_revision_valid CHECK (
        source_revision ~ '^[0-9a-f]+$'
        AND length(source_revision) IN (40, 64)
    ),

    CONSTRAINT artifacts_kind_valid CHECK (
        artifact_kind = 'oci_image'
    ),

    CONSTRAINT artifacts_reference_not_blank CHECK (
        length(btrim(artifact_reference)) > 0
    ),

    CONSTRAINT artifacts_digest_sha256 CHECK (
        content_digest ~ '^sha256:[0-9a-f]{64}$'
    ),

    CONSTRAINT artifacts_verification_status_valid CHECK (
        verification_status IN ('pending', 'verified', 'failed')
    ),

    CONSTRAINT artifacts_verified_timestamp_consistent CHECK (
        (verification_status = 'verified') = (verified_at IS NOT NULL)
    ),

    CONSTRAINT artifacts_metadata_object CHECK (
        jsonb_typeof(metadata) = 'object'
    )
);

CREATE UNIQUE INDEX artifacts_build_digest_unique
    ON artifacts (build_operation_id, content_digest);

CREATE INDEX artifacts_project_repository_history_idx
    ON artifacts (project_id, repository_id, created_at DESC);


CREATE TABLE deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    repository_id UUID NOT NULL,
    artifact_id UUID,
    last_operation_id UUID,

    environment TEXT NOT NULL DEFAULT 'preview',
    desired_state TEXT NOT NULL DEFAULT 'active',
    observed_state TEXT NOT NULL DEFAULT 'pending',

    runtime_instance_ref TEXT,
    preview_address TEXT,

    created_by_subject TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,

    CONSTRAINT deployments_project_repository_id_unique
        UNIQUE (project_id, repository_id, id),

    CONSTRAINT deployments_repository_fk
        FOREIGN KEY (project_id, repository_id)
        REFERENCES repositories (project_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT deployments_artifact_fk
        FOREIGN KEY (project_id, repository_id, artifact_id)
        REFERENCES artifacts (project_id, repository_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT deployments_last_operation_fk
        FOREIGN KEY (project_id, repository_id, last_operation_id)
        REFERENCES operations (project_id, repository_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT deployments_environment_valid CHECK (
        environment = 'preview'
    ),

    CONSTRAINT deployments_desired_state_valid CHECK (
        desired_state IN ('active', 'inactive', 'deleted')
    ),

    CONSTRAINT deployments_observed_state_valid CHECK (
        observed_state IN (
            'unknown',
            'pending',
            'deploying',
            'ready',
            'failed',
            'stopping',
            'stopped',
            'deleting',
            'deleted',
            'needs_reconciliation'
        )
    ),

    CONSTRAINT deployments_ready_requires_artifact CHECK (
        observed_state <> 'ready' OR artifact_id IS NOT NULL
    ),

    CONSTRAINT deployments_creator_not_blank CHECK (
        length(btrim(created_by_subject)) > 0
    )
);

CREATE INDEX deployments_project_repository_history_idx
    ON deployments (project_id, repository_id, updated_at DESC);

CREATE INDEX deployments_reconciliation_idx
    ON deployments (updated_at)
    WHERE observed_state = 'needs_reconciliation';


CREATE TABLE audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID,
    actor_subject TEXT,

    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id UUID,
    outcome TEXT NOT NULL,

    request_id TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT audit_events_project_fk
        FOREIGN KEY (project_id)
        REFERENCES projects (id)
        ON DELETE RESTRICT,

    CONSTRAINT audit_events_action_not_blank CHECK (
        length(btrim(action)) > 0
    ),

    CONSTRAINT audit_events_resource_type_not_blank CHECK (
        length(btrim(resource_type)) > 0
    ),

    CONSTRAINT audit_events_outcome_valid CHECK (
        outcome IN ('success', 'denied', 'failure')
    ),

    CONSTRAINT audit_events_details_object CHECK (
        jsonb_typeof(details) = 'object'
    )
);

CREATE INDEX audit_events_project_history_idx
    ON audit_events (project_id, created_at DESC)
    WHERE project_id IS NOT NULL;

CREATE INDEX audit_events_actor_history_idx
    ON audit_events (actor_subject, created_at DESC)
    WHERE actor_subject IS NOT NULL;


COMMENT ON TABLE operations IS
    'Durable operation state. External side effects require idempotency and reconciliation.';

COMMENT ON COLUMN operations.request_fingerprint IS
    'SHA-256 hex fingerprint of the normalized request; application must reject key reuse with a different fingerprint.';

COMMENT ON COLUMN operations.request_payload IS
    'Normalized, non-secret request data only. Never store credentials or secret-bearing bodies.';

COMMENT ON TABLE operation_events IS
    'Operation history. Treat events as append-only in application permissions and write paths.';

COMMENT ON TABLE outbox_events IS
    'Transactional work-dispatch records. Insert the operation and its initial outbox event in the same database transaction.';

COMMENT ON COLUMN outbox_events.payload IS
    'Non-secret event data only. Include stable identifiers, not credentials or full request bodies.';

COMMENT ON TABLE artifacts IS
    'Build outputs with source provenance and immutable content-digest identity.';

COMMENT ON TABLE deployments IS
    'Desired and observed deployment state are stored separately for reconciliation.';

COMMENT ON TABLE audit_events IS
    'Security and administrative audit trail. Details must be structured, minimized, and redacted.';
