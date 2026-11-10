-- NEXVARIN: durable deployment ingress route records.
--
-- Route state is independent from deployment runtime state.
-- Caddy is an external system whose active configuration must be
-- reconciled against these durable records.
--
-- The initial scope permits one immutable preview hostname per deployment.
-- The Caddy route identifier is derived from the route UUID:
-- nexvarin-route-<uuid>.
--
-- Reconciliation uses database polling. This migration does not add
-- outbox dispatch or perform any Caddy configuration changes.

CREATE TABLE deployment_routes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL,
    repository_id UUID NOT NULL,
    deployment_id UUID NOT NULL,

    hostname TEXT NOT NULL,

    desired_state TEXT NOT NULL DEFAULT 'active',
    observed_state TEXT NOT NULL DEFAULT 'pending',

    desired_generation BIGINT NOT NULL DEFAULT 1,
    observed_generation BIGINT,

    reconcile_attempt_count INTEGER NOT NULL DEFAULT 0,
    next_reconcile_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,

    last_error_code TEXT,
    last_reconciled_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT deployment_routes_deployment_unique
        UNIQUE (deployment_id),

    CONSTRAINT deployment_routes_hostname_unique
        UNIQUE (hostname),

    CONSTRAINT deployment_routes_deployment_fk
        FOREIGN KEY (project_id, repository_id, deployment_id)
        REFERENCES deployments (project_id, repository_id, id)
        ON DELETE RESTRICT,

    CONSTRAINT deployment_routes_hostname_valid CHECK (
        length(hostname) BETWEEN 3 AND 253
        AND hostname ~
            '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$'
    ),

    CONSTRAINT deployment_routes_desired_state_valid CHECK (
        desired_state IN ('active', 'inactive')
    ),

    CONSTRAINT deployment_routes_observed_state_valid CHECK (
        observed_state IN (
            'pending',
            'active',
            'removing',
            'removed',
            'needs_reconciliation'
        )
    ),

    CONSTRAINT deployment_routes_generation_valid CHECK (
        desired_generation > 0
        AND (
            observed_generation IS NULL
            OR (
                observed_generation > 0
                AND observed_generation <= desired_generation
            )
        )
    ),

    CONSTRAINT deployment_routes_observed_generation_required CHECK (
        observed_state NOT IN ('active', 'removed')
        OR observed_generation IS NOT NULL
    ),

    CONSTRAINT deployment_routes_attempt_count_nonnegative CHECK (
        reconcile_attempt_count >= 0
    ),

    CONSTRAINT deployment_routes_lease_pair CHECK (
        (lease_owner IS NULL) = (lease_expires_at IS NULL)
    ),

    CONSTRAINT deployment_routes_lease_owner_valid CHECK (
        lease_owner IS NULL
        OR (
            length(btrim(lease_owner)) > 0
            AND length(lease_owner) <= 255
        )
    ),

    CONSTRAINT deployment_routes_error_code_valid CHECK (
        last_error_code IS NULL
        OR last_error_code ~ '^[a-z0-9_.-]{1,128}$'
    )
);

CREATE INDEX deployment_routes_project_history_idx
    ON deployment_routes (
        project_id,
        repository_id,
        created_at DESC
    );

CREATE INDEX deployment_routes_reconcile_due_idx
    ON deployment_routes (
        next_reconcile_at,
        created_at,
        id
    )
    WHERE observed_generation IS DISTINCT FROM desired_generation
       OR observed_state IN (
           'pending',
           'removing',
           'needs_reconciliation'
       );

CREATE INDEX deployment_routes_lease_expiry_idx
    ON deployment_routes (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;

CREATE INDEX deployment_routes_active_idx
    ON deployment_routes (updated_at)
    WHERE observed_state = 'active';


-- Route identity and hostname ownership are immutable.
-- Desired configuration changes advance the generation automatically.
-- Controller observations must never move backwards.

CREATE FUNCTION enforce_deployment_route_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.project_id IS DISTINCT FROM OLD.project_id
       OR NEW.repository_id IS DISTINCT FROM OLD.repository_id
       OR NEW.deployment_id IS DISTINCT FROM OLD.deployment_id
       OR NEW.hostname IS DISTINCT FROM OLD.hostname THEN
        RAISE EXCEPTION
            'deployment route identity and hostname are immutable'
            USING
                ERRCODE = '23514',
                CONSTRAINT = 'deployment_routes_identity_immutable';
    END IF;

    IF NEW.desired_generation < OLD.desired_generation THEN
        RAISE EXCEPTION
            'deployment route generation cannot decrease'
            USING
                ERRCODE = '23514',
                CONSTRAINT = 'deployment_routes_generation_monotonic';
    END IF;

    IF NEW.desired_state IS DISTINCT FROM OLD.desired_state THEN
        IF NEW.desired_generation = OLD.desired_generation THEN
            NEW.desired_generation := OLD.desired_generation + 1;
        END IF;

        NEW.next_reconcile_at := clock_timestamp();
    END IF;

    IF OLD.observed_generation IS NOT NULL
       AND (
           NEW.observed_generation IS NULL
           OR NEW.observed_generation < OLD.observed_generation
       ) THEN
        RAISE EXCEPTION
            'deployment route observed generation cannot decrease'
            USING
                ERRCODE = '23514',
                CONSTRAINT = 'deployment_routes_observed_generation_monotonic';
    END IF;

    NEW.updated_at := clock_timestamp();

    RETURN NEW;
END;
$$;

CREATE TRIGGER deployment_routes_revision_guard
    BEFORE UPDATE ON deployment_routes
    FOR EACH ROW
    EXECUTE FUNCTION enforce_deployment_route_revision();


-- Changes to deployment eligibility or its verified upstream invalidate
-- the route's previous reconciliation generation.
--
-- The ingress controller must still evaluate current deployment eligibility
-- before installing a route. A generation bump is a durable wake-up signal,
-- not authorization to route an ineligible deployment.

CREATE FUNCTION schedule_deployment_route_reconciliation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE deployment_routes
    SET
        desired_generation = desired_generation + 1,
        next_reconcile_at = clock_timestamp()
    WHERE deployment_id = NEW.id;

    RETURN NEW;
END;
$$;

CREATE TRIGGER deployments_route_reconciliation
    AFTER UPDATE OF
        desired_state,
        observed_state,
        upstream_address,
        deleted_at
    ON deployments
    FOR EACH ROW
    WHEN (
        OLD.desired_state IS DISTINCT FROM NEW.desired_state
        OR OLD.observed_state IS DISTINCT FROM NEW.observed_state
        OR OLD.upstream_address IS DISTINCT FROM NEW.upstream_address
        OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
    )
    EXECUTE FUNCTION schedule_deployment_route_reconciliation();


COMMENT ON TABLE deployment_routes IS
    'Durable desired and observed preview ingress state. Caddy configuration is external state reconciled from these records.';

COMMENT ON COLUMN deployment_routes.hostname IS
    'Canonical lowercase ASCII hostname. Immutable and globally unique across active, inactive, and removed route records.';

COMMENT ON COLUMN deployment_routes.desired_generation IS
    'Monotonically increasing desired-state generation. Deployment eligibility changes also advance this generation.';

COMMENT ON COLUMN deployment_routes.observed_generation IS
    'Latest desired generation confirmed against active Caddy configuration. NULL means no generation has been confirmed.';

COMMENT ON COLUMN deployment_routes.lease_owner IS
    'Unique controller claim token, not merely a stable process identifier.';

COMMENT ON COLUMN deployment_routes.next_reconcile_at IS
    'Earliest time the route may be claimed for reconciliation. Failed routes remain durable and are not dead-lettered.';
