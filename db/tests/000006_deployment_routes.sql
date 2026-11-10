-- NEXVARIN: deployment route schema and behavioral tests.
--
-- Apply all migrations in an isolated schema, then verify the route
-- identity, ownership, hostname, generation, lease, and deployment
-- reconciliation invariants.
--
-- All fixtures are rolled back. This test is non-destructive to the
-- database used by the CI runner.

\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA nexvarin_deployment_route_test;

SET LOCAL search_path = nexvarin_deployment_route_test, public;

\ir ../migrations/000001_initial_control_plane.sql
\ir ../migrations/000002_enforce_operation_scope.sql
\ir ../migrations/000003_enforce_deployment_operation_uniqueness.sql
\ir ../migrations/000004_add_deployment_upstream_address.sql
\ir ../migrations/000005_require_upstream_for_new_deployments.sql
\ir ../migrations/000006_add_deployment_routes.sql


-- Fixtures: two projects, three repositories' worth of deployment scope,
-- verified artifact references, and three deployment records.

INSERT INTO projects (
    id,
    slug,
    name,
    owner_subject
)
VALUES
(
    '10000000-0000-4000-8000-000000000001',
    'route-test-one',
    'Deployment route test one',
    'route-test'
),
(
    '10000000-0000-4000-8000-000000000002',
    'route-test-two',
    'Deployment route test two',
    'route-test'
);

INSERT INTO repositories (
    id,
    project_id,
    provider,
    canonical_url,
    display_name
)
VALUES
(
    '20000000-0000-4000-8000-000000000001',
    '10000000-0000-4000-8000-000000000001',
    'generic_git',
    'https://example.invalid/route-test-one.git',
    'route-test-one'
),
(
    '20000000-0000-4000-8000-000000000002',
    '10000000-0000-4000-8000-000000000002',
    'generic_git',
    'https://example.invalid/route-test-two.git',
    'route-test-two'
);

INSERT INTO operations (
    id,
    project_id,
    repository_id,
    operation_type,
    requested_by,
    idempotency_scope,
    idempotency_key,
    request_fingerprint,
    source_revision
)
VALUES
(
    '30000000-0000-4000-8000-000000000001',
    '10000000-0000-4000-8000-000000000001',
    '20000000-0000-4000-8000-000000000001',
    'build',
    'route-test',
    'route-test',
    'route-build-one',
    repeat('a', 64),
    repeat('b', 40)
),
(
    '30000000-0000-4000-8000-000000000002',
    '10000000-0000-4000-8000-000000000002',
    '20000000-0000-4000-8000-000000000002',
    'build',
    'route-test',
    'route-test',
    'route-build-two',
    repeat('c', 64),
    repeat('d', 40)
);

INSERT INTO artifacts (
    id,
    project_id,
    repository_id,
    build_operation_id,
    source_revision,
    artifact_reference,
    content_digest
)
VALUES
(
    '40000000-0000-4000-8000-000000000001',
    '10000000-0000-4000-8000-000000000001',
    '20000000-0000-4000-8000-000000000001',
    '30000000-0000-4000-8000-000000000001',
    repeat('b', 40),
    'localhost/nexvarin/route-test-one:fixture',
    'sha256:' || repeat('1', 64)
),
(
    '40000000-0000-4000-8000-000000000002',
    '10000000-0000-4000-8000-000000000002',
    '20000000-0000-4000-8000-000000000002',
    '30000000-0000-4000-8000-000000000002',
    repeat('d', 40),
    'localhost/nexvarin/route-test-two:fixture',
    'sha256:' || repeat('2', 64)
);

INSERT INTO deployments (
    id,
    project_id,
    repository_id,
    artifact_id,
    created_by_subject
)
VALUES
(
    '50000000-0000-4000-8000-000000000001',
    '10000000-0000-4000-8000-000000000001',
    '20000000-0000-4000-8000-000000000001',
    '40000000-0000-4000-8000-000000000001',
    'route-test'
),
(
    '50000000-0000-4000-8000-000000000002',
    '10000000-0000-4000-8000-000000000001',
    '20000000-0000-4000-8000-000000000001',
    '40000000-0000-4000-8000-000000000001',
    'route-test'
),
(
    '50000000-0000-4000-8000-000000000003',
    '10000000-0000-4000-8000-000000000002',
    '20000000-0000-4000-8000-000000000002',
    '40000000-0000-4000-8000-000000000002',
    'route-test'
);


-- A valid route starts with generation one, pending observation,
-- and no observed generation or lease.

INSERT INTO deployment_routes (
    id,
    project_id,
    repository_id,
    deployment_id,
    hostname
)
VALUES (
    '60000000-0000-4000-8000-000000000001',
    '10000000-0000-4000-8000-000000000001',
    '20000000-0000-4000-8000-000000000001',
    '50000000-0000-4000-8000-000000000001',
    'app-one.preview.example'
);

DO $assertions$
DECLARE
    failed_constraint TEXT;
    actual_generation BIGINT;
BEGIN
    -- One deployment cannot own multiple route records.

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000001',
            'second.preview.example'
        );

        RAISE EXCEPTION
            'A deployment was allowed to own multiple routes';
    EXCEPTION
        WHEN unique_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_deployment_unique' THEN
                RAISE;
            END IF;
    END;

    -- A route cannot claim a deployment through another project/repository.

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000002',
            '20000000-0000-4000-8000-000000000002',
            '50000000-0000-4000-8000-000000000001',
            'wrong-scope.preview.example'
        );

        RAISE EXCEPTION
            'A cross-project deployment route was accepted';
    EXCEPTION
        WHEN foreign_key_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_deployment_fk' THEN
                RAISE;
            END IF;
    END;

    -- Hostnames are globally unique, including inactive and removed routes.

    UPDATE deployment_routes
    SET desired_state = 'inactive'
    WHERE id = '60000000-0000-4000-8000-000000000001';

    UPDATE deployment_routes
    SET observed_generation = 2,
        observed_state = 'removed'
    WHERE id = '60000000-0000-4000-8000-000000000001';

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000002',
            'app-one.preview.example'
        );

        RAISE EXCEPTION
            'A removed route hostname was allowed to be reused';
    EXCEPTION
        WHEN unique_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_hostname_unique' THEN
                RAISE;
            END IF;
    END;

    -- Hostname syntax rejects uppercase, wildcard, underscore,
    -- empty-label, and trailing-dot forms.

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000002',
            'Invalid.preview.example'
        );

        RAISE EXCEPTION
            'An uppercase hostname was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_hostname_valid' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000002',
            '*.preview.example'
        );

        RAISE EXCEPTION
            'A wildcard hostname was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_hostname_valid' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000002',
            'bad_name.preview.example'
        );

        RAISE EXCEPTION
            'An underscore hostname was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_hostname_valid' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000002',
            'bad..preview.example'
        );

        RAISE EXCEPTION
            'An empty hostname label was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_hostname_valid' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO deployment_routes (
            project_id,
            repository_id,
            deployment_id,
            hostname
        )
        VALUES (
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '50000000-0000-4000-8000-000000000002',
            'trailing.preview.example.'
        );

        RAISE EXCEPTION
            'A trailing-dot hostname was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_hostname_valid' THEN
                RAISE;
            END IF;
    END;

    -- Route identity and hostname cannot be reassigned.

    BEGIN
        UPDATE deployment_routes
        SET hostname = 'changed.preview.example'
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'A route hostname was mutable';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_identity_immutable' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET deployment_id =
            '50000000-0000-4000-8000-000000000002'
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'A route deployment identity was mutable';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_identity_immutable' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET id = '60000000-0000-4000-8000-000000000009'
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'A route primary identity was mutable';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_identity_immutable' THEN
                RAISE;
            END IF;
    END;

    -- The route was deactivated at generation two. Reactivation advances
    -- desired generation to three while the observed removal remains at two.

    SELECT desired_generation
    INTO actual_generation
    FROM deployment_routes
    WHERE id = '60000000-0000-4000-8000-000000000001';

    IF actual_generation <> 2 THEN
        RAISE EXCEPTION
            'Initial desired-state change produced generation %, expected 2',
            actual_generation;
    END IF;

    UPDATE deployment_routes
    SET desired_state = 'active'
    WHERE id = '60000000-0000-4000-8000-000000000001';

    SELECT desired_generation
    INTO actual_generation
    FROM deployment_routes
    WHERE id = '60000000-0000-4000-8000-000000000001';

    IF actual_generation <> 3 THEN
        RAISE EXCEPTION
            'Route reactivation produced generation %, expected 3',
            actual_generation;
    END IF;

    -- Updating other fields without changing desired state does not
    -- advance the generation a second time.

    UPDATE deployment_routes
    SET desired_state = 'active',
        last_error_code = 'test_retry'
    WHERE id = '60000000-0000-4000-8000-000000000001';

    SELECT desired_generation
    INTO actual_generation
    FROM deployment_routes
    WHERE id = '60000000-0000-4000-8000-000000000001';

    IF actual_generation <> 3 THEN
        RAISE EXCEPTION
            'Unchanged desired state unexpectedly advanced generation';
    END IF;

    -- Desired generations cannot move backwards.

    BEGIN
        UPDATE deployment_routes
        SET desired_generation = 2
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'Desired generation was allowed to decrease';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_generation_monotonic' THEN
                RAISE;
            END IF;
    END;

    -- Observations can lag the desired generation, but cannot move backwards.
    -- The old removal remains confirmed at generation two while generation
    -- three is awaiting reconciliation.

    BEGIN
        UPDATE deployment_routes
        SET observed_generation = 1
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'Observed generation was allowed to decrease';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_observed_generation_monotonic' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET observed_generation = NULL
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'Observed generation was allowed to become NULL';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_observed_generation_monotonic' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET observed_generation = 4
        WHERE id = '60000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'Observed generation was allowed to exceed desired generation';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_generation_valid' THEN
                RAISE;
            END IF;
    END;

    -- The route catches up to the current desired generation.

    UPDATE deployment_routes
    SET observed_state = 'active',
        observed_generation = 3
    WHERE id = '60000000-0000-4000-8000-000000000001';

    -- Deployment eligibility changes must invalidate the route generation.

    UPDATE deployments
    SET observed_state = 'ready',
        upstream_address = '127.0.0.1:31001'
    WHERE id = '50000000-0000-4000-8000-000000000001';

    SELECT desired_generation
    INTO actual_generation
    FROM deployment_routes
    WHERE id = '60000000-0000-4000-8000-000000000001';

    IF actual_generation <> 4 THEN
        RAISE EXCEPTION
            'Deployment readiness/upstream change produced generation %, expected 4',
            actual_generation;
    END IF;

    UPDATE deployments
    SET desired_state = 'inactive'
    WHERE id = '50000000-0000-4000-8000-000000000001';

    UPDATE deployments
    SET deleted_at = clock_timestamp()
    WHERE id = '50000000-0000-4000-8000-000000000001';

    SELECT desired_generation
    INTO actual_generation
    FROM deployment_routes
    WHERE id = '60000000-0000-4000-8000-000000000001';

    IF actual_generation <> 6 THEN
        RAISE EXCEPTION
            'Deployment deactivation/deletion produced generation %, expected 6',
            actual_generation;
    END IF;

    -- Updates unrelated to deployment route eligibility do not invalidate it.

    UPDATE deployments
    SET updated_at = clock_timestamp()
    WHERE id = '50000000-0000-4000-8000-000000000001';

    SELECT desired_generation
    INTO actual_generation
    FROM deployment_routes
    WHERE id = '60000000-0000-4000-8000-000000000001';

    IF actual_generation <> 6 THEN
        RAISE EXCEPTION
            'An unrelated deployment update advanced route generation';
    END IF;

    -- A deployment referenced by a route cannot be deleted.

    BEGIN
        DELETE FROM deployments
        WHERE id = '50000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'A deployment with a route was deleted';
    EXCEPTION
        WHEN foreign_key_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_deployment_fk' THEN
                RAISE;
            END IF;
    END;
END;
$assertions$;


-- A second valid route is used for independent lease and state checks.

INSERT INTO deployment_routes (
    id,
    project_id,
    repository_id,
    deployment_id,
    hostname
)
VALUES (
    '60000000-0000-4000-8000-000000000002',
    '10000000-0000-4000-8000-000000000001',
    '20000000-0000-4000-8000-000000000001',
    '50000000-0000-4000-8000-000000000002',
    'app-two.preview.example'
);

DO $assertions$
DECLARE
    failed_constraint TEXT;
BEGIN
    -- Controller lease owner and expiry must be set or cleared together.

    BEGIN
        UPDATE deployment_routes
        SET lease_owner = 'controller:route-test'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'A route lease owner was accepted without an expiry';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <> 'deployment_routes_lease_pair' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET lease_expires_at = now() + interval '1 minute'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'A route lease expiry was accepted without an owner';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <> 'deployment_routes_lease_pair' THEN
                RAISE;
            END IF;
    END;

    -- Expired leases must remain representable so a controller can reclaim them.

    UPDATE deployment_routes
    SET lease_owner = 'controller:expired-test',
        lease_expires_at = now() - interval '1 second'
    WHERE id = '60000000-0000-4000-8000-000000000002';

    UPDATE deployment_routes
    SET lease_owner = NULL,
        lease_expires_at = NULL
    WHERE id = '60000000-0000-4000-8000-000000000002';

    -- Blank and oversized lease owner tokens are invalid.

    BEGIN
        UPDATE deployment_routes
        SET lease_owner = ' ',
            lease_expires_at = now() + interval '1 minute'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'A blank lease owner was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_lease_owner_valid' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET lease_owner = repeat('x', 256),
            lease_expires_at = now() + interval '1 minute'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'An oversized lease owner was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_lease_owner_valid' THEN
                RAISE;
            END IF;
    END;

    -- Attempt count and error-code constraints.

    BEGIN
        UPDATE deployment_routes
        SET reconcile_attempt_count = -1
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'A negative reconciliation attempt count was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_attempt_count_nonnegative' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET last_error_code = 'Invalid Error Code'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'An invalid reconciliation error code was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_error_code_valid' THEN
                RAISE;
            END IF;
    END;

    -- Active or removed observations must identify the observed generation.

    BEGIN
        UPDATE deployment_routes
        SET observed_state = 'active'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'An active route without an observed generation was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_observed_generation_required' THEN
                RAISE;
            END IF;
    END;

    BEGIN
        UPDATE deployment_routes
        SET observed_state = 'removed'
        WHERE id = '60000000-0000-4000-8000-000000000002';

        RAISE EXCEPTION
            'A removed route without an observed generation was accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployment_routes_observed_generation_required' THEN
                RAISE;
            END IF;
    END;
END;
$assertions$;


-- The test is deliberately non-destructive to the CI database.

ROLLBACK;

\echo 'Deployment route schema and behavioral tests passed.'
