\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA nexvarin_upstream_upgrade_test;

SET LOCAL search_path = nexvarin_upstream_upgrade_test, public;

-- Build the schema as it existed immediately before migration 000005.
\ir ../migrations/000001_initial_control_plane.sql
\ir ../migrations/000002_enforce_operation_scope.sql
\ir ../migrations/000003_enforce_deployment_operation_uniqueness.sql
\ir ../migrations/000004_add_deployment_upstream_address.sql

-- Create representative pre-migration deployment records.
DO $fixtures$
BEGIN
    INSERT INTO projects (
        id,
        slug,
        name,
        owner_subject
    )
    VALUES (
        '10000000-0000-4000-8000-000000000001',
        'upstream-upgrade-test',
        'Upstream upgrade compatibility test',
        'migration-test'
    );

    INSERT INTO repositories (
        id,
        project_id,
        provider,
        canonical_url,
        display_name,
        default_branch
    )
    VALUES (
        '20000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001',
        'generic_git',
        'https://example.invalid/upstream-upgrade-test.git',
        'upstream-upgrade-test',
        'main'
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
    VALUES (
        '30000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        'build',
        'migration-test',
        'migration-test',
        'upstream-fixture-build',
        repeat('a', 64),
        repeat('b', 40)
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
    VALUES (
        '40000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        '30000000-0000-4000-8000-000000000001',
        repeat('b', 40),
        'migration-test-artifact',
        'sha256:' || repeat('c', 64)
    );

    -- Legacy ready deployment without a recorded upstream.
    INSERT INTO deployments (
        id,
        project_id,
        repository_id,
        artifact_id,
        desired_state,
        observed_state,
        created_by_subject
    )
    VALUES (
        '50000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        '40000000-0000-4000-8000-000000000001',
        'active',
        'ready',
        'migration-test'
    );

    -- Existing pending deployment without an upstream.
    INSERT INTO deployments (
        id,
        project_id,
        repository_id,
        artifact_id,
        desired_state,
        observed_state,
        created_by_subject
    )
    VALUES (
        '50000000-0000-4000-8000-000000000002',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        '40000000-0000-4000-8000-000000000001',
        'active',
        'pending',
        'migration-test'
    );

    -- Existing ready deployment that already has a valid upstream.
    INSERT INTO deployments (
        id,
        project_id,
        repository_id,
        artifact_id,
        desired_state,
        observed_state,
        upstream_address,
        created_by_subject
    )
    VALUES (
        '50000000-0000-4000-8000-000000000003',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        '40000000-0000-4000-8000-000000000001',
        'active',
        'ready',
        '127.0.0.1:32782',
        'migration-test'
    );
END;
$fixtures$;

-- Apply the proposed upgrade migration against existing data.
\ir ../migrations/000005_require_upstream_for_new_deployments.sql

DO $assertions$
DECLARE
    legacy_required BOOLEAN;
    pending_required BOOLEAN;
    existing_addressed_required BOOLEAN;
    new_addressed_required BOOLEAN;
    failed_constraint TEXT;
BEGIN
    -- Existing ready records without upstreams retain their legacy status.
    SELECT upstream_address_required
    INTO legacy_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000001';

    IF legacy_required IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION
            'Legacy ready deployment was not preserved as exempt';
    END IF;

    -- Existing non-ready records remain subject to the new contract.
    SELECT upstream_address_required
    INTO pending_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000002';

    IF pending_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'Existing pending deployment must require an upstream';
    END IF;

    -- Existing ready records with an upstream are not grandfathered.
    SELECT upstream_address_required
    INTO existing_addressed_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000003';

    IF existing_addressed_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'Existing addressed deployment must require an upstream';
    END IF;

    -- A new ready deployment without an upstream must be rejected.
    BEGIN
        INSERT INTO deployments (
            id,
            project_id,
            repository_id,
            artifact_id,
            observed_state,
            created_by_subject
        )
        VALUES (
            '50000000-0000-4000-8000-000000000004',
            '10000000-0000-4000-8000-000000000001',
            '20000000-0000-4000-8000-000000000001',
            '40000000-0000-4000-8000-000000000001',
            'ready',
            'migration-test'
        );

        RAISE EXCEPTION
            'New ready deployment without upstream was unexpectedly accepted';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployments_ready_requires_upstream_address' THEN
                RAISE;
            END IF;
    END;

    -- A new ready deployment with a valid upstream must be accepted.
    INSERT INTO deployments (
        id,
        project_id,
        repository_id,
        artifact_id,
        observed_state,
        upstream_address,
        upstream_address_required,
        created_by_subject
    )
    VALUES (
        '50000000-0000-4000-8000-000000000005',
        '10000000-0000-4000-8000-000000000001',
        '20000000-0000-4000-8000-000000000001',
        '40000000-0000-4000-8000-000000000001',
        'ready',
        '127.0.0.1:32783',
        FALSE,
        'migration-test'
    );

    -- The trigger must override an attempted legacy exemption.
    SELECT upstream_address_required
    INTO new_addressed_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000005';

    IF new_addressed_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'New deployment was allowed to retain a legacy exemption';
    END IF;

    -- An update cannot downgrade an already-enforced deployment.
    UPDATE deployments
    SET upstream_address_required = FALSE
    WHERE id = '50000000-0000-4000-8000-000000000005';

    SELECT upstream_address_required
    INTO new_addressed_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000005';

    IF new_addressed_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'Deployment requirement was downgraded by an update';
    END IF;

    -- Unchanged legacy records retain their original exemption.
    SELECT upstream_address_required
    INTO legacy_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000001';

    IF legacy_required IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION
            'Unchanged legacy deployment lost its compatibility exemption';
    END IF;

    -- Leaving the legacy ready state must promote the record.
    UPDATE deployments
    SET observed_state = 'deploying',
        updated_at = now()
    WHERE id = '50000000-0000-4000-8000-000000000001';

    SELECT upstream_address_required
    INTO legacy_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000001';

    IF legacy_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'Legacy deployment was not promoted after leaving ready state';
    END IF;

    -- A promoted deployment cannot return to ready without an upstream.
    BEGIN
        UPDATE deployments
        SET observed_state = 'ready',
            updated_at = now()
        WHERE id = '50000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'Promoted deployment became ready without an upstream';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployments_ready_requires_upstream_address' THEN
                RAISE;
            END IF;
    END;

    -- The promoted deployment can become ready after a valid upstream
    -- is supplied.
    UPDATE deployments
    SET observed_state = 'ready',
        upstream_address = '127.0.0.1:32784',
        updated_at = now()
    WHERE id = '50000000-0000-4000-8000-000000000001';

    SELECT upstream_address_required
    INTO legacy_required
    FROM deployments
    WHERE id = '50000000-0000-4000-8000-000000000001';

    IF legacy_required IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'Promoted deployment unexpectedly regained a legacy exemption';
    END IF;

    -- A ready deployment cannot discard its upstream while remaining ready.
    BEGIN
        UPDATE deployments
        SET upstream_address = NULL,
            updated_at = now()
        WHERE id = '50000000-0000-4000-8000-000000000001';

        RAISE EXCEPTION
            'Ready deployment was allowed to discard its upstream';
    EXCEPTION
        WHEN check_violation THEN
            GET STACKED DIAGNOSTICS
                failed_constraint = CONSTRAINT_NAME;

            IF failed_constraint <>
                'deployments_ready_requires_upstream_address' THEN
                RAISE;
            END IF;
    END;
END;
$assertions$;

-- The test is deliberately non-destructive to the CI database.
ROLLBACK;

\echo 'Upstream-address migration compatibility tests passed.'
