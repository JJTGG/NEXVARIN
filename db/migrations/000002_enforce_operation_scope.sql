
-- NEXVARIN: enforce repository scope and reconciliation linkage.
--
-- Apply after 000001_initial_control_plane.sql.
-- Run transactionally using the selected migration runner.
--
-- This migration tightens database invariants. It does not replace
-- application-level authorization, state-transition validation, or
-- worker reconciliation logic.

-- Child operations must remain within the same repository as their parent.
-- Drop the original project-only relationship and replace it with a
-- composite foreign key that includes repository_id.

ALTER TABLE operations
    DROP CONSTRAINT operations_parent_fk;

ALTER TABLE operations
    ADD CONSTRAINT operations_parent_repository_required
    CHECK (
        parent_operation_id IS NULL
        OR repository_id IS NOT NULL
    );

ALTER TABLE operations
    ADD CONSTRAINT operations_parent_same_repository_fk
    FOREIGN KEY (project_id, repository_id, parent_operation_id)
    REFERENCES operations (project_id, repository_id, id)
    ON DELETE RESTRICT;


-- Build, deploy, and cleanup operations in the initial vertical slice
-- must identify the repository they operate on.

ALTER TABLE operations
    ADD CONSTRAINT operations_repository_required_for_execution
    CHECK (
        operation_type NOT IN ('build', 'deploy', 'cleanup')
        OR repository_id IS NOT NULL
    );


-- Reconciliation must investigate an existing operation and preserve
-- its relationship to that operation's repository.

ALTER TABLE operations
    ADD CONSTRAINT operations_reconcile_requires_parent
    CHECK (
        operation_type <> 'reconcile'
        OR parent_operation_id IS NOT NULL
    );


COMMENT ON CONSTRAINT operations_parent_same_repository_fk
ON operations IS
    'Child operations must reference a parent operation in the same project and repository.';

COMMENT ON CONSTRAINT operations_repository_required_for_execution
ON operations IS
    'Build, deploy, and cleanup operations require an explicit repository.';

COMMENT ON CONSTRAINT operations_reconcile_requires_parent
ON operations IS
    'Reconciliation operations must be linked to the operation under investigation.';
