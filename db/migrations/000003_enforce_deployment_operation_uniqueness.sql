-- Ensure each operation references at most one deployment record.
--
-- A deploy operation can be retried. Its existing deployment record
-- must be reused rather than creating duplicate records.
--
-- Multiple deployments without a linked operation remain permitted.

CREATE UNIQUE INDEX deployments_last_operation_unique
    ON deployments (
        project_id,
        repository_id,
        last_operation_id
    )
    WHERE last_operation_id IS NOT NULL;
