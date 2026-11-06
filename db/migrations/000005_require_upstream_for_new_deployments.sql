-- NEXVARIN: require verified upstream addresses for new deployments.
--
-- Existing deployments may contain ready records created before upstream
-- addresses were persisted. Preserve those records as explicit legacy
-- exceptions rather than inventing an upstream or silently changing their
-- observed state.
--
-- New deployment records must satisfy the upstream-address requirement
-- before they can be marked ready. Legacy exceptions are promoted into the
-- enforced contract when their state or upstream address changes.

ALTER TABLE deployments
    ADD COLUMN upstream_address_required BOOLEAN NOT NULL DEFAULT TRUE;

-- Only existing ready deployments without a recorded upstream are
-- grandfathered. All other existing records remain under the new contract.
UPDATE deployments
SET upstream_address_required = FALSE
WHERE observed_state = 'ready'
  AND upstream_address IS NULL;

-- An exemption is valid only while the deployment remains ready and
-- has no recorded upstream address.
ALTER TABLE deployments
    ADD CONSTRAINT deployments_upstream_address_legacy_shape
    CHECK (
        upstream_address_required
        OR (
            observed_state = 'ready'
            AND upstream_address IS NULL
        )
    );

-- Deployments subject to the new contract cannot become ready without
-- an upstream address. Migration 000004 validates its format.
ALTER TABLE deployments
    ADD CONSTRAINT deployments_ready_requires_upstream_address
    CHECK (
        NOT upstream_address_required
        OR observed_state <> 'ready'
        OR upstream_address IS NOT NULL
    );

-- Keep the function in the active schema. This permits the migration
-- to run both in the production schema and in an isolated upgrade test.
CREATE FUNCTION enforce_deployment_upstream_address_requirement()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.upstream_address_required := TRUE;
    ELSIF OLD.upstream_address_required THEN
        NEW.upstream_address_required := TRUE;
    ELSIF NEW.observed_state <> 'ready'
       OR NEW.upstream_address IS NOT NULL THEN
        NEW.upstream_address_required := TRUE;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER deployments_upstream_address_requirement_guard
    BEFORE INSERT OR UPDATE ON deployments
    FOR EACH ROW
    EXECUTE FUNCTION enforce_deployment_upstream_address_requirement();

COMMENT ON COLUMN deployments.upstream_address_required IS
    'Whether this deployment must have a verified upstream before becoming ready. False is reserved for pre-migration ready records without an upstream and is promoted when they leave that legacy state.';