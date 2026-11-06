-- NEXVARIN: persist verified deployment upstream addresses.
--
-- An upstream address is the internal loopback destination used by
-- local ingress to reach a deployment. It is distinct from the public
-- preview_address, which will be assigned by the ingress integration.
--
-- Only ready deployments may retain an upstream address. The address
-- must use the initial Podman runtime's verified loopback binding and
-- a valid TCP port.

ALTER TABLE deployments
    ADD COLUMN upstream_address TEXT;

ALTER TABLE deployments
    ADD CONSTRAINT deployments_upstream_address_valid
    CHECK (
        upstream_address IS NULL
        OR (
            observed_state = 'ready'
            AND upstream_address ~
                '^127[.]0[.]0[.]1:([1-9]|[1-9][0-9]{1,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$'
        )
    );

COMMENT ON COLUMN deployments.upstream_address IS
    'Verified loopback host:port for a ready deployment; distinct from the public preview_address.';
