# API Reference

The full HTTP contract of both planes is the OpenAPI spec:
[openapi.yaml](openapi.yaml) — import it into Swagger UI / Redoc, or
browse it raw. Every non-gateway-owned Bot API method is proxied
verbatim, so the spec lists only the gateway-specific surface plus the
proxying rule.

The typed Go client (`pkg/client`) is the ergonomic view of the same
contract; the [examples/](../examples/) show it in three languages.

The MTProto method allowlist and its deliberate exclusions are pinned
in [sidecar-spec.md](sidecar-spec.md) section 10.
