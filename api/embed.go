// Package apispec embeds the OpenAPI specification so the API binary can
// serve it at /openapi.yaml and render Swagger UI at /docs.
package apispec

import _ "embed"

//go:embed openapi.yaml
var Spec []byte
