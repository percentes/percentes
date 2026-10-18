// Package configs carries the reference configurations a binary builds
// on away from the source tree.
package configs

import _ "embed"

// ReleaseReference is release.reference.yaml.
//
//go:embed release.reference.yaml
var ReleaseReference []byte
