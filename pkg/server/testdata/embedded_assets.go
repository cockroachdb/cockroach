package testdata

import "embed"

// TestAssets is a test-only embed.FS, and should not be used for any other purposes.
//go:embed assets
var TestAssets embed.FS
