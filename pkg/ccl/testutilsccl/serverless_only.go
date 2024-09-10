// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package testutilsccl

import "github.com/cockroachdb/cockroach/pkg/testutils/skip"

// ServerlessOnly is called in tests to mark them as testing functionality that
// is Serverless specific. This is changed from a no-op to a test skip once a
// version is no longer used by Serverless in production.
func ServerlessOnly(t skip.SkippableTest) {
	// Uncomment in release branches that no longer support serverless.
	// skip.IgnoreLint(t, "version is not used by serverless in production")
}
