// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package operation

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"

type Operation interface {
	// TODO(bilal): Instead of encapsulating test.Test, copy over the small
	// set of relevant methods, ideally moving them to a shared interface.
	test.Test

	GetCleanupState(string) string
	SetCleanupState(string, string)
}
