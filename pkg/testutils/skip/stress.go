// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package skip

import "github.com/cockroachdb/cockroach/pkg/util/envutil"

var stress = envutil.EnvOrDefaultBool("COCKROACH_NIGHTLY_STRESS", false)

// NightlyStress returns true iff the process is running as part of CockroachDB's
// nightly stress tests.
func NightlyStress() bool {
	return stress
}
