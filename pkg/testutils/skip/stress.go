// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package skip

import "github.com/cockroachdb/cockroach/pkg/util/envutil"

var nightlyStress = envutil.EnvOrDefaultBool("COCKROACH_NIGHTLY_STRESS", false)

var stress = envutil.EnvOrDefaultBool("COCKROACH_STRESS", false)

// NightlyStress returns true iff the process is running as part of CockroachDB's
// nightly stress tests.
func NightlyStress() bool {
	return nightlyStress
}

// Stress returns true iff the process is running under a local _dev_ instance of the stress, i.e., ./dev test ... --stress
func Stress() bool {
	return stress
}
