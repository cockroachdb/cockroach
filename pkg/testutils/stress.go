// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package testutils

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

var stress = envutil.EnvOrDefaultBool("COCKROACH_NIGHTLY_STRESS", false)

// NightlyStress returns true iff the process is running as part of CockroachDB's
// nightly stress tests.
func NightlyStress() bool {
	return stress
}
