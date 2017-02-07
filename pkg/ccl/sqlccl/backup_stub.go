// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	_ "cloud.google.com/go/storage"    // force a dependency (see below)
	_ "github.com/rlmcpherson/s3gof3r" // force a dependency (see below)
)

// HACK(dt): Work on backup and restore is happening on a branch, including
// code that uses new external libraries. These dummy imports mean master has
// the same dependencies, and thus the branch can use the same `vendor` dir.
