// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package buildccl

import (
	"github.com/cockroachdb/cockroach/pkg/build"
)

func init() {
	build.Distribution = "CCL"
}
