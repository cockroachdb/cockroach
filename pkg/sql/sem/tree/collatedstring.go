// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "golang.org/x/text/collate"

// DefaultCollationTag is the "default" collation for strings.
const DefaultCollationTag = "default"

func init() {
	if collate.CLDRVersion != "23" {
		panic("This binary was built with an incompatible version of golang.org/x/text. " +
			"See https://github.com/cockroachdb/cockroach/issues/63738 for details")
	}
}
