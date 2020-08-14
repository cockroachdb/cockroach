// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

// Force import to keep bazel happy.
import (
	_ "go/constant"

	_ "github.com/cockroachdb/cockroach/pkg/geo/geopb"
	_ "github.com/cockroachdb/cockroach/pkg/roachpb"
	_ "github.com/cockroachdb/cockroach/pkg/sql/lex"
	_ "github.com/cockroachdb/cockroach/pkg/sql/privilege"
	_ "github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	_ "github.com/cockroachdb/cockroach/pkg/sql/types"
	_ "github.com/lib/pq/oid"
)
