// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecop

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buildutil.VerifyNoImports(t,
		"github.com/cockroachdb/cockroach/pkg/sql/colexecop", true,
		[]string{
			"github.com/cockroachdb/cockroach/pkg/sql/colcontainer",
			"github.com/cockroachdb/cockroach/pkg/sql/colexec",
			"github.com/cockroachdb/cockroach/pkg/sql/colflow",
			"github.com/cockroachdb/cockroach/pkg/sql/rowexec",
			"github.com/cockroachdb/cockroach/pkg/sql/rowflow",
		}, nil,
	)
}
