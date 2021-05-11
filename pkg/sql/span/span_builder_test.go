// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package span

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestSpanBuilderDoesNotSplitSystemTableFamilySpans(t *testing.T) {
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	builder := MakeBuilder(&evalCtx, keys.SystemSQLCodec, systemschema.DescriptorTable,
		systemschema.DescriptorTable.GetPrimaryIndex())

	if res := builder.CanSplitSpanIntoFamilySpans(
		1, 1, false); res {
		t.Errorf("expected the system table to not be splittable")
	}
}
