// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (b *Builder) buildCreateTrigger(cf *tree.CreateTrigger, inScope *scope) (outScope *scope) {
	panic(unimplemented.NewWithIssue(126359, "CREATE TRIGGER"))
}
