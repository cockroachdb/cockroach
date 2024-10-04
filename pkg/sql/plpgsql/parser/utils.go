// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"

type declareHeader struct {
	label    string
	initVars []plpgsqltree.PLpgSQLVariable
}
