// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"

type declareHeader struct {
	label    string
	initVars []plpgsqltree.PLpgSQLVariable
}
