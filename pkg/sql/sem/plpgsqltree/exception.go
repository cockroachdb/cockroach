// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plpgsqltree

type PLpgSQLExceptionBlock struct {
	SqlStateVarNo int
	SqlErrmNo     int
	ExecList      []*Exception
}

type Exception struct {
	LineNo    int
	Condition *Condition
	Action    []Statement
}

type Condition struct {
	SqlErrState int
	Name        string // TODO postgres says it's helpful for debugging.
	Next        *Condition
}
