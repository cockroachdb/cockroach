// Copyright 2022 The Cockroach Authors.
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
	ExecList      []*PLpgSQLException
}

type PLpgSQLException struct {
	LineNo    int
	Condition *PLpgSQLCondition
	Action    []PLpgSQLStatement
}

type PLpgSQLCondition struct {
	SqlErrState int
	Name        string // TODO postgres says it's helpful for debugging.
	Next        *PLpgSQLCondition
}
