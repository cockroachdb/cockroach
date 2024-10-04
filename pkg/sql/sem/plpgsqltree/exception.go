// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
