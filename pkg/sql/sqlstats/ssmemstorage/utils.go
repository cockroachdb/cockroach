// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ssmemstorage

import "github.com/cockroachdb/cockroach/pkg/sql/sqlstats"

type stmtList []stmtKey

func (s stmtList) Len() int {
	return len(s)
}
func (s stmtList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s stmtList) Less(i, j int) bool {
	return s[i].anonymizedStmt < s[j].anonymizedStmt
}

type txnList []sqlstats.TransactionFingerprintID

func (t txnList) Len() int {
	return len(t)
}

func (t txnList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t txnList) Less(i, j int) bool {
	return t[i] < t[j]
}
