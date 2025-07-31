// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssmemstorage

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
)

type stmtList []stmtKey

func (s stmtList) Len() int {
	return len(s)
}
func (s stmtList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s stmtList) Less(i, j int) bool {
	cmp := strings.Compare(s[i].stmtNoConstants, s[j].stmtNoConstants)
	if cmp == -1 {
		return true
	}

	if cmp == 1 {
		return false
	}
	return s[i].transactionFingerprintID < s[j].transactionFingerprintID
}

type txnList []appstatspb.TransactionFingerprintID

func (t txnList) Len() int {
	return len(t)
}

func (t txnList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t txnList) Less(i, j int) bool {
	return t[i] < t[j]
}
