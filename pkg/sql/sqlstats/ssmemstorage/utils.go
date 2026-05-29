// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssmemstorage

type stmtList []stmtKey

func (s stmtList) Len() int {
	return len(s)
}
func (s stmtList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s stmtList) Less(i, j int) bool {
	if s[i].fingerprintID != s[j].fingerprintID {
		return s[i].fingerprintID < s[j].fingerprintID
	}
	if s[i].transactionFingerprintID != s[j].transactionFingerprintID {
		return s[i].transactionFingerprintID < s[j].transactionFingerprintID
	}
	return s[i].aggregatedTs.Before(s[j].aggregatedTs)
}

type txnList []txnKey

func (t txnList) Len() int {
	return len(t)
}

func (t txnList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t txnList) Less(i, j int) bool {
	if t[i].transactionFingerprintID != t[j].transactionFingerprintID {
		return t[i].transactionFingerprintID < t[j].transactionFingerprintID
	}
	return t[i].aggregatedTs.Before(t[j].aggregatedTs)
}
