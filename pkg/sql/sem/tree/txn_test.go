// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestToKVIsoLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in  tree.IsolationLevel
		out isolation.Level
	}{
		{tree.ReadUncommittedIsolation, isolation.ReadCommitted},
		{tree.ReadCommittedIsolation, isolation.ReadCommitted},
		{tree.RepeatableReadIsolation, isolation.Snapshot},
		{tree.SnapshotIsolation, isolation.Snapshot},
		{tree.SerializableIsolation, isolation.Serializable},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.out, tc.in.ToKVIsoLevel())
		})
	}
}

func TestFromKVIsoLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		in  isolation.Level
		out tree.IsolationLevel
	}{
		{isolation.ReadCommitted, tree.ReadCommittedIsolation},
		{isolation.Snapshot, tree.RepeatableReadIsolation},
		{isolation.Serializable, tree.SerializableIsolation},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.out, tree.FromKVIsoLevel(tc.in))
		})
	}
}

func TestUpgradeToEnabledLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const RU = tree.ReadUncommittedIsolation
	const RC = tree.ReadCommittedIsolation
	const RR = tree.RepeatableReadIsolation
	const SI = tree.SnapshotIsolation
	const SER = tree.SerializableIsolation

	testCases := []struct {
		in                      tree.IsolationLevel
		allowReadCommitted      bool
		allowRepeatableRead     bool
		hasLicense              bool
		expOut                  tree.IsolationLevel
		expUpgraded             bool
		expUpgradedDueToLicense bool
	}{
		{RU, false, false, false, SER, true, false},
		{RU, true, false, false, SER, true, true},
		{RU, false, true, false, SER, true, true},
		{RU, true, true, false, SER, true, true},
		{RU, false, false, true, SER, true, false},
		{RU, true, false, true, RC, true, false},
		{RU, false, true, true, RR, true, false},
		{RU, true, true, true, RC, true, false},
		{RC, false, false, false, SER, true, false},
		{RC, true, false, false, SER, true, true},
		{RC, false, true, false, SER, true, true},
		{RC, true, true, false, SER, true, true},
		{RC, false, false, true, SER, true, false},
		{RC, true, false, true, RC, false, false},
		{RC, false, true, true, RR, true, false},
		{RC, true, true, true, RC, false, false},
		{RR, false, false, false, SER, true, false},
		{RR, true, false, false, SER, true, false},
		{RR, false, true, false, SER, true, true},
		{RR, true, true, false, SER, true, true},
		{RR, false, false, true, SER, true, false},
		{RR, true, false, true, SER, true, false},
		{RR, false, true, true, RR, false, false},
		{RR, true, true, true, RR, false, false},
		{SI, false, false, false, SER, true, false},
		{SI, true, false, false, SER, true, false},
		{SI, false, true, false, SER, true, true},
		{SI, true, true, false, SER, true, true},
		{SI, false, false, true, SER, true, false},
		{SI, true, false, true, SER, true, false},
		{SI, false, true, true, RR, false, false},
		{SI, true, true, true, RR, false, false},
		{SER, false, false, false, SER, false, false},
		{SER, true, false, false, SER, false, false},
		{SER, false, true, false, SER, false, false},
		{SER, true, true, false, SER, false, false},
		{SER, false, false, true, SER, false, false},
		{SER, true, false, true, SER, false, false},
		{SER, false, true, true, SER, false, false},
		{SER, true, true, true, SER, false, false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			res, upgraded, upgradedDueToLicense := tc.in.UpgradeToEnabledLevel(
				tc.allowReadCommitted, tc.allowRepeatableRead, tc.hasLicense)
			require.Equal(t, tc.expOut, res)
			require.Equal(t, tc.expUpgraded, upgraded)
			require.Equal(t, tc.expUpgradedDueToLicense, upgradedDueToLicense)
		})
	}
}
