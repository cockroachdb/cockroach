// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		allowRC                 bool
		allowRR                 bool
		license                 bool
		expOut                  tree.IsolationLevel
		expUpgraded             bool
		expUpgradedDueToLicense bool
	}{
		{in: RU, allowRC: false, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RU, allowRC: true, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RU, allowRC: false, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RU, allowRC: true, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RU, allowRC: false, allowRR: false, license: true, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RU, allowRC: true, allowRR: false, license: true, expOut: RC, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RU, allowRC: false, allowRR: true, license: true, expOut: RR, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RU, allowRC: true, allowRR: true, license: true, expOut: RC, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RC, allowRC: false, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RC, allowRC: true, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RC, allowRC: false, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RC, allowRC: true, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RC, allowRC: false, allowRR: false, license: true, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RC, allowRC: true, allowRR: false, license: true, expOut: RC, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: RC, allowRC: false, allowRR: true, license: true, expOut: RR, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RC, allowRC: true, allowRR: true, license: true, expOut: RC, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: RR, allowRC: false, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RR, allowRC: true, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RR, allowRC: false, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RR, allowRC: true, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: RR, allowRC: false, allowRR: false, license: true, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RR, allowRC: true, allowRR: false, license: true, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: RR, allowRC: false, allowRR: true, license: true, expOut: RR, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: RR, allowRC: true, allowRR: true, license: true, expOut: RR, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SI, allowRC: false, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: SI, allowRC: true, allowRR: false, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: SI, allowRC: false, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: SI, allowRC: true, allowRR: true, license: false, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: true},
		{in: SI, allowRC: false, allowRR: false, license: true, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: SI, allowRC: true, allowRR: false, license: true, expOut: SER, expUpgraded: true, expUpgradedDueToLicense: false},
		{in: SI, allowRC: false, allowRR: true, license: true, expOut: RR, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SI, allowRC: true, allowRR: true, license: true, expOut: RR, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: false, allowRR: false, license: false, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: true, allowRR: false, license: false, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: false, allowRR: true, license: false, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: true, allowRR: true, license: false, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: false, allowRR: false, license: true, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: true, allowRR: false, license: true, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: false, allowRR: true, license: true, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
		{in: SER, allowRC: true, allowRR: true, license: true, expOut: SER, expUpgraded: false, expUpgradedDueToLicense: false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			res, upgraded, upgradedDueToLicense := tc.in.UpgradeToEnabledLevel(
				tc.allowRC, tc.allowRR, tc.license)
			require.Equal(t, tc.expOut, res)
			require.Equal(t, tc.expUpgraded, upgraded)
			require.Equal(t, tc.expUpgradedDueToLicense, upgradedDueToLicense)
		})
	}
}
