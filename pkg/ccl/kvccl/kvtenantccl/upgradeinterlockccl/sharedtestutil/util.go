// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// package sharedtestutil contains util shared between upgradeinterlockccl and testgen.
// The goal of this util split is minimizing the dependencies of testgen.
package sharedtestutil

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
)

const (
	CurrentBinaryVersion = iota
	LaggingBinaryVersion
	NumConfigs
)

type TestVariant int

var Variants = map[TestVariant]string{
	CurrentBinaryVersion: "current_binary_version",
	LaggingBinaryVersion: "lagging_binary_version",
}

type TestConfig struct {
	Name          string
	ExpUpgradeErr [NumConfigs][]string // empty if expecting a nil error
	ExpStartupErr [NumConfigs]string   // empty if expecting a nil error
	PausePoint    upgradebase.PausePoint
}

var Tests = map[string]TestConfig{
	"pause_after_first_check_for_instances": {
		// In this case we won't see the new server in the first
		// transaction, and will instead see it when we try and commit the
		// first transaction.
		Name:       "pause_after_first_check_for_instances",
		PausePoint: upgradebase.AfterFirstCheckForInstances,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{"pq: upgrade failed due to active SQL servers with incompatible binary version",
				fmt.Sprintf("sql server 2 is running a binary version %s which is less than the attempted upgrade version", clusterversion.MinSupported.String())},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"",
		},
	},
	"pause_after_fence_RPC": {
		Name:       "pause_after_fence_RPC",
		PausePoint: upgradebase.AfterFenceRPC,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{"pq: upgrade failed due to active SQL servers with incompatible binary version"},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"",
		},
	},
	"pause_after_fence_write_to_settings_table": {
		Name:       "pause_after_fence_write_to_settings_table",
		PausePoint: upgradebase.AfterFenceWriteToSettingsTable,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{""},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"preventing SQL server from starting because its binary version is too low for the tenant active version",
		},
	},
	"pause_after_second_check_of_instances": {
		Name:       "pause_after_second_check_of_instances",
		PausePoint: upgradebase.AfterSecondCheckForInstances,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{""},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"preventing SQL server from starting because its binary version is too low for the tenant active version",
		},
	},
	"pause_after_migration": {
		Name:       "pause_after_migration",
		PausePoint: upgradebase.AfterMigration,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{""},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"preventing SQL server from starting because its binary version is too low for the tenant active version",
		},
	},
	"pause_after_version_bump_RPC": {
		Name:       "pause_after_version_bump_RPC",
		PausePoint: upgradebase.AfterVersionBumpRPC,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{""},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"preventing SQL server from starting because its binary version is too low for the tenant active version",
		},
	},
	"pause_after_write_to_settings_table": {
		Name:       "pause_after_write_to_settings_table",
		PausePoint: upgradebase.AfterVersionWriteToSettingsTable,
		ExpUpgradeErr: [NumConfigs][]string{
			{""},
			{""},
		},
		ExpStartupErr: [NumConfigs]string{
			"",
			"preventing SQL server from starting because its binary version is too low for the tenant active version",
		},
	},
}
