// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package upgrade captures the facilities needed to define and execute
// upgrades for a crdb cluster. These upgrades can be arbitrarily long
// running, are free to send out arbitrary requests cluster wide, change
// internal DB state, and much more. They're typically reserved for crdb
// internal operations and state. Each upgrade is idempotent in nature, is
// associated with a specific cluster version, and executed when the cluster
// version is made active on every node in the cluster.
//
// Examples of upgrades that apply would be upgrades to move all raft state
// from one storage engine to another, or purging all usage of the replicated
// truncated state in KV.
package upgrade

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
)

// JobDeps are upgrade-specific dependencies used by the upgrade job to run
// upgrades.
type JobDeps interface {

	// GetUpgrade returns the upgrade associated with the cluster version
	// if one exists.
	GetUpgrade(key roachpb.Version) (upgradebase.Upgrade, bool)

	// SystemDeps returns a handle to upgrade dependencies on a system tenant.
	SystemDeps() SystemDeps
}

// RestoreBehavior explains what the behavior of a given upgrade migration is
// during restoration of table, database or a cluster backups.
//
// For example, a migration that modifies every descriptor in the catalog e.g.
// to populate a new field, needs to also insert itself into RESTORE, so that
// when raw descriptors from a backup that could predate that migration, and
// thus not have that field populated, are injected into the catalog, they are
// migrated accordingly to uphold the invariant that all descriptors in the
// catalog are migrated. Such a migration would use `RestoreActionImplemented`
// and then a sentence specifying what is done during restore, for example:
// RestoreActionImplemented("new field is populated during allocateDescriptorRequests").
//
// If an upgrade migration only affects state that is not restored, such as the
// system tables that contain instance information and liveness, it can use a
// RestoreActionNotRequired string explaining this fact, e.g.
// RestoreActionNotRequired("liveness table is not restored").
type RestoreBehavior struct {
	msg string
}

// RestoreActionNotRequired is used to signal an upgrade migration only affects
// persisted state that is not written by restore. The string should mention
// why no action is required.
func RestoreActionNotRequired(msg string) RestoreBehavior {
	return RestoreBehavior{msg: msg}
}

// RestoreActionImplemented is used to signal an upgrade migration affects state
// that is written by restore and has implemented the requisite logic in restore
// to perform the equivalent upgrade on restored data as it is restored. The
// string should mention what is done during restore and where it is done.
func RestoreActionImplemented(msg string) RestoreBehavior {
	return RestoreBehavior{msg: msg}
}

type upgrade struct {
	description string
	// v is the version that this upgrade is associated with. The upgrade runs
	// when the cluster's version is incremented to v or, for permanent upgrades
	// (see below) when the cluster is bootstrapped at v or above.
	v roachpb.Version

	restore RestoreBehavior
}

// Version is part of the upgradebase.Upgrade interface.
func (m *upgrade) Version() roachpb.Version {
	return m.v
}

// Permanent is part of the upgradebase.Upgrade interface.
func (m *upgrade) Permanent() bool {
	return m.v.LessEq(clusterversion.VBootstrapMax.Version())
}

// Name is part of the upgradebase.Upgrade interface.
func (m *upgrade) Name() string {
	if m.Permanent() {
		return m.description
	}
	return fmt.Sprintf("Upgrade to %s: %q", m.v.String(), m.description)
}

func (m *upgrade) RestoreBehavior() string {
	return m.restore.msg
}
