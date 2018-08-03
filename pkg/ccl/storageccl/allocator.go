// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// PreferredLeaseholders calculates which of the existing replicas are most
// preferred to be leaseholders for a range governed by the provided zone
// config. Only considers the current leaseholder a valid option if
// checkTransferLeaseSource is true. Returns the preferred leaseholder(s) and
// whether the current leaseholder is one of them (i.e. whether a transfer is
// needed).
//
// TODO(a-robinson): Should this check the enterprise license or is only
// checking it when setting leaseholder preferences sufficient? If it's enough,
// we should just do that.
func PreferredLeaseholders(
	zone config.ZoneConfig,
	existing []roachpb.ReplicaDescriptor,
	leaseStoreID roachpb.StoreID,
	checkTransferLeaseSource bool,
	getStoreDescFn func(roachpb.StoreID) (roachpb.StoreDescriptor, bool),
) ([]roachpb.ReplicaDescriptor, bool) {
	// Don't consider the current leaseholder if asked not to.
	var preferred []roachpb.ReplicaDescriptor
	if !checkTransferLeaseSource {
		var candidates []roachpb.ReplicaDescriptor
		for _, repl := range existing {
			if repl.StoreID != leaseStoreID {
				candidates = append(candidates, repl)
			}
		}
		preferred = preferredLeaseholdersImpl(zone, candidates, getStoreDescFn)
	} else {
		preferred = preferredLeaseholdersImpl(zone, existing, getStoreDescFn)
	}
	if len(preferred) == 0 {
		return existing, !checkTransferLeaseSource
	}
	if !storage.StoreHasReplica(leaseStoreID, preferred) {
		return preferred, true
	}
	return preferred, !checkTransferLeaseSource
}

func preferredLeaseholdersImpl(
	zone config.ZoneConfig,
	existing []roachpb.ReplicaDescriptor,
	getStoreDescFn func(roachpb.StoreID) (roachpb.StoreDescriptor, bool),
) []roachpb.ReplicaDescriptor {
	// Go one preference at a time. As soon as we've found replicas that match a
	// preference, we don't need to look at the later preferences, because
	// they're meant to be ordered by priority.
	for _, preference := range zone.LeasePreferences {
		var preferred []roachpb.ReplicaDescriptor
		for _, repl := range existing {
			// TODO(a-robinson): Do all these lookups at once, up front? We could
			// easily be passing a slice of StoreDescriptors around all the Allocator
			// functions instead of ReplicaDescriptors.
			storeDesc, ok := getStoreDescFn(repl.StoreID)
			if !ok {
				continue
			}
			if storage.SubConstraintsCheck(storeDesc, preference.Constraints) {
				preferred = append(preferred, repl)
			}
		}
		if len(preferred) > 0 {
			return preferred
		}
	}
	return nil
}

func init() {
	storage.PreferredLeaseholders = PreferredLeaseholders
}
