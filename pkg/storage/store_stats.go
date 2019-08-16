// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type ConstraintStatus struct {
	FailRangeCount int
}

type ZoneID struct {
	zoneID    uint32
	subZoneID uint32
}

type StoreStats struct {
	log.AmbientContext

	ConstraintStats map[ZoneID]map[string]ConstraintStatus
	LocalityStats   map[string]ConstraintStatus

	// Contains the list of the stores of the current node
	localStores *Stores
	// Constraints constructed from the locality information
	localityConstraints []config.Constraints
	// List of all stores known to exist on all nodes.
	allStoresCfg *StoreConfig
	// The store that is the current meta 1 leaseholder
	meta1LeaseHolder *Store
	// Latest zone config
	latestConfig *config.SystemConfig
	// Node liveness by store id
	nodeLiveStatus map[roachpb.NodeID]storagepb.NodeLivenessStatus
	// ZoneConfigs
	zoneConfigsByID map[uint32]*config.ZoneConfig

	totalRanges int
}

func NewStoreStats(
	ambient log.AmbientContext, localStores *Stores, allStoresCfg *StoreConfig,
) *StoreStats {
	return &StoreStats{
		AmbientContext:  ambient,
		localStores:     localStores,
		allStoresCfg:    allStoresCfg,
		ConstraintStats: map[ZoneID]map[string]ConstraintStatus{},
		LocalityStats:   map[string]ConstraintStatus{},
	}
}

// Start the periodic calls to Update()
func (stats *StoreStats) Start(stopper *stop.Stopper) {
	ctx := stats.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			stats.Update()
			time.Sleep(10 * time.Second)
		}
	})
}

// Update all constraint compliance stats.
func (stats *StoreStats) Update() {
	stats.meta1LeaseHolder = stats.Meta1LeaseHolderStore()
	if stats.meta1LeaseHolder == nil {
		return
	}

	stats.updateLatestConfig()
	if stats.latestConfig == nil {
		return
	}

	ctx := stats.AnnotateCtx(context.Background())
	//var err error
	//if stats.zoneConfigsByID, err = stats.ZoneConfigsByID(); err != nil {
	//	log.Errorf(ctx, "error during zone config load: %s", err)
	//	return
	//}

	stats.updateNodeLiveness()

	if err := stats.updateLocalityConstraints(); err != nil {
		log.Errorf(ctx, "Unable to update the locality constraints: %s", err)
	}

	if err := stats.processRanges(); err != nil {
		log.Errorf(ctx, "Unable to re-process all conformance compliance: %s", err)
		return
	} else {
		nl := stats.allStoresCfg.NodeLiveness.GetLivenessStatusMap()
		fmt.Printf("Liveness: %+v\n", nl)
		fmt.Printf("Total ranges: %d\n", stats.totalRanges)
		fmt.Printf("ConstraintStats:\n")
		stats.printConstraintStats(stats.ConstraintStats)
		fmt.Printf("LocalityStats:\n")
		//stats.printConstraintStats(stats.LocalityStats)
		fmt.Printf("Saving...")
		stats.save()
		fmt.Printf("Done\n")
	}
}

func (stats *StoreStats) printConstraintStats(zstats map[ZoneID]map[string]ConstraintStatus) {
	zoneIds := make([]ZoneID, 0, len(zstats))
	for z, _ := range zstats {
		zoneIds = append(zoneIds, z)
	}
	sort.Slice(zoneIds, func(i, j int) bool {
		return zoneIds[i].zoneID < zoneIds[j].zoneID ||
			(zoneIds[i].zoneID == zoneIds[j].zoneID && zoneIds[i].subZoneID < zoneIds[j].subZoneID)
	})
	for _, z := range zoneIds {
		cstats := zstats[z]
		fmt.Printf("  Zone: %v\n", z)
		keys := make([]string, 0, len(cstats))
		for k, _ := range cstats {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := cstats[k]
			fmt.Printf("    %d: %s\n", v.FailRangeCount, k)
		}
	}
}

// Return the node store that is the leaseholder of Meta1 range or
// nil if none of the node's stores are holding the Meta1 lease.
func (stats *StoreStats) Meta1LeaseHolderStore() *Store {
	meta1RangeID := roachpb.RangeID(1)
	var meta1LeaseHolder *Store
	stats.localStores.VisitStores(func(s *Store) error {
		if repl, _ := s.GetReplica(meta1RangeID); repl != nil {
			if repl.OwnsValidLease(s.Clock().Now()) {
				meta1LeaseHolder = s
				return nil
			}
		}
		return nil
	})
	return meta1LeaseHolder
}

func (stats *StoreStats) updateLatestConfig() {
	stats.latestConfig = stats.meta1LeaseHolder.Gossip().GetSystemConfig()
}

func (stats *StoreStats) db() *client.DB {
	if stats.meta1LeaseHolder != nil {
		return stats.meta1LeaseHolder.DB()
	}
	return nil
}

func (stats *StoreStats) updateLocalityConstraints() error {
	localityConstraintsByName := make(map[string]config.Constraints, 16)
	for _, sd := range stats.allStoresCfg.StorePool.GetStores() {
		c := config.Constraints{
			Constraints: make([]config.Constraint, 0),
		}

		if sd.desc == nil {
			return errors.AssertionFailedf(
				"unable to get store descriptor for store")
		}
		for _, t := range sd.desc.Node.Locality.Tiers {
			c.Constraints = append(c.Constraints, config.Constraint{Type: config.Constraint_REQUIRED, Key: t.Key, Value: t.Value})
			localityConstraintsByName[c.String()] = c
		}
	}
	stats.localityConstraints = make([]config.Constraints, 0, len(localityConstraintsByName))
	for _, c := range localityConstraintsByName {
		stats.localityConstraints = append(stats.localityConstraints, c)
	}
	return nil
}

func (stats *StoreStats) updateNodeLiveness() {
	stats.nodeLiveStatus = stats.allStoresCfg.NodeLiveness.GetLivenessStatusMap()
}

func (stats *StoreStats) isStoreLive(store roachpb.StoreDescriptor) bool {
	l, ok := stats.nodeLiveStatus[store.Node.NodeID]
	if !ok {
		return false
	} else {
		switch l {
		// Decommissioning nodes are considered live nodes.
		case storagepb.NodeLivenessStatus_LIVE, storagepb.NodeLivenessStatus_DECOMMISSIONING:
			return true
		default:
			return false
		}
	}
}

func (stats *StoreStats) getZoneConfig(
	id uint32,
) (uint32, *config.ZoneConfig, uint32, *config.ZoneConfig, error) {
	getKey := func(key roachpb.Key) (*roachpb.Value, error) {
		return stats.latestConfig.GetValue(key), nil
	}
	return sql.GetZoneConfig(id, getKey, false)
}

func (stats *StoreStats) GetZoneConfigAndID(k roachpb.RKey) (*config.ZoneConfig, ZoneID) {
	id, keySuffix := config.DecodeKeyIntoZoneIDAndSuffix(k)
	zid, zone, pid, placeholder, err := stats.getZoneConfig(id) //TODO(darin) cache result
	if zone == nil || err != nil {
		return stats.latestConfig.DefaultZoneConfig, ZoneID{}
	}
	if placeholder != nil {
		if subzone, pos := placeholder.GetSubzoneForKeySuffix(keySuffix); subzone != nil {
			return &subzone.Config, ZoneID{zoneID: pid, subZoneID: uint32(pos + 1)}
		}
	} else if subzone, pos := zone.GetSubzoneForKeySuffix(keySuffix); subzone != nil {
		return &subzone.Config, ZoneID{zoneID: id, subZoneID: uint32(pos + 1)}
	}
	return zone, ZoneID{zoneID: zid, subZoneID: 0}
}

func (stats *StoreStats) processRange(r *roachpb.RangeDescriptor) error {
	zoneCfg, zoneID := stats.GetZoneConfigAndID(r.StartKey)
	storeDescs := make([]roachpb.StoreDescriptor, len(r.Replicas().Voters()))
	for i, repl := range r.Replicas().Voters() {
		var ok bool
		storeDescs[i], ok = stats.allStoresCfg.StorePool.getStoreDescriptor(repl.StoreID)
		if !ok {
			return errors.AssertionFailedf(
				"unable to get store descriptor for store id %d", repl.StoreID)
		}
	}
	// Evaluate all zone constraints for the stores of the given range.
	for _, c := range zoneCfg.Constraints {
		stats.processConstraintForRangeStores(zoneID, &c, storeDescs)
	}
	for _, c := range stats.localityConstraints {
		stats.processLocalityForRangeStores(&c, storeDescs)
	}

	return nil
}

// Check a single constraint against a range with replicas in each of the stores
// given.
func (stats *StoreStats) processConstraintForRangeStores(
	zid ZoneID, c *config.Constraints, storeDescs []roachpb.StoreDescriptor,
) {
	passCount, totalCount := 0, 0
	for _, storeDesc := range storeDescs {
		totalCount++
		isStoreLive := stats.isStoreLive(storeDesc)

		constraintFailed := false
		for _, constraint := range c.Constraints {
			// StoreMatchesConstraint returns whether a store matches the given constraint.
			hasConstraint := config.StoreHasConstraint(storeDesc, constraint)
			// For required constraints - consider unavailable nodes as not matching.
			if constraint.Type == config.Constraint_REQUIRED && (!hasConstraint || !isStoreLive) {
				constraintFailed = true
				break
			}
			// For prohibited constraint - consider unavailable nodes as matching.
			if constraint.Type == config.Constraint_PROHIBITED && hasConstraint {
				constraintFailed = true
				break
			}
		}
		if !constraintFailed {
			passCount++
		}
	}
	if passCount < totalCount {
		if c.NumReplicas == 0 || int32(passCount) < c.NumReplicas {
			zoneConstraints := stats.ConstraintStats[zid]
			if zoneConstraints == nil {
				zoneConstraints = make(map[string]ConstraintStatus)
				stats.ConstraintStats[zid] = zoneConstraints
			}

			key := c.String()
			check := zoneConstraints[key]
			check.FailRangeCount++
			zoneConstraints[key] = check
		}
	}
}

// Check a single locality constraint against a range with replicas in each of the stores
// given.
func (stats *StoreStats) processLocalityForRangeStores(
	c *config.Constraints, storeDescs []roachpb.StoreDescriptor,
) {
	isCritical := false

	// Compute the required quorum adjusted for the number of stores that are not live.
	quorumCount := len(storeDescs)/2 + 1
	for _, storeDesc := range storeDescs {
		isStoreLive := stats.isStoreLive(storeDesc)
		if !isStoreLive {
			if quorumCount > 0 {
				quorumCount--
				if quorumCount == 0 {
					break
				}
			}
		}
	}

	passCount := 0
	for _, storeDesc := range storeDescs {
		storeHasConstraint := true
		for _, constraint := range c.Constraints {
			// For required constraints - consider unavailable nodes as not matching.
			if !config.StoreHasConstraint(storeDesc, constraint) {
				storeHasConstraint = false
				break
			}
		}

		if storeHasConstraint {
			passCount++
			if quorumCount <= passCount {
				isCritical = true
				break
			}
		}
	}
	if isCritical {
		key := c.String()
		check := stats.LocalityStats[key]
		check.FailRangeCount++
		stats.LocalityStats[key] = check
	}
}

func (stats *StoreStats) clearResults() {
	stats.ConstraintStats = map[ZoneID]map[string]ConstraintStatus{}
	stats.LocalityStats = map[string]ConstraintStatus{}
	stats.totalRanges = 0
}

// Generate constraint conformance info.
func (stats *StoreStats) processRanges() error {
	stats.clearResults()
	ctx := stats.AnnotateCtx(context.Background())
	db := stats.meta1LeaseHolder.DB()
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"unable to scan range descriptors")
		}

		var r roachpb.RangeDescriptor
		for _, row := range rows {
			if err := row.ValueProto(&r); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"%s: unable to unmarshal range descriptor", row.Key)
			}

			if err = stats.processRange(&r); err != nil {
				return err
			}
			stats.totalRanges++
		}
		return nil
	}); err != nil {
		stats.clearResults()
		return err
	}
	return nil
}

// Load the previously stored stats. This should only happen when a node becomes
// a meta1 leasoholder for the first time. It is possible that the tables
// are not created yet. In this case we would return an error so this can be
// retried later on.
func (stats *StoreStats) loadPrevious() error {
	ctx := stats.AnnotateCtx(context.Background())
	if err := stats.db().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (stats *StoreStats) save() error {
	ctx := stats.AnnotateCtx(context.Background())
	if err := stats.db().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		const stmt = "UPSERT INTO system.zone_violations (zone_id, subzone_id, " +
			"type, config, updated_at, violating_ranges, violating_mb) " +
			"VALUES ($1,$2,$3,$4,$5,$6,$7)"
		for c, zoneCons := range stats.ConstraintStats {
			for config, violation := range zoneCons {
				_, err := stats.allStoresCfg.SQLExecutor.Exec(
					ctx,
					"zone-violation-upsert",
					txn,
					stmt,
					c.zoneID,
					c.subZoneID,
					"constraint",
					config,
					time.Now(),
					violation.FailRangeCount,
					0,
				)
				if err != nil {
					return errors.NewAssertionErrorWithWrappedErrf(err,
						"%s: unable to update constraint", c)
				}
			}
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}
