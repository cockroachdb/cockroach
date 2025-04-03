// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regionliveness

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var RegionLivenessEnabled = settings.RegisterBoolSetting(settings.ApplicationLevel,
	"sql.region_liveness.enabled",
	"enables region liveness for system databases",
	false, /* disabled */
	settings.WithVisibility(settings.Reserved))

var RegionLivenessProbeTimeout = settings.RegisterDurationSetting(settings.ApplicationLevel,
	"sql.region_liveness.probe.timeout",
	"set the probing timeout for region liveness, which will be the maximum "+
		"time a query to a region can take before it starts getting quarantined",
	15*time.Second, /* 15 seconds */
	settings.WithVisibility(settings.Reserved))

// LiveRegions are regions which are currently still avaialble,
// and not quarantined due to expiration.
type LiveRegions map[string]struct{}

// ForEach does ordered iteration over the regions.
func (l LiveRegions) ForEach(fn func(region string) error) error {
	regions := make([]string, 0, len(l))
	for r := range l {
		regions = append(regions, r)
	}
	sort.Slice(regions, func(a, b int) bool {
		return regions[a] < regions[b]
	})
	for _, r := range regions {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// UnavailableAtPhysicalRegions is map of regions (in physical representation).
type UnavailableAtPhysicalRegions map[string]*tree.DTimestamp

// ContainsPhysicalRepresentation contains the physical representation of a region
// as stored inside the KV.
func (u UnavailableAtPhysicalRegions) ContainsPhysicalRepresentation(
	physicalRepresentationForRegion string,
) bool {
	_, ok := u[physicalRepresentationForRegion]
	return ok
}

// Prober used to determine the set of regions which are still alive.
type Prober interface {
	// ProbeLiveness can be used after a timeout to label a regions as unavailable.
	ProbeLiveness(ctx context.Context, region string) error
	// ProbeLivenessWithPhysicalRegion can be used after a timeout to label a regions as unavailable,
	// with this version only the physical representation is required.
	ProbeLivenessWithPhysicalRegion(ctx context.Context, regionBytes []byte) error
	// QueryLiveness can be used to get the list of regions which are currently
	// accessible.
	QueryLiveness(ctx context.Context, txn *kv.Txn) (LiveRegions, error)
	// QueryUnavailablePhysicalRegions returns a list of regions that are unavailable at
	// right now as physical representations.
	QueryUnavailablePhysicalRegions(ctx context.Context, txn *kv.Txn, filterAvailable bool) (UnavailableAtPhysicalRegions, error)
	// GetProbeTimeout gets maximum timeout waiting on a table before issuing
	// liveness queries.
	GetProbeTimeout() (bool, time.Duration)
	// MarkPhysicalRegionAsAvailable deletes the unavailable_at timestamp for a region.
	MarkPhysicalRegionAsAvailable(ctx context.Context, txn *kv.Txn, region string, timestamp *tree.DTimestamp) error
}

type CachedDatabaseRegions interface {
	IsMultiRegion() bool
	GetRegionEnumTypeDesc() catalog.RegionEnumTypeDescriptor
	GetSystemDatabaseVersion() *roachpb.Version
}

type livenessProber struct {
	db              *kv.DB
	codec           keys.SQLCodec
	kvWriter        bootstrap.KVWriter
	cachedDBRegions CachedDatabaseRegions
	settings        *clustersettings.Settings
}

var testingBeforeProbeLivenessHook func()
var testingUnavailableAtTTLOverride time.Duration

func TestingSetBeforeProbeLivenessHook(hook func()) func() {
	testingBeforeProbeLivenessHook = hook
	return func() {
		testingBeforeProbeLivenessHook = nil
	}
}

func TestingSetUnavailableAtTTLOverride(duration time.Duration) func() {
	oldValue := testingUnavailableAtTTLOverride
	testingUnavailableAtTTLOverride = duration
	return func() {
		testingUnavailableAtTTLOverride = oldValue
	}
}

// NewLivenessProber creates a new region liveness prober.
func NewLivenessProber(
	db *kv.DB,
	codec keys.SQLCodec,
	cachedDBRegions CachedDatabaseRegions,
	settings *clustersettings.Settings,
) Prober {
	return &livenessProber{
		db:              db,
		codec:           codec,
		kvWriter:        bootstrap.MakeKVWriter(codec, systemschema.RegionLivenessTable),
		cachedDBRegions: cachedDBRegions,
		settings:        settings,
	}
}

// ProbeLiveness implements Prober.
func (l *livenessProber) ProbeLiveness(ctx context.Context, region string) error {
	// If region liveness is disabled then nothing to do.
	regionLivenessEnabled, _ := l.GetProbeTimeout()
	if !regionLivenessEnabled {
		return nil
	}
	// Resolve the physical value for this region.
	regionEnum := l.cachedDBRegions.GetRegionEnumTypeDesc()
	foundIdx := -1
	for i := 0; i < regionEnum.NumEnumMembers(); i++ {
		if regionEnum.GetMemberLogicalRepresentation(i) == region {
			foundIdx = i
			break
		}
	}
	if foundIdx == -1 {
		return errors.AssertionFailedf("unable to find region %s in region enum", region)
	}
	return l.ProbeLivenessWithPhysicalRegion(ctx, regionEnum.GetMemberPhysicalRepresentation(foundIdx))
}

func (l *livenessProber) ProbeLivenessWithPhysicalRegion(
	ctx context.Context, regionBytes []byte,
) error {
	// If region liveness is disabled then nothing to do.
	regionLivenessEnabled, tableTimeout := l.GetProbeTimeout()
	if !regionLivenessEnabled {
		return nil
	}
	regionEnumValue := tree.NewDBytes(tree.DBytes(regionBytes))
	// Probe from the SQL instances table to confirm if the region
	// is live.
	err := timeutil.RunWithTimeout(ctx, "probe-liveness", tableTimeout,
		func(ctx context.Context) error {
			return l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if testingBeforeProbeLivenessHook != nil {
					testingBeforeProbeLivenessHook()
				}
				instancesTable := systemschema.SQLInstancesTable()
				indexPrefix := l.codec.IndexPrefix(uint32(instancesTable.GetID()), uint32(instancesTable.GetPrimaryIndexID()))
				regionPrefixBytes, err := keyside.Encode(indexPrefix, regionEnumValue, encoding.Ascending)
				if err != nil {
					return err
				}
				regionPrefix := roachpb.Key(regionPrefixBytes)
				regionPrefixEnd := regionPrefix.PrefixEnd()
				_, err = txn.Scan(ctx, regionPrefix, regionPrefixEnd, 0)
				return err
			})
		})

	// Region is alive or we hit some other error.
	if err == nil || !IsQueryTimeoutErr(err) {
		log.VEventf(ctx, 2, "region probe completed with error: %v", err)
		return err
	}

	// Region has gone down, set the unavailable_at time on it
	return l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		defaultTTL := slbase.DefaultTTL.Get(&l.settings.SV)
		if testingUnavailableAtTTLOverride != 0 {
			defaultTTL = testingUnavailableAtTTLOverride
		}
		defaultHeartbeat := slbase.DefaultHeartBeat.Get(&l.settings.SV)
		// Get the read timestamp and pick a commit deadline.
		commitDeadline := txn.ReadTimestamp().AddDuration(defaultHeartbeat)
		txnTS := commitDeadline.AddDuration(defaultTTL)
		if err := txn.UpdateDeadline(ctx, commitDeadline); err != nil {
			return err
		}
		ba := txn.NewBatch()
		// Insert a new unavailable_at time.
		err := l.kvWriter.Insert(ctx, ba, false, regionEnumValue, tree.MustMakeDTimestamp(txnTS.GoTime(), time.Microsecond))
		if err != nil {
			return err
		}
		log.VEventf(ctx, 2, "marking region %q as dead at time: %s", string(regionBytes), txnTS.String())
		if err := txn.Run(ctx, ba); err != nil {
			// Conditional put failing is fine, since it means someone else
			// has marked the region as dead.
			if errors.HasType(err, &kvpb.ConditionFailedError{}) {
				log.VEventf(ctx, 2, "ignoring condition failed for region: %q", string(regionBytes))
				return nil
			}
			return err
		}
		return nil
	})
}

// QueryLiveness implements Prober.
func (l *livenessProber) QueryLiveness(ctx context.Context, txn *kv.Txn) (LiveRegions, error) {
	// Database is not multi-region so report a single region.
	if l.cachedDBRegions == nil ||
		!l.cachedDBRegions.IsMultiRegion() {
		return nil, nil
	}
	regionStatus := make(LiveRegions)
	if err := l.cachedDBRegions.GetRegionEnumTypeDesc().ForEachPublicRegion(func(regionName catpb.RegionName) error {
		regionStatus[string(regionName)] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	// If region liveness is disabled, return nil.
	if !RegionLivenessEnabled.Get(&l.settings.SV) {
		return regionStatus, nil
	}
	// Detect and down regions and remove them.
	unavailableAtRegions, err := l.QueryUnavailablePhysicalRegions(ctx, txn, true)
	if err != nil {
		return nil, err
	}
	regionEnum := l.cachedDBRegions.GetRegionEnumTypeDesc()
	for i := 0; i < regionEnum.NumEnumMembers(); i++ {
		if unavailableAtRegions.ContainsPhysicalRepresentation(string(regionEnum.GetMemberPhysicalRepresentation(i))) {
			delete(regionStatus, regionEnum.GetMemberLogicalRepresentation(i))
		}
	}
	return regionStatus, nil
}

// QueryUnavailablePhysicalRegions implements Prober.
func (l *livenessProber) QueryUnavailablePhysicalRegions(
	ctx context.Context, txn *kv.Txn, filterAvailable bool,
) (UnavailableAtPhysicalRegions, error) {
	// Scan the entire region liveness table.
	regionLivenessIndex := l.codec.IndexPrefix(uint32(systemschema.RegionLivenessTable.GetID()), uint32(systemschema.RegionLivenessTable.GetPrimaryIndexID()))
	keyValues, err := txn.Scan(ctx, regionLivenessIndex, regionLivenessIndex.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	// Detect any down regions and remove them.
	unavailableAtRegions := make(UnavailableAtPhysicalRegions)
	datumAlloc := &tree.DatumAlloc{}
	for _, keyValue := range keyValues {
		tuple, err := keyValue.Value.GetTuple()
		if err != nil {
			return nil, err
		}
		enumDatum, _, err := keyside.Decode(datumAlloc, types.Bytes, keyValue.Key[len(regionLivenessIndex):], encoding.Ascending)
		if err != nil {
			return nil, err
		}
		enumBytes := enumDatum.(*tree.DBytes)
		ts, _, err := valueside.Decode(datumAlloc, types.Timestamp, tuple)
		if err != nil {
			return nil, err
		}
		unavailableAt := ts.(*tree.DTimestamp)
		// Region is now officially unavailable, so lets remove
		// it.
		if txn.ReadTimestamp().GoTime().After(unavailableAt.Time) ||
			!filterAvailable {
			unavailableAtRegions[string(*enumBytes)] = unavailableAt
		}
	}
	return unavailableAtRegions, nil
}

// MarkPhysicalRegionAsAvailable implements Prober.
func (l *livenessProber) MarkPhysicalRegionAsAvailable(
	ctx context.Context, txn *kv.Txn, region string, timestamp *tree.DTimestamp,
) error {
	ba := txn.NewBatch()
	// Encode a key for this region, and delete it.
	err := l.kvWriter.Delete(ctx, ba, false /*kvTrace*/, tree.NewDBytes(tree.DBytes(region)), timestamp)
	if err != nil {
		return err
	}
	return txn.Run(ctx, ba)
}

// GetProbeTimeout gets maximum timeout waiting on a table before issuing
// liveness queries.
func (l *livenessProber) GetProbeTimeout() (bool, time.Duration) {
	return RegionLivenessEnabled.Get(&l.settings.SV),
		RegionLivenessProbeTimeout.Get(&l.settings.SV)
}

// IsQueryTimeoutErr determines if a query timeout error was hit, specifically
// when checking for region liveness.
func IsQueryTimeoutErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.QueryCanceled ||
		errors.HasType(err, (*timeutil.TimeoutError)(nil)) ||
		errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil)) ||
		pgerror.GetPGCode(err) == pgcode.RangeUnavailable ||
		errors.Is(err, context.DeadlineExceeded)
}

// IsMissingRegionEnumErr determines if a query hit an error because of a missing
// because of the region enum.
func IsMissingRegionEnumErr(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.InvalidTextRepresentation ||
		errors.Is(err, types.EnumValueNotYetPublicError)
}
