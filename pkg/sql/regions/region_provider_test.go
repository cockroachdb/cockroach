// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package regions_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestGetRegions exercises the logic of the regions.Provider.
// The test uses only the external API and mocks the dependencies appropriately
// to exercise the logic.
func TestGetRegions(t *testing.T) {
	type regionMap = map[string][]string
	mkResponse := func(rm regionMap) *serverpb.RegionsResponse {
		ret := &serverpb.RegionsResponse{
			Regions: make(map[string]*serverpb.RegionsResponse_Region),
		}
		for k, v := range rm {
			ret.Regions[k] = &serverpb.RegionsResponse_Region{
				Zones: v,
			}
		}
		return ret
	}
	newTestCollection := func(lm descs.LeaseManager) *descs.Collection {
		c := descs.MakeTestCollection(context.Background(), keys.SystemSQLCodec, lm)
		c.SetDescriptorSessionDataProvider(catsessiondata.DefaultDescriptorSessionDataProvider)
		return &c
	}
	clock := hlc.NewClockForTesting(timeutil.DefaultTimeSource{})
	db := kv.NewDB(
		log.MakeTestingAmbientCtxWithNewTracer(),
		&kv.MockTxnSenderFactory{},
		clock, nil,
	)
	txn := kv.NewTxnFromProto(
		context.Background(), db, 0,
		clock.NowAsClockTimestamp(), kv.RootTxn, &roachpb.Transaction{},
	)
	mkLeaseManagerWithRegions := func(readOnly map[string]struct{}, regions ...string) fakeLeaseManager {
		return fakeLeaseManager{
			descs: map[descpb.ID]lease.LeasedDescriptor{
				keys.SystemDatabaseID: fakeLeasedDescriptor{fakeSystemDatabase{
					isMultiRegion: len(regions) > 0,
				}},
				systemRegionsEnumID: fakeLeasedDescriptor{fakeRegionEnum{
					vals:     regions,
					readOnly: readOnly,
				}},
			},
		}
	}
	mkTenantRegions := func(regions ...string) *descs.Collection {
		return newTestCollection(mkLeaseManagerWithRegions(nil, regions...))
	}
	for _, testCase := range []struct {
		name            string
		forSystemTenant bool

		systemRegions    *serverpb.RegionsResponse
		systemRegionsErr error

		descs *descs.Collection

		exp      *serverpb.RegionsResponse
		expErrRE string
	}{
		{
			name:          "r2 missing from tenant, r3 missing from host",
			systemRegions: mkResponse(regionMap{"r1": {"a", "b"}, "r2": {"a"}}),
			descs:         mkTenantRegions("r1", "r3"),
			exp:           mkResponse(regionMap{"r1": {"a", "b"}, "r3": {}}),
		},
		{
			name:          "no tenant regions",
			systemRegions: mkResponse(regionMap{"r1": {"a", "b"}, "r2": {"a"}}),
			descs:         mkTenantRegions(),
			exp:           mkResponse(regionMap{"r1": {"a", "b"}, "r2": {"a"}}),
		},
		{
			name:            "system tenant ignore systemdb",
			forSystemTenant: true,
			systemRegions:   mkResponse(regionMap{"r1": {"a"}, "r2": {"b"}}),
			descs:           mkTenantRegions("r3"),
			exp:             mkResponse(regionMap{"r1": {"a"}, "r2": {"b"}}),
		},
		{
			name:             "system errors propagate",
			systemRegionsErr: errors.New("boom system"),
			expErrRE:         "boom system",
		},
		{
			name:          "no system database",
			systemRegions: mkResponse(regionMap{"r1": {"a"}, "r2": {"b"}}),
			descs: newTestCollection(fakeLeaseManager{
				errs: map[descpb.ID]error{
					keys.SystemDatabaseID: errors.New("boom"),
				},
			}),
			expErrRE: "failed to resolve system database for regions: boom",
		},
		{
			name:          "region enum not found",
			systemRegions: mkResponse(regionMap{"r1": {"a"}, "r2": {"b"}}),
			descs: func() *descs.Collection {
				lm := mkLeaseManagerWithRegions(nil /* readOnly */, "r1")
				lm.errs = map[descpb.ID]error{systemRegionsEnumID: errors.New("boom")}
				return newTestCollection(lm)
			}(),
			expErrRE: `failed to resolve multi-region enum for the database \(10\): boom`,
		},
		{
			name:          "region enum type not an enum",
			systemRegions: mkResponse(regionMap{"r1": {"a"}, "r2": {"b"}}),
			descs: func() *descs.Collection {
				lm := mkLeaseManagerWithRegions(nil /* readOnly */, "r1")
				lm.descs[systemRegionsEnumID] = fakeLeasedDescriptor{
					fakeRegionEnum{isNotEnum: true},
				}
				return newTestCollection(lm)
			}(),
			expErrRE: `multi-region type fake_crdb_region \(10\) for the database is not an enum`,
		},
		{
			name:          "read-only members ignored",
			systemRegions: mkResponse(regionMap{"r1": {"a"}, "r2": {"b"}, "r3": {"c"}}),
			descs: func() *descs.Collection {
				return newTestCollection(mkLeaseManagerWithRegions(
					map[string]struct{}{"r1": {}, "r3": {}},
					"r1", "r2", "r3",
				))
			}(),
			exp: mkResponse(regionMap{"r2": {"b"}}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var codec keys.SQLCodec
			if testCase.forSystemTenant {
				codec = keys.SystemSQLCodec
			} else {
				codec = keys.MakeSQLCodec(roachpb.MustMakeTenantID(10))
			}
			p := regions.NewProvider(
				codec,
				connectorFunc(func() (*serverpb.RegionsResponse, error) {
					return testCase.systemRegions, testCase.systemRegionsErr
				}),
				txn,
				testCase.descs,
			)
			got, err := p.GetRegions(context.Background())

			if testCase.expErrRE != "" {
				require.Regexp(t, testCase.expErrRE, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.exp, got)
			}
		})
	}
}

type fakeLeaseManager struct {
	errs  map[descpb.ID]error
	descs map[descpb.ID]lease.LeasedDescriptor
}

func (f fakeLeaseManager) AcquireByName(
	ctx context.Context,
	timestamp hlc.Timestamp,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (lease.LeasedDescriptor, error) {
	panic("unimplemented")
}

func (f fakeLeaseManager) Acquire(
	ctx context.Context, timestamp hlc.Timestamp, id descpb.ID,
) (lease.LeasedDescriptor, error) {
	if err, ok := f.errs[id]; ok {
		return nil, err
	}
	ld := f.descs[id]
	return ld, nil
}

func (f fakeLeaseManager) IncGaugeAfterLeaseDuration(gauge lease.AfterLeaseDurationGauge) func() {
	return func() {}
}

func (f fakeLeaseManager) GetSafeReplicationTS() hlc.Timestamp {
	return hlc.Timestamp{}
}

func (f fakeLeaseManager) GetLeaseGeneration() int64 {
	return 0
}

var _ descs.LeaseManager = (*fakeLeaseManager)(nil)

type fakeSystemDatabase struct {
	catalog.DatabaseDescriptor
	isMultiRegion bool
}

func (d fakeSystemDatabase) IsMultiRegion() bool {
	return d.isMultiRegion
}

func (d fakeSystemDatabase) MultiRegionEnumID() (descpb.ID, error) {
	if d.isMultiRegion {
		return systemRegionsEnumID, nil
	}
	return 0, errors.New("boom")
}

func (f fakeSystemDatabase) SkipNamespace() bool        { return true }
func (d fakeSystemDatabase) GetID() descpb.ID           { return keys.SystemDatabaseID }
func (d fakeSystemDatabase) Dropped() bool              { return false }
func (d fakeSystemDatabase) IsUncommittedVersion() bool { return false }
func (d fakeSystemDatabase) Adding() bool               { return false }
func (d fakeSystemDatabase) ForEachUDTDependentForHydration(func(t *types.T) error) error {
	return nil
}
func (d fakeSystemDatabase) MaybeRequiresTypeHydration() bool { return false }

type fakeLeasedDescriptor struct {
	catalog.Descriptor
}

func (f fakeLeasedDescriptor) Underlying() catalog.Descriptor {
	return f.Descriptor
}
func (f fakeLeasedDescriptor) Expiration(_ context.Context) hlc.Timestamp {
	return hlc.MaxTimestamp
}
func (f fakeLeasedDescriptor) Release(ctx context.Context) {
}

type fakeRegionEnum struct {
	catalog.NonAliasTypeDescriptor
	isNotEnum bool
	vals      []string
	readOnly  map[string]struct{}
}

func (f fakeRegionEnum) NumEnumMembers() int {
	return len(f.vals)
}
func (f fakeRegionEnum) GetMemberPhysicalRepresentation(enumMemberOrdinal int) []byte {
	return nil
}
func (f fakeRegionEnum) GetMemberLogicalRepresentation(enumMemberOrdinal int) string {
	return f.vals[enumMemberOrdinal]
}
func (f fakeRegionEnum) IsMemberReadOnly(enumMemberOrdinal int) bool {
	_, ok := f.readOnly[f.vals[enumMemberOrdinal]]
	return ok
}
func (d fakeRegionEnum) GetID() descpb.ID           { return systemRegionsEnumID }
func (d fakeRegionEnum) GetName() string            { return "fake_crdb_region" }
func (f fakeRegionEnum) SkipNamespace() bool        { return true }
func (d fakeRegionEnum) Dropped() bool              { return false }
func (d fakeRegionEnum) IsUncommittedVersion() bool { return false }
func (d fakeRegionEnum) Adding() bool               { return false }
func (d fakeRegionEnum) ForEachUDTDependentForHydration(func(t *types.T) error) error {
	return nil
}
func (d fakeRegionEnum) MaybeRequiresTypeHydration() bool { return false }
func (d fakeRegionEnum) AsEnumTypeDescriptor() catalog.EnumTypeDescriptor {
	if d.isNotEnum {
		return nil
	}
	return d
}

type connectorFunc func() (*serverpb.RegionsResponse, error)

func (i connectorFunc) Regions(
	ctx context.Context, request *serverpb.RegionsRequest,
) (*serverpb.RegionsResponse, error) {
	return i()
}

var _ regions.Connector = (connectorFunc)(nil)

var _ catalog.EnumTypeDescriptor = (*fakeRegionEnum)(nil)

const systemRegionsEnumID = 10
