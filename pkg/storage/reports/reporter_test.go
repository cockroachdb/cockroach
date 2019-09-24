// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/keysutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

type zone struct {
	// 0 means unset.
	replicas int32
	// "" means unset. "[]" means empty.
	constraints string
}

func (z zone) toZoneConfig() config.ZoneConfig {
	cfg := config.NewZoneConfig()
	if z.replicas != 0 {
		cfg.NumReplicas = proto.Int32(z.replicas)
	}
	if z.constraints != "" {
		var constraintsList config.ConstraintsList
		if err := yaml.UnmarshalStrict([]byte(z.constraints), &constraintsList); err != nil {
			panic(err)
		}
		cfg.Constraints = constraintsList.Constraints
		cfg.InheritedConstraints = false
	}
	return *cfg
}

// Note that we support multi-column partitions but we don't support
// sub-partitions.
type partition struct {
	name string
	// start defines the partition's start key as a sequence of int column values.
	start []int
	// end defines the partition's end key as a sequence of int column values.
	end  []int
	zone *zone
}

func (p partition) toPartitionDescriptor() sqlbase.PartitioningDescriptor_Range {
	var startKey roachpb.Key
	for _, val := range p.start {
		startKey = encoding.EncodeIntValue(startKey, encoding.NoColumnID, int64(val))
	}
	var endKey roachpb.Key
	for _, val := range p.end {
		endKey = encoding.EncodeIntValue(endKey, encoding.NoColumnID, int64(val))
	}
	return sqlbase.PartitioningDescriptor_Range{
		Name:          p.name,
		FromInclusive: startKey,
		ToExclusive:   endKey,
	}
}

type partitioning []partition

func (p partitioning) numCols() int {
	if len(p) == 0 {
		return 0
	}
	return len(p[0].start)
}

func (p partitioning) validate() error {
	// Validate that all partitions (if any) have the same number of columns.
	if len(p) == 0 {
		return nil
	}
	numCols := len(p[0].start)
	for _, pp := range p {
		if len(pp.start) != numCols {
			return errors.Errorf("partition start doesn't have expected number of columns: %v", pp.start)
		}
		if len(pp.end) != numCols {
			return errors.Errorf("partition end doesn't have expected number of columns: %v", pp.end)
		}
	}
	return nil
}

type index struct {
	name       string
	zone       *zone
	partitions partitioning
}

func (idx index) toIndexDescriptor(id int) sqlbase.IndexDescriptor {
	var idxDesc sqlbase.IndexDescriptor
	idxDesc.ID = sqlbase.IndexID(id)
	idxDesc.Name = idx.name
	if len(idx.partitions) > 0 {
		neededCols := idx.partitions.numCols()
		for i := 0; i < neededCols; i++ {
			idxDesc.ColumnIDs = append(idxDesc.ColumnIDs, sqlbase.ColumnID(i))
			idxDesc.ColumnNames = append(idxDesc.ColumnNames, fmt.Sprintf("col%d", i))
			idxDesc.ColumnDirections = append(idxDesc.ColumnDirections, sqlbase.IndexDescriptor_ASC)
		}
		idxDesc.Partitioning.NumColumns = uint32(len(idx.partitions[0].start))
		for _, p := range idx.partitions {
			idxDesc.Partitioning.Range = append(idxDesc.Partitioning.Range, p.toPartitionDescriptor())
		}
	}
	return idxDesc
}

type table struct {
	name       string
	zone       *zone
	indexes    []index
	partitions partitioning
}

func (t table) validate() error {
	return t.partitions.validate()
}

type database struct {
	name   string
	tables []table
	zone   *zone
}

// constraintEntry represents an expected entry in the constraints conformance
// report.
type constraintEntry struct {
	// object is the name of the table/index/partition that this entry refers to.
	// The format is "<database>" or "<table>[.<index>[.<partition>]]". A
	// partition on the primary key is "<table>.<partition>".
	object         string
	constraint     string
	constraintType ConstraintType
	numRanges      int
}

// toReportEntry transforms the entry into the key/value format of the generated
// report.
func (c constraintEntry) toReportEntry(
	objects map[string]ZoneKey,
) (ConstraintStatusKey, ConstraintStatus, error) {
	zk, ok := objects[c.object]
	if !ok {
		return ConstraintStatusKey{}, ConstraintStatus{},
			errors.AssertionFailedf("missing object: " + c.object)
	}
	k := ConstraintStatusKey{
		ZoneKey:       zk,
		ViolationType: c.constraintType,
		Constraint:    ConstraintRepr(c.constraint),
	}
	v := ConstraintStatus{FailRangeCount: c.numRanges}
	return k, v, nil
}

type split struct {
	key    string
	stores []int
}

type store struct {
	id    int
	attrs string // comma separated
}

// toStoreDescriptor transforms the store into a StoreDescriptor.
//
// nodeDesc is the descriptor of the parent node.
func (s store) toStoreDesc(nodeDesc roachpb.NodeDescriptor) roachpb.StoreDescriptor {
	desc := roachpb.StoreDescriptor{
		StoreID: roachpb.StoreID(s.id),
		Node:    nodeDesc,
	}
	desc.Attrs.Attrs = append(desc.Attrs.Attrs, strings.Split(s.attrs, ",")...)
	return desc
}

type node struct {
	id int
	// locality is the node's locality, in the same format that the --locality
	// flag. "" for not setting a locality.
	locality string
	stores   []store
}

func (n node) toDescriptors() (roachpb.NodeDescriptor, []roachpb.StoreDescriptor, error) {
	nodeDesc := roachpb.NodeDescriptor{
		NodeID: roachpb.NodeID(n.id),
	}
	if n.locality != "" {
		if err := nodeDesc.Locality.Set(n.locality); err != nil {
			return roachpb.NodeDescriptor{}, nil, err
		}
	}
	storeDescs := make([]roachpb.StoreDescriptor, len(n.stores))
	for i, s := range n.stores {
		storeDescs[i] = s.toStoreDesc(nodeDesc)
	}
	return nodeDesc, storeDescs, nil
}

type conformanceConstraintTestCase struct {
	name        string
	schema      []database
	splits      []split
	nodes       []node
	deadStores  []int
	defaultZone zone

	exp []constraintEntry
}

func TestConformanceReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []conformanceConstraintTestCase{
		{
			name:        "simple no violations",
			defaultZone: zone{replicas: 3},
			schema: []database{
				{
					name:   "db1",
					tables: []table{{name: "t1"}, {name: "t2"}},
					// The database has a zone requesting everything to be on SSDs.
					zone: &zone{
						replicas:    3,
						constraints: "[+ssd]",
					},
				},
				{
					name:   "db2",
					tables: []table{{name: "sentinel"}},
				},
			},
			splits: []split{
				{key: "/Table/t1", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk/1", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk/2", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk/3", stores: []int{1, 2, 3}},
				{key: "/Table/t2", stores: []int{1, 2, 3}},
				{key: "/Table/t2/pk", stores: []int{1, 2, 3}},
				{
					// This range is not covered by the db1's zone config and so it won't
					// be counted.
					key: "/Table/sentinel", stores: []int{1, 2, 3},
				},
			},
			nodes: []node{
				{id: 1, locality: "", stores: []store{{id: 1, attrs: "ssd"}}},
				{id: 2, locality: "", stores: []store{{id: 2, attrs: "ssd"}}},
				{id: 3, locality: "", stores: []store{{id: 3, attrs: "ssd"}}},
			},
			deadStores: nil,
			exp: []constraintEntry{{
				object:         "db1",
				constraint:     "+ssd",
				constraintType: Constraint,
				numRanges:      0,
			}},
		},
		{
			// Test zone constraints inheritance at all levels.
			name:        "violations at multiple levels",
			defaultZone: zone{replicas: 3, constraints: "[+default]"},
			schema: []database{
				{
					name: "db1",
					// All the objects will have zones asking for different tags.
					zone: &zone{
						replicas:    3,
						constraints: "[+db1]",
					},
					tables: []table{
						{
							name: "t1",
							zone: &zone{
								replicas:    3,
								constraints: "[+t1]",
							},
						},
						{
							name: "t2",
							zone: &zone{
								replicas:    3,
								constraints: "[+t2]",
							},
						},
						// Violations for this one will count towards db1.
						{name: "sentinel"},
					},
				}, {
					name: "db2",
					zone: &zone{constraints: "[+db2]"},
					// Violations for this one will count towards db2, except for the
					// partition part.
					tables: []table{{
						name: "t3",
						partitions: []partition{{
							name:  "p1",
							start: []int{100},
							end:   []int{200},
							zone:  &zone{constraints: "[+p1]"},
						}},
					}},
				}, {
					name: "db3",
					// Violations for this one will count towards the default zone.
					tables: []table{{name: "t4"}},
				},
			},
			splits: []split{
				{key: "/Table/t1", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk/1", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk/2", stores: []int{1, 2, 3}},
				{key: "/Table/t1/pk/3", stores: []int{1, 2, 3}},
				{key: "/Table/t2", stores: []int{1, 2, 3}},
				{key: "/Table/t2/pk", stores: []int{1, 2, 3}},
				{key: "/Table/sentinel", stores: []int{1, 2, 3}},
				{key: "/Table/t3", stores: []int{1, 2, 3}},
				{key: "/Table/t3/pk/100", stores: []int{1, 2, 3}},
				{key: "/Table/t3/pk/101", stores: []int{1, 2, 3}},
				{key: "/Table/t3/pk/199", stores: []int{1, 2, 3}},
				{key: "/Table/t3/pk/200", stores: []int{1, 2, 3}},
				{key: "/Table/t4", stores: []int{1, 2, 3}},
			},
			// None of the stores have any attributes.
			nodes: []node{
				{id: 1, locality: "", stores: []store{{id: 1}}},
				{id: 2, locality: "", stores: []store{{id: 2}}},
				{id: 3, locality: "", stores: []store{{id: 3}}},
			},
			deadStores: nil,
			exp: []constraintEntry{
				{
					object:         "default",
					constraint:     "+default",
					constraintType: Constraint,
					numRanges:      1,
				},
				{
					object:         "db1",
					constraint:     "+db1",
					constraintType: Constraint,
					numRanges:      1,
				},
				{
					object:         "t1",
					constraint:     "+t1",
					constraintType: Constraint,
					numRanges:      5,
				},
				{
					object:         "t2",
					constraint:     "+t2",
					constraintType: Constraint,
					numRanges:      2,
				},
				{
					object:         "db2",
					constraint:     "+db2",
					constraintType: Constraint,
					numRanges:      2,
				},
				{
					object:         "t3.p1",
					constraint:     "+p1",
					constraintType: Constraint,
					numRanges:      3,
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runConformanceReportTest(t, tc)
		})
	}
}

type row struct {
	ConstraintStatusKey
	ConstraintStatus
}

func (r row) String() string {
	return fmt.Sprintf("%s failed:%d", r.ConstraintStatusKey, r.ConstraintStatus.FailRangeCount)
}

type rows []row

func (r rows) String() string {
	var sb strings.Builder
	for _, rr := range r {
		sb.WriteString(rr.String())
		sb.WriteRune('\n')
	}
	return sb.String()
}

// runConformanceReportTest runs one test case. It processes the input schema,
// runs the reports, and verifies that the report looks as expected.
func runConformanceReportTest(t *testing.T, tc conformanceConstraintTestCase) {
	rangeStore, sysCfg, storeResolver, objects, err := processTestCase(tc)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := computeConstraintConformanceReport(
		context.Background(), &rangeStore, sysCfg, storeResolver)
	if err != nil {
		t.Fatal(err)
	}

	// Sort the report's keys.
	gotRows := make(rows, len(rep.constraints))
	i := 0
	for k, v := range rep.constraints {
		gotRows[i] = row{ConstraintStatusKey: k, ConstraintStatus: v}
		i++
	}
	sort.Slice(gotRows, func(i, j int) bool {
		return gotRows[i].ConstraintStatusKey.Less(gotRows[j].ConstraintStatusKey)
	})

	expRows := make(rows, len(tc.exp))
	for i, exp := range tc.exp {
		k, v, err := exp.toReportEntry(objects)
		if err != nil {
			t.Fatalf("failed to process expected entry %d: %s", i, err)
		}
		expRows[i] = row{
			ConstraintStatusKey: k,
			ConstraintStatus:    v,
		}
	}
	require.Equal(t, expRows, gotRows)
}

type testRangeIter struct {
	ranges []roachpb.RangeDescriptor
}

var _ RangeIterator = &testRangeIter{}

// Next is part of the RangeIterator interface.
func (t *testRangeIter) Next(ctx context.Context) (roachpb.RangeDescriptor, error) {
	if len(t.ranges) == 0 {
		return roachpb.RangeDescriptor{}, nil
	}
	first := t.ranges[0]
	t.ranges = t.ranges[1:]
	return first, nil
}

// Close is part of the RangeIterator interface.
func (t *testRangeIter) Close(context.Context) {
	t.ranges = nil
}

// processTestCase takes the input schema and turns it into:
// - a collection of ranges represented as an implementation of RangeStore
// - a SystemConfig populated with descriptors and zone configs.
// - a collection of stores represented as an implementation of StoreResolver.
// - a map from "object names" to ZoneKeys; each table/index/partition with a zone is mapped to
// the id that the report will use for it. See constraintEntry.object for the key format.
func processTestCase(
	tc conformanceConstraintTestCase,
) (testRangeIter, *config.SystemConfig, StoreResolver, map[string]ZoneKey, error) {
	tableToID := make(map[string]int)
	idxToID := make(map[string]int)
	objects := make(map[string]ZoneKey)
	// Databases and tables share the id space, so we'll use a common counter for them.
	// And we're going to use keys in user space, otherwise there's special cases
	// in the zone config lookup that we bump into.
	objectCounter := keys.MinUserDescID
	sysCfgBuilder := systemConfigBuilder{
		// Use a non-zero timestamp for descriptor creation/modification time.
		ts: hlc.Timestamp{WallTime: 1},
	}
	sysCfgBuilder.setDefaultZoneConfig(tc.defaultZone.toZoneConfig())
	objects["default"] = MakeZoneKey(keys.RootNamespaceID, NoSubzone)
	// Assign ids to databases, table, indexes; create descriptors and populate
	// the SystemConfig.
	for _, db := range tc.schema {
		dbID := objectCounter
		objectCounter++
		objects[db.name] = MakeZoneKey(uint32(dbID), NoSubzone)
		if db.zone != nil {
			sysCfgBuilder.addZone(dbID, db.zone.toZoneConfig())
		}
		sysCfgBuilder.addDBDesc(dbID, sqlbase.DatabaseDescriptor{
			Name: db.name,
			ID:   sqlbase.ID(dbID),
		})

		for _, table := range db.tables {
			tableID := objectCounter
			objectCounter++
			tableToID[table.name] = tableID
			objects[table.name] = MakeZoneKey(uint32(tableID), NoSubzone)
			// Create a table descriptor to be used for creating the zone config.
			tableDesc, err := makeTableDesc(table, tableID, dbID)
			if err != nil {
				return testRangeIter{}, nil, nil, nil, errors.Wrap(err, "error creating table descriptor")
			}
			sysCfgBuilder.addTableDesc(tableID, tableDesc)

			// Create the table's zone config.
			var tableZone *config.ZoneConfig
			if table.zone != nil {
				tableZone = new(config.ZoneConfig)
				*tableZone = table.zone.toZoneConfig()
			}
			// Add subzones for the PK partitions.
			if len(table.partitions) > 0 {
				pk := index{
					name:       "PK",
					zone:       nil, // PK's never have zones; the table might have a zone.
					partitions: table.partitions,
				}
				tableZone = addIndexSubzones(pk, tableZone, tableDesc, 1 /* id of PK */, objects)
			}
			// Add subzones for all the indexes.
			for i, idx := range table.indexes {
				idxID := i + 2 // index 1 is the PK
				idxToID[fmt.Sprintf("%s.%s", table.name, idx.name)] = idxID
				tableZone = addIndexSubzones(idx, tableZone, tableDesc, idxID, objects)
			}
			// Fill in the SubzoneSpans.
			if tableZone != nil {
				var err error
				tableZone.SubzoneSpans, err = sql.GenerateSubzoneSpans(
					nil, uuid.UUID{} /* clusterID */, &tableDesc, tableZone.Subzones,
					false /* hasNewSubzones */)
				if err != nil {
					return testRangeIter{}, nil, nil, nil, errors.Wrap(err, "error generating subzone spans")
				}
				sysCfgBuilder.addZone(tableID, *tableZone)
			}
		}
	}

	keyScanner := keysutils.MakePrettyScannerForNamedTables(tableToID, idxToID)
	ranges := make([]roachpb.RangeDescriptor, len(tc.splits))
	for i, split := range tc.splits {
		prettyKey := tc.splits[i].key
		startKey, err := keyScanner.Scan(split.key)
		if err != nil {
			return testRangeIter{}, nil, nil, nil, errors.Wrapf(err, "failed to parse key: %s", prettyKey)
		}
		var endKey roachpb.Key
		if i < len(tc.splits)-1 {
			prettyKey := tc.splits[i+1].key
			endKey, err = keyScanner.Scan(prettyKey)
			if err != nil {
				return testRangeIter{}, nil, nil, nil, errors.Wrapf(err, "failed to parse key: %s", prettyKey)
			}
		} else {
			endKey = roachpb.KeyMax
		}

		rd := roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i + 1), // IDs start at 1
			StartKey: keys.MustAddr(startKey),
			EndKey:   keys.MustAddr(endKey),
		}
		for _, storeID := range split.stores {
			rd.AddReplica(roachpb.NodeID(storeID), roachpb.StoreID(storeID), roachpb.VOTER_FULL)
		}
		ranges[i] = rd
	}

	var storeDescs []roachpb.StoreDescriptor
	for _, n := range tc.nodes {
		_ /* nodeDesc */, sds, err := n.toDescriptors()
		if err != nil {
			return testRangeIter{}, nil, nil, nil, err
		}
		storeDescs = append(storeDescs, sds...)
	}
	storeResolver := func(r roachpb.RangeDescriptor) []roachpb.StoreDescriptor {
		stores := make([]roachpb.StoreDescriptor, len(r.Replicas().Voters()))
		for i, rep := range r.Replicas().Voters() {
			for _, desc := range storeDescs {
				if rep.StoreID == desc.StoreID {
					stores[i] = desc
					break
				}
			}
		}
		return stores
	}
	return testRangeIter{ranges: ranges}, sysCfgBuilder.build(), storeResolver, objects, nil
}

func makeTableDesc(t table, tableID int, dbID int) (sqlbase.TableDescriptor, error) {
	if err := t.validate(); err != nil {
		return sqlbase.TableDescriptor{}, err
	}

	desc := sqlbase.TableDescriptor{
		ID:       sqlbase.ID(tableID),
		Name:     t.name,
		ParentID: sqlbase.ID(dbID),
	}
	pkIdx := index{
		name:       "PK",
		zone:       nil,
		partitions: t.partitions,
	}
	desc.Indexes = append(desc.Indexes, pkIdx.toIndexDescriptor(1))

	for i, idx := range t.indexes {
		idxID := i + 2 // index 1 is the PK
		desc.Indexes = append(desc.Indexes, idx.toIndexDescriptor(idxID))
	}
	numCols := 0
	for _, idx := range desc.Indexes {
		c := len(idx.ColumnIDs)
		if c > numCols {
			numCols = c
		}
	}
	for i := 0; i < numCols; i++ {
		desc.Columns = append(desc.Columns, sqlbase.ColumnDescriptor{
			Name: fmt.Sprintf("col%d", i),
			ID:   sqlbase.ColumnID(i),
			Type: *types.Int,
		})
	}

	return desc, nil
}

// addIndexSubzones creates subzones for an index and all of its partitions and
// appends them to a parent table zone, returning the amended parent.
// If the index and its partitions have no zones, the parent is returned unchanged (
// and possibly nil).
//
// parent: Can be nil if the parent table doesn't have a zone of its own. In that
//   case, if any subzones are created, a placeholder zone will also be created and returned.
// subzone: The index's ZoneConfig. Can be nil if the index doesn't have a zone config.
// objects: A mapping from object name to ZoneKey that gets populated with all the new subzones.
func addIndexSubzones(
	idx index,
	parent *config.ZoneConfig,
	tableDesc sql.TableDescriptor,
	idxID int,
	objects map[string]ZoneKey,
) *config.ZoneConfig {
	res := parent

	ensureParent := func() {
		if res != nil {
			return
		}
		// Create a placeholder zone config.
		res = config.NewZoneConfig()
		res.DeleteTableConfig()
	}

	if idx.zone != nil {
		ensureParent()
		res.SetSubzone(config.Subzone{
			IndexID:       uint32(idxID),
			PartitionName: "",
			Config:        idx.zone.toZoneConfig(),
		})
		objects[fmt.Sprintf("%s.%s", tableDesc.Name, idx.name)] =
			MakeZoneKey(uint32(tableDesc.ID), SubzoneID(len(res.Subzones)))
	}

	for _, p := range idx.partitions {
		if p.zone != nil {
			ensureParent()
			res.SetSubzone(config.Subzone{
				IndexID:       uint32(idxID),
				PartitionName: p.name,
				Config:        p.zone.toZoneConfig(),
			})
			var objectName string
			if idxID == 1 {
				objectName = fmt.Sprintf("%s.%s", tableDesc.Name, p.name)
			} else {
				objectName = fmt.Sprintf("%s.%s.%s", tableDesc.Name, idx.name, p.name)
			}
			objects[objectName] = MakeZoneKey(uint32(tableDesc.ID), SubzoneID(len(res.Subzones)))
		}
	}
	return res
}

// systemConfigBuilder build a system config. Clients will call some setters and then call build().
type systemConfigBuilder struct {
	kv                []roachpb.KeyValue
	defaultZoneConfig *config.ZoneConfig

	// ts is used for the creation time of synthesized descriptors
	ts hlc.Timestamp
}

func (b *systemConfigBuilder) setDefaultZoneConfig(cfg config.ZoneConfig) {
	b.defaultZoneConfig = &cfg
	b.addZone(keys.RootNamespaceID, cfg)
}

func (b *systemConfigBuilder) addZone(id int, cfg config.ZoneConfig) {
	k := config.MakeZoneKey(uint32(id))
	var v roachpb.Value
	if err := v.SetProto(&cfg); err != nil {
		panic(err)
	}
	b.kv = append(b.kv, roachpb.KeyValue{Key: k, Value: v})
}

// build constructs a SystemConfig containing all the information accumulated in the builder.
func (b *systemConfigBuilder) build() *config.SystemConfig {
	if b.defaultZoneConfig == nil {
		panic("default zone config not set")
	}

	sort.Slice(b.kv, func(i, j int) bool {
		return bytes.Compare(b.kv[i].Key, b.kv[j].Key) < 0
	})

	cfg := config.NewSystemConfig(b.defaultZoneConfig)
	cfg.SystemConfigEntries.Values = b.kv
	return cfg
}

// addTableDesc adds a table descriptor to the SystemConfig.
func (b *systemConfigBuilder) addTableDesc(id int, tableDesc sqlbase.TableDescriptor) {
	if tableDesc.ParentID == 0 {
		panic(fmt.Sprintf("parent not set for table %q", tableDesc.Name))
	}
	// Write the table to the SystemConfig, in the descriptors table.
	k := sqlbase.MakeDescMetadataKey(sqlbase.ID(id))
	desc := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Table{
			Table: &tableDesc,
		},
	}
	desc.Table(b.ts)
	var v roachpb.Value
	if err := v.SetProto(desc); err != nil {
		panic(err)
	}
	b.kv = append(b.kv, roachpb.KeyValue{Key: k, Value: v})
}

// addTableDesc adds a database descriptor to the SystemConfig.
func (b *systemConfigBuilder) addDBDesc(id int, dbDesc sqlbase.DatabaseDescriptor) {
	// Write the table to the SystemConfig, in the descriptors table.
	k := sqlbase.MakeDescMetadataKey(sqlbase.ID(id))
	desc := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Database{
			Database: &dbDesc,
		},
	}
	var v roachpb.Value
	if err := v.SetProto(desc); err != nil {
		panic(err)
	}
	b.kv = append(b.kv, roachpb.KeyValue{Key: k, Value: v})
}

// Test the constraint conformance report in a real cluster.
func TestConstraintConformanceReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#40919")

	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 5, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
			1: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}}},
			2: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			3: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
			4: {Locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}}},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Create a table and a zone config for it.
	// The zone will be configured with a constraints that can't be satisfied
	// because there are not enough nodes in the requested region.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using constraints='[+region=r1]'")
	require.NoError(t, err)

	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Wait for the violation to be detected.
	testutils.SucceedsSoon(t, func() error {
		r := db.QueryRow(
			"select violating_ranges from system.replication_constraint_stats where zone_id = $1",
			zoneID)
		var numViolations int
		if err := r.Scan(&numViolations); err != nil {
			return err
		}
		if numViolations == 0 {
			return fmt.Errorf("violation not detected yet")
		}
		return nil
	})

	// Now change the constraint asking for t to be placed in r2. This time it can be satisfied.
	_, err = db.Exec("alter table t configure zone using constraints='[+region=r2]'")
	require.NoError(t, err)

	// Wait for the violation to clear.
	testutils.SucceedsSoon(t, func() error {
		r := db.QueryRow(
			"select violating_ranges from system.replication_constraint_stats where zone_id = $1",
			zoneID)
		var numViolations int
		if err := r.Scan(&numViolations); err != nil {
			return err
		}
		if numViolations > 0 {
			return fmt.Errorf("still reporting violations")
		}
		return nil
	})
}

// Test the critical localities report in a real cluster.
func TestCriticalLocalitiesReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	// 2 regions, 3 dcs per region.
	tc := serverutils.StartTestCluster(t, 6, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc1"}},
				},
			},
			1: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc2"}},
				},
			},
			2: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc3"}},
				},
			},
			3: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc4"}},
				},
			},
			4: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc5"}},
				},
			},
			5: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc6"}},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the reports.
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Since we're using ReplicationManual, all the ranges will start with a
	// single replica on node 1. So, the node's dc and the node's region are
	// critical. Let's verify that.

	// Collect all the zones that exist at cluster bootstrap.
	systemZoneIDs := make([]int, 0, 10)
	systemZones := make([]config.ZoneConfig, 0, 10)
	{
		rows, err := db.Query("select id, config from system.zones")
		require.NoError(t, err)
		for rows.Next() {
			var zoneID int
			var buf []byte
			cfg := config.ZoneConfig{}
			require.NoError(t, rows.Scan(&zoneID, &buf))
			require.NoError(t, protoutil.Unmarshal(buf, &cfg))
			systemZoneIDs = append(systemZoneIDs, zoneID)
			systemZones = append(systemZones, cfg)
		}
		require.NoError(t, rows.Err())
	}
	require.True(t, len(systemZoneIDs) > 0, "expected some system zones, got none")
	// Remove the entries in systemZoneIDs that don't get critical locality reports.
	i := 0
	for j, zid := range systemZoneIDs {
		if zoneChangesReplication(&systemZones[j]) {
			systemZoneIDs[i] = zid
			i++
		}
	}
	systemZoneIDs = systemZoneIDs[:i]

	expCritLoc := []string{"region=r1", "region=r1,dc=dc1"}

	// Wait for the report to be generated.
	{
		var rowCount int
		testutils.SucceedsSoon(t, func() error {
			r := db.QueryRow("select count(1) from system.replication_critical_localities")
			require.NoError(t, r.Scan(&rowCount))
			if rowCount == 0 {
				return fmt.Errorf("no report yet")
			}
			return nil
		})
		require.Equal(t, 2*len(systemZoneIDs), rowCount)
	}

	// Check that we have all the expected rows.
	for _, zid := range systemZoneIDs {
		for _, s := range expCritLoc {
			r := db.QueryRow(
				"select at_risk_ranges from system.replication_critical_localities "+
					"where zone_id=$1 and locality=$2",
				zid, s)
			var numRanges int
			require.NoError(t, r.Scan(&numRanges))
			require.NotEqual(t, 0, numRanges)
		}
	}

	// Now create a table and a zone for it. At first n1 should be critical for it.
	// Then we'll upreplicate it in different ways.

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using num_replicas=3; " +
		"alter table t split at values (0);")
	require.NoError(t, err)
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Check initial conditions.
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1"))

	// Upreplicate to 2 dcs. Now they're both critical.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r1", "region=r1,dc=dc1", "region=r1,dc=dc2"))

	// Upreplicate to one more dc. Now no dc is critical, only the region.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r1"))

	// Move two replicas to the other region. Now that region is critical.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,4,5], 1)")
	require.NoError(t, err)
	require.NoError(t, checkCritical(db, zoneID, "region=r2"))
}

func checkCritical(db *gosql.DB, zoneID int, locs ...string) error {
	return testutils.SucceedsSoonError(func() error {
		rows, err := db.Query(
			"select locality, at_risk_ranges from system.replication_critical_localities "+
				"where zone_id=$1", zoneID)
		if err != nil {
			return err
		}
		critical := make(map[string]struct{})
		for rows.Next() {
			var numRanges int
			var loc string
			err := rows.Scan(&loc, &numRanges)
			if err != nil {
				return err
			}
			if numRanges == 0 {
				return fmt.Errorf("expected ranges_at_risk for %s", loc)
			}
			critical[loc] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(locs) != len(critical) {
			return fmt.Errorf("expected critical: %s, got: %s", locs, critical)
		}
		for _, l := range locs {
			if _, ok := critical[l]; !ok {
				return fmt.Errorf("missing critical locality: %s", l)
			}
		}
		return nil
	})
}

// Test the replication status report in a real cluster.
func TestReplicationStatusReportIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tc := serverutils.StartTestCluster(t, 4, base.TestClusterArgs{
		// We're going to do our own replication.
		// All the system ranges will start with a single replica on node 1.
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	// Speed up the generation of the
	_, err := db.Exec("set cluster setting kv.replication_reports.interval = '1ms'")
	require.NoError(t, err)

	// Create a table with a dummy zone config. Configuring the zone is useful
	// only for creating the zone; we don't actually care about the configuration.
	// Also do a split by hand. With manual replication, we're not getting the
	// split for the table automatically.
	_, err = db.Exec("create table t(x int primary key); " +
		"alter table t configure zone using num_replicas=3; " +
		"alter table t split at values (0);")
	require.NoError(t, err)
	// Get the id of the newly created zone.
	r := db.QueryRow("select zone_id from crdb_internal.zones where table_name = 't'")
	var zoneID int
	require.NoError(t, r.Scan(&zoneID))

	// Upreplicate the range.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)")
	require.NoError(t, err)
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 0, 0))

	// Over-replicate.
	_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3,4], 1)")
	require.NoError(t, err)
	require.NoError(t, checkZoneReplication(db, zoneID, 1, 0, 1, 0))

	// TODO(andrei): I'd like to downreplicate to one replica and then stop that
	// node and check that the range is counter us "unavailable", but stopping a
	// node makes the report generation simply block sometimes trying to scan
	// Meta2. I believe I believe it's due to #40529.
	// Once stopping a node works, next thing is to start it up again.
	// Take inspiration from replica_learner_test.go.

	//// Down-replicate to one node and then kill the node. Check that the range becomes unavailable.
	//_, err = db.Exec("ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[4], 1)")
	//require.NoError(t, err)
	//tc.StopServer(3)
	//require.NoError(t, checkZoneReplication(db, zoneID, 1, 1, 0, 1))
}

func checkZoneReplication(db *gosql.DB, zoneID, total, under, over, unavailable int) error {
	return testutils.SucceedsSoonError(func() error {
		r := db.QueryRow(
			"select total_ranges, under_replicated_ranges, over_replicated_ranges, "+
				"unavailable_ranges from system.replication_stats where zone_id=$1",
			zoneID)
		var gotTotal, gotUnder, gotOver, gotUnavailable int
		if err := r.Scan(&gotTotal, &gotUnder, &gotOver, &gotUnavailable); err != nil {
			return err
		}
		if total != gotTotal {
			return fmt.Errorf("expected total: %d, got: %d", total, gotTotal)
		}
		if under != gotUnder {
			return fmt.Errorf("expected under: %d, got: %d", total, gotUnder)
		}
		if over != gotOver {
			return fmt.Errorf("expected over: %d, got: %d", over, gotOver)
		}
		if unavailable != gotUnavailable {
			return fmt.Errorf("expected unavailable: %d, got: %d", unavailable, gotUnavailable)
		}
		return nil
	})
}

func TestMeta2RangeIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// First make an interator with a large page size and use it to determine the numner of ranges.
	iter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	numRanges := 0
	for {
		rd, err := iter.Next(ctx)
		require.NoError(t, err)
		if rd.RangeID == 0 {
			break
		}
		numRanges++
	}
	require.True(t, numRanges > 20, "expected over 20 ranges, got: %d", numRanges)

	// Now make an interator with a small page size and check that we get just as many ranges.
	iter = makeMeta2RangeIter(db, 2 /* batch size */)
	numRangesPaginated := 0
	for {
		rd, err := iter.Next(ctx)
		require.NoError(t, err)
		if rd.RangeID == 0 {
			break
		}
		numRangesPaginated++
	}
	require.Equal(t, numRanges, numRangesPaginated)
}

// Test that a retriable error returned from the range iterator is properly
// handled by resetting the report.
func TestRetriableErrorWhenGenerationReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	cfg := s.GossipI().(*gossip.Gossip).GetSystemConfig()
	dummyNodeChecker := func(id roachpb.NodeID) bool { return true }

	saver := makeReplicationStatsReportSaver()
	v := makeReplicationStatsVisitor(ctx, cfg, dummyNodeChecker, &saver)
	realIter := makeMeta2RangeIter(db, 10000 /* batchSize */)
	require.NoError(t, visitRanges(ctx, &realIter, cfg, &v))
	expReport := v.report
	require.True(t, len(expReport.stats) > 0, "unexpected empty report")

	realIter = makeMeta2RangeIter(db, 10000 /* batchSize */)
	errorIter := erroryRangeIterator{
		iter:           realIter,
		injectErrAfter: 3,
	}
	v = makeReplicationStatsVisitor(ctx, cfg, func(id roachpb.NodeID) bool { return true }, &saver)
	require.NoError(t, visitRanges(ctx, &errorIter, cfg, &v))
	require.True(t, len(v.report.stats) > 0, "unexpected empty report")
	require.Equal(t, expReport, v.report)
}

type erroryRangeIterator struct {
	iter           meta2RangeIter
	rangesReturned int
	injectErrAfter int
}

var _ RangeIterator = &erroryRangeIterator{}

func (it *erroryRangeIterator) Next(ctx context.Context) (roachpb.RangeDescriptor, error) {
	if it.rangesReturned == it.injectErrAfter {
		// Don't inject any more errors.
		it.injectErrAfter = -1

		var err error
		err = roachpb.NewTransactionRetryWithProtoRefreshError(
			"injected err", uuid.Nil, roachpb.Transaction{})
		// Let's wrap the error to check the unwrapping.
		err = errors.Wrap(err, "dummy wrapper")
		// Feed the error to the underlying iterator to reset it.
		it.iter.handleErr(ctx, err)
		return roachpb.RangeDescriptor{}, err
	}
	it.rangesReturned++
	rd, err := it.iter.Next(ctx)
	return rd, err
}

func (it *erroryRangeIterator) Close(ctx context.Context) {
	it.iter.Close(ctx)
}

// computeConstraintConformanceReport iterates through all the ranges and
// generates the constraint conformance report.
func computeConstraintConformanceReport(
	ctx context.Context,
	rangeStore RangeIterator,
	cfg *config.SystemConfig,
	storeResolver StoreResolver,
) (*replicationConstraintStatsReportSaver, error) {
	saver := makeReplicationConstraintStatusReportSaver()
	v := makeConstraintConformanceVisitor(ctx, cfg, storeResolver, &saver)
	err := visitRanges(ctx, rangeStore, cfg, &v)
	return v.report, err
}
