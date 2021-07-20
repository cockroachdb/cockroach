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
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/keysutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestConformanceReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tests := []conformanceConstraintTestCase{
		{
			name: "simple no violations",
			baseReportTestCase: baseReportTestCase{
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
					{key: "/Table/t1", stores: "1 2 3"},
					{key: "/Table/t1/pk", stores: "1 2 3"},
					{key: "/Table/t1/pk/1", stores: "1 2 3"},
					{key: "/Table/t1/pk/2", stores: "1 2 3"},
					{key: "/Table/t1/pk/3", stores: "1 2 3"},
					{key: "/Table/t2", stores: "1 2 3"},
					{key: "/Table/t2/pk", stores: "1 2 3"},
					{
						// This range is not covered by the db1's zone config and so it won't
						// be counted.
						key: "/Table/sentinel", stores: "1 2 3",
					},
				},
				nodes: []node{
					{id: 1, locality: "", stores: []store{{id: 1, attrs: "ssd"}}},
					{id: 2, locality: "", stores: []store{{id: 2, attrs: "ssd"}}},
					{id: 3, locality: "", stores: []store{{id: 3, attrs: "ssd"}}},
				},
			},
			exp: []constraintEntry{{
				object:         "db1",
				constraint:     "+ssd",
				constraintType: Constraint,
				numRanges:      0,
			}},
		},
		{
			name: "violations at multiple levels",
			// Test zone constraints inheritance at all levels.
			baseReportTestCase: baseReportTestCase{
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
					{key: "/Table/t1", stores: "1 2 3"},
					{key: "/Table/t1/pk", stores: "1 2 3"},
					{key: "/Table/t1/pk/1", stores: "1 2 3"},
					{key: "/Table/t1/pk/2", stores: "1 2 3"},
					{key: "/Table/t1/pk/3", stores: "1 2 3"},
					{key: "/Table/t2", stores: "1 2 3"},
					{key: "/Table/t2/pk", stores: "1 2 3"},
					{key: "/Table/sentinel", stores: "1 2 3"},
					{key: "/Table/t3", stores: "1 2 3"},
					{key: "/Table/t3/pk/100", stores: "1 2 3"},
					{key: "/Table/t3/pk/101", stores: "1 2 3"},
					{key: "/Table/t3/pk/199", stores: "1 2 3"},
					{key: "/Table/t3/pk/200", stores: "1 2 3"},
					{key: "/Table/t4", stores: "1 2 3"},
				},
				// None of the stores have any attributes.
				nodes: []node{
					{id: 1, locality: "", stores: []store{{id: 1}}},
					{id: 2, locality: "", stores: []store{{id: 2}}},
					{id: 3, locality: "", stores: []store{{id: 3}}},
				},
			},
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
		{
			// Test that, when we have conjunctions of constraints, we report at the
			// level of the whole conjunction, not for individuals constraints within
			// the conjunction. I.e. if we have a constraint {"+region=us,+dc=dc1":1}
			// (one replica needs to be in a node with locality us,dc1), and instead
			// the replica is in us,dc2, then the report has an entry for the pair
			// "us,dc1" with a violation instead of one entry for "us" with no
			// violation, and another entry for "dc1" with one violation.
			name: "constraint conjunctions",
			baseReportTestCase: baseReportTestCase{
				defaultZone: zone{replicas: 3},
				schema: []database{
					{
						name:   "db1",
						tables: []table{{name: "t1"}, {name: "t2"}},
						// The database has a zone requesting everything to be on SSDs.
						zone: &zone{
							replicas: 2,
							// The first conjunction will be satisfied; the second won't.
							constraints: `{"+region=us,+dc=dc1":1,"+region=us,+dc=dc2":1}`,
						},
					},
				},
				splits: []split{
					{key: "/Table/t1", stores: "1 2"},
				},
				nodes: []node{
					{id: 1, locality: "region=us,dc=dc1", stores: []store{{id: 1}}},
					{id: 2, locality: "region=us,dc=dc3", stores: []store{{id: 2}}},
				},
			},
			exp: []constraintEntry{
				{
					object:         "db1",
					constraint:     "+region=us,+dc=dc1:1",
					constraintType: Constraint,
					numRanges:      0,
				},
				{
					object:         "db1",
					constraint:     "+region=us,+dc=dc2:1",
					constraintType: Constraint,
					numRanges:      1,
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

// runConformanceReportTest runs one test case. It processes the input schema,
// runs the reports, and verifies that the report looks as expected.
func runConformanceReportTest(t *testing.T, tc conformanceConstraintTestCase) {
	ctc, err := compileTestCase(tc.baseReportTestCase)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := computeConstraintConformanceReport(
		context.Background(), &ctc.iter, ctc.cfg, ctc.resolver)
	if err != nil {
		t.Fatal(err)
	}

	// Sort the report's keys.
	gotRows := make(rows, len(rep))
	i := 0
	for k, v := range rep {
		gotRows[i] = row{ConstraintStatusKey: k, ConstraintStatus: v}
		i++
	}
	sort.Slice(gotRows, func(i, j int) bool {
		return gotRows[i].ConstraintStatusKey.Less(gotRows[j].ConstraintStatusKey)
	})

	expRows := make(rows, len(tc.exp))
	for i, exp := range tc.exp {
		k, v, err := exp.toReportEntry(ctc.objectToZone)
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

type zone struct {
	// 0 means unset.
	replicas int32
	// "" means unset. "[]" means empty.
	constraints string
}

func (z zone) toZoneConfig() zonepb.ZoneConfig {
	cfg := zonepb.NewZoneConfig()
	if z.replicas != 0 {
		cfg.NumReplicas = proto.Int32(z.replicas)
	}
	if z.constraints != "" {
		var constraintsList zonepb.ConstraintsList
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

func (p partition) toPartitionDescriptor() descpb.PartitioningDescriptor_Range {
	var startKey roachpb.Key
	for _, val := range p.start {
		startKey = encoding.EncodeIntValue(startKey, encoding.NoColumnID, int64(val))
	}
	var endKey roachpb.Key
	for _, val := range p.end {
		endKey = encoding.EncodeIntValue(endKey, encoding.NoColumnID, int64(val))
	}
	return descpb.PartitioningDescriptor_Range{
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

func (idx index) toIndexDescriptor(id int) descpb.IndexDescriptor {
	var idxDesc descpb.IndexDescriptor
	idxDesc.ID = descpb.IndexID(id)
	idxDesc.Name = idx.name
	if len(idx.partitions) > 0 {
		neededCols := idx.partitions.numCols()
		for i := 0; i < neededCols; i++ {
			idxDesc.KeyColumnIDs = append(idxDesc.KeyColumnIDs, descpb.ColumnID(i))
			idxDesc.KeyColumnNames = append(idxDesc.KeyColumnNames, fmt.Sprintf("col%d", i))
			idxDesc.KeyColumnDirections = append(idxDesc.KeyColumnDirections, descpb.IndexDescriptor_ASC)
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
	if len(t.indexes) == 0 || t.indexes[0].name != "PK" {
		return errors.Errorf("missing PK index from table: %q", t.name)
	}
	return t.partitions.validate()
}

func (t *table) addPKIdx() {
	if len(t.indexes) > 0 && t.indexes[0].name == "PK" {
		return
	}
	// Add the PK index if missing.
	pkIdx := index{
		name:       "PK",
		zone:       nil,
		partitions: t.partitions,
	}
	t.indexes = append([]index{pkIdx}, t.indexes...)
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
			errors.AssertionFailedf("missing object: %s", c.object)
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
	key string
	// stores lists the stores that are gonna have replicas for a particular range. It's a space-separated
	// list of store ids, with an optional replica type. If the type of replica is missing, it will
	// be a voter-full.
	// The format is a space-separated list of <storeID><?replica type>. For example:
	// "1 2 3 4v 5i 6o 7d 8l" (1,2,3,4 are voter-full, then incoming, outgoing, demoting, learner).
	stores string
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
	// dead, if set, indicates that the node is to be considered dead for the
	// purposes of reports generation. In production, this corresponds to a node
	// with an expired liveness record.
	dead bool
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
	baseReportTestCase
	name string
	exp  []constraintEntry
}

type baseReportTestCase struct {
	schema      []database
	splits      []split
	nodes       []node
	defaultZone zone
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

func TestConstraintReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// This test uses the cluster as a recipient for a report saved from outside
	// the cluster. We disable the cluster's own production of reports so that it
	// doesn't interfere with the test.
	ReporterInterval.Override(ctx, &st.SV, 0)
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	con := s.InternalExecutor().(sqlutil.InternalExecutor)
	defer s.Stopper().Stop(ctx)

	// Verify that tables are empty.
	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{})

	// Add several replication constraint statuses.
	report := make(ConstraintReport)
	report.ensureEntry(MakeZoneKey(1, 3), "constraint", "+country=CH")
	report.AddViolation(MakeZoneKey(2, 3), "constraint", "+country=CH")
	report.ensureEntry(MakeZoneKey(2, 3), "constraint", "+country=CH")
	report.AddViolation(MakeZoneKey(5, 6), "constraint", "+ssd")
	report.AddViolation(MakeZoneKey(5, 6), "constraint", "+ssd")
	report.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	report.ensureEntry(MakeZoneKey(7, 8), "constraint", "+dc=east")
	report.ensureEntry(MakeZoneKey(7, 8), "constraint", "+dc=east")
	report.AddViolation(MakeZoneKey(8, 9), "constraint", "+dc=west")
	report.ensureEntry(MakeZoneKey(8, 9), "constraint", "+dc=east")

	time1 := time.Date(2001, 1, 1, 10, 0, 0, 0, time.UTC)
	r := makeReplicationConstraintStatusReportSaver()
	require.NoError(t, r.Save(ctx, report, time1, db, con))
	report = make(ConstraintReport)

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		{"1", "3", "'constraint'", "'+country=CH'", "1", "NULL", "0"},
		{"2", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"5", "6", "'constraint'", "'+ssd'", "1", "'2001-01-01 10:00:00+00:00'", "2"},
		{"7", "8", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"7", "8", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
		{"8", "9", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"8", "9", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 10:00:00+00:00'"},
	})
	require.Equal(t, 7, r.LastUpdatedRowCount())

	// Add new set of replication constraint statuses to the existing report and verify the old ones are deleted.
	report.AddViolation(MakeZoneKey(1, 3), "constraint", "+country=CH")
	report.ensureEntry(MakeZoneKey(5, 6), "constraint", "+ssd")
	report.AddViolation(MakeZoneKey(6, 8), "constraint", "+dc=east")
	report.ensureEntry(MakeZoneKey(6, 8), "constraint", "+dc=west")
	report.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	report.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	report.AddViolation(MakeZoneKey(8, 9), "constraint", "+dc=west")
	report.ensureEntry(MakeZoneKey(8, 9), "constraint", "+dc=east")

	time2 := time.Date(2001, 1, 1, 11, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, report, time2, db, con))
	report = make(ConstraintReport)

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		// Wasn't violated before - is violated now.
		{"1", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 11:00:00+00:00'", "1"},
		// Was violated before - isn't violated now.
		{"5", "6", "'constraint'", "'+ssd'", "1", "NULL", "0"},
		// Didn't exist before - new for this run and violated.
		{"6", "8", "'constraint'", "'+dc=east'", "1", "'2001-01-01 11:00:00+00:00'", "1"},
		// Didn't exist before - new for this run and not violated.
		{"6", "8", "'constraint'", "'+dc=west'", "1", "NULL", "0"},
		// Was violated before - and it still is but the range count changed.
		{"7", "8", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "2"},
		// Was violated before - and it still is but the range count didn't change.
		{"8", "9", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		// Wasn't violated before - and is still not violated.
		{"8", "9", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 11:00:00+00:00'"},
	})
	require.Equal(t, 7, r.LastUpdatedRowCount())

	time3 := time.Date(2001, 1, 1, 11, 30, 0, 0, time.UTC)
	// If some other server takes over and does an update.
	rows, err := con.Exec(ctx, "another-updater", nil, "update system.reports_meta set generated=$1 where id=1", time3)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "update system.replication_constraint_stats "+
		"set violation_start=null, violating_ranges=0 where zone_id=1 and subzone_id=3")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "update system.replication_constraint_stats "+
		"set violation_start=$1, violating_ranges=5 where zone_id=5 and subzone_id=6", time3)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "delete from system.replication_constraint_stats "+
		"where zone_id=7 and subzone_id=8")
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	// Add new set of replication constraint statuses to the existing report and verify the everything is good.
	report.AddViolation(MakeZoneKey(1, 3), "constraint", "+country=CH")
	report.ensureEntry(MakeZoneKey(5, 6), "constraint", "+ssd")
	report.AddViolation(MakeZoneKey(6, 8), "constraint", "+dc=east")
	report.ensureEntry(MakeZoneKey(6, 8), "constraint", "+dc=west")
	report.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	report.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	report.AddViolation(MakeZoneKey(8, 9), "constraint", "+dc=west")
	report.ensureEntry(MakeZoneKey(8, 9), "constraint", "+dc=east")

	time4 := time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, report, time4, db, con))
	report = make(ConstraintReport)

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		{"1", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 12:00:00+00:00'", "1"},
		{"5", "6", "'constraint'", "'+ssd'", "1", "NULL", "0"},
		{"6", "8", "'constraint'", "'+dc=east'", "1", "'2001-01-01 11:00:00+00:00'", "1"},
		{"6", "8", "'constraint'", "'+dc=west'", "1", "NULL", "0"},
		{"7", "8", "'constraint'", "'+dc=west'", "1", "'2001-01-01 12:00:00+00:00'", "2"},
		{"8", "9", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"8", "9", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 12:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	// A brand new report (after restart for example) - still works.
	// Add several replication constraint statuses.
	r = makeReplicationConstraintStatusReportSaver()
	report.AddViolation(MakeZoneKey(1, 3), "constraint", "+country=CH")

	time5 := time.Date(2001, 1, 1, 12, 30, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, report, time5, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		{"1", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 12:00:00+00:00'", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 12:30:00+00:00'"},
	})
	require.Equal(t, 6, r.LastUpdatedRowCount())
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

// compiledTestCase represents the result of calling compileTestCase(). It
// contains everything needed to generate replication reports for a test case.
type compiledTestCase struct {
	// A collection of ranges represented as an implementation of RangeStore.
	iter testRangeIter
	// The SystemConfig populated with descriptors and zone configs.
	cfg *config.SystemConfig
	// A collection of stores represented as an implementation of StoreResolver.
	resolver StoreResolver
	// A function that dictates whether a node is to be considered live or dead.
	checker nodeChecker

	// A map from "object names" to ZoneKeys; each table/index/partition with a
	// zone is mapped to the id that the report will use for it. See
	// constraintEntry.object for the key format.
	objectToZone map[string]ZoneKey
	// The inverse of objectToZone.
	zoneToObject map[ZoneKey]string

	// nodeLocalities maps from each node to its locality.
	nodeLocalities map[roachpb.NodeID]roachpb.Locality
	// allLocalities is the list of all localities in the cluster, at all
	// granularities.
	allLocalities map[roachpb.NodeID]map[string]roachpb.Locality
}

// compileTestCase takes the input schema and turns it into a compiledTestCase.
func compileTestCase(tc baseReportTestCase) (compiledTestCase, error) {
	tableToID := make(map[string]int)
	idxToID := make(map[string]int)
	// Databases and tables share the id space, so we'll use a common counter for them.
	// And we're going to use keys in user space, otherwise there's special cases
	// in the zone config lookup that we bump into.
	objectCounter := keys.MinUserDescID
	sysCfgBuilder := makeSystemConfigBuilder()
	if err := sysCfgBuilder.setDefaultZoneConfig(tc.defaultZone.toZoneConfig()); err != nil {
		return compiledTestCase{}, err
	}
	// Assign ids to databases, table, indexes; create descriptors and populate
	// the SystemConfig.
	for _, db := range tc.schema {
		dbID := objectCounter
		objectCounter++
		if db.zone != nil {
			if err := sysCfgBuilder.addDatabaseZone(db.name, dbID, db.zone.toZoneConfig()); err != nil {
				return compiledTestCase{}, err
			}
		}
		sysCfgBuilder.addDBDesc(dbID,
			dbdesc.NewInitial(descpb.ID(dbID), db.name, security.AdminRoleName()))

		for _, table := range db.tables {
			tableID := objectCounter
			objectCounter++
			tableToID[table.name] = tableID

			// Create a table descriptor to be used for creating the zone config.
			table.addPKIdx()
			tableDesc, err := makeTableDesc(table, tableID, dbID)
			if err != nil {
				return compiledTestCase{}, errors.Wrap(err, "error creating table descriptor")
			}
			sysCfgBuilder.addTableDesc(tableID, tableDesc)

			tableZone, err := generateTableZone(table, tableDesc)
			if err != nil {
				return compiledTestCase{}, err
			}
			if tableZone != nil {
				if err := sysCfgBuilder.addTableZone(tableDesc, *tableZone); err != nil {
					return compiledTestCase{}, err
				}
			}
			// Add the indexes to idxToID.
			for i, idx := range table.indexes {
				idxID := i + 1 // index 1 is the PK
				idxToID[fmt.Sprintf("%s.%s", table.name, idx.name)] = idxID
			}
		}
	}

	var allStores []roachpb.StoreDescriptor
	for i, n := range tc.nodes {
		// Ensure nodes are not defined more than once.
		for j := 0; j < i; j++ {
			if tc.nodes[j].id == n.id {
				return compiledTestCase{}, errors.Errorf("duplicate node definition: %d", n.id)
			}
		}

		_ /* nodeDesc */, sds, err := n.toDescriptors()
		if err != nil {
			return compiledTestCase{}, err
		}
		allStores = append(allStores, sds...)
	}

	keyScanner := keysutils.MakePrettyScannerForNamedTables(tableToID, idxToID)
	ranges, err := processSplits(keyScanner, tc.splits, allStores)
	if err != nil {
		return compiledTestCase{}, err
	}

	nodeLocalities := make(map[roachpb.NodeID]roachpb.Locality, len(allStores))
	for _, storeDesc := range allStores {
		nodeDesc := storeDesc.Node
		// Note: We might overwrite the node's localities here. We assume that all
		// the stores for a node have the same node descriptor.
		nodeLocalities[nodeDesc.NodeID] = nodeDesc.Locality
	}
	allLocalities := expandLocalities(nodeLocalities)
	storeResolver := func(r *roachpb.RangeDescriptor) []roachpb.StoreDescriptor {
		replicas := r.Replicas().FilterToDescriptors(func(_ roachpb.ReplicaDescriptor) bool {
			return true
		})
		stores := make([]roachpb.StoreDescriptor, len(replicas))
		for i, rep := range replicas {
			for _, desc := range allStores {
				if rep.StoreID == desc.StoreID {
					stores[i] = desc
					break
				}
			}
		}
		return stores
	}
	nodeChecker := func(nodeID roachpb.NodeID) bool {
		for _, n := range tc.nodes {
			if n.id != int(nodeID) {
				continue
			}
			return !n.dead
		}
		panic(fmt.Sprintf("node %d not found", nodeID))
	}
	cfg, zoneToObject := sysCfgBuilder.build()
	objectToZone := make(map[string]ZoneKey)
	for k, v := range zoneToObject {
		objectToZone[v] = k
	}
	return compiledTestCase{
		iter:           testRangeIter{ranges: ranges},
		cfg:            cfg,
		resolver:       storeResolver,
		checker:        nodeChecker,
		zoneToObject:   zoneToObject,
		objectToZone:   objectToZone,
		nodeLocalities: nodeLocalities,
		allLocalities:  allLocalities,
	}, nil
}

func generateTableZone(t table, tableDesc descpb.TableDescriptor) (*zonepb.ZoneConfig, error) {
	// Create the table's zone config.
	var tableZone *zonepb.ZoneConfig
	if t.zone != nil {
		tableZone = new(zonepb.ZoneConfig)
		*tableZone = t.zone.toZoneConfig()
	}
	// Add subzones for the PK partitions.
	tableZone = addIndexSubzones(t.indexes[0], tableZone, 1 /* id of PK */)
	// Add subzones for all the indexes.
	for i, idx := range t.indexes {
		idxID := i + 1 // index 1 is the PK
		tableZone = addIndexSubzones(idx, tableZone, idxID)
	}
	// Fill in the SubzoneSpans.
	if tableZone != nil {
		var err error
		tableZone.SubzoneSpans, err = sql.GenerateSubzoneSpans(
			nil, uuid.UUID{} /* clusterID */, keys.SystemSQLCodec,
			tabledesc.NewBuilder(&tableDesc).BuildImmutableTable(), tableZone.Subzones, false /* hasNewSubzones */)
		if err != nil {
			return nil, errors.Wrap(err, "error generating subzone spans")
		}
	}
	return tableZone, nil
}

func processSplits(
	keyScanner keysutil.PrettyScanner, splits []split, stores []roachpb.StoreDescriptor,
) ([]roachpb.RangeDescriptor, error) {

	findStore := func(storeID int) (roachpb.StoreDescriptor, error) {
		for _, s := range stores {
			if s.StoreID == roachpb.StoreID(storeID) {
				return s, nil
			}
		}
		return roachpb.StoreDescriptor{}, errors.Errorf("store not found: %d", storeID)
	}

	ranges := make([]roachpb.RangeDescriptor, len(splits))
	var lastKey roachpb.Key
	for i, split := range splits {
		prettyKey := splits[i].key
		startKey, err := keyScanner.Scan(split.key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse key: %s", prettyKey)
		}
		if lastKey.Compare(startKey) != -1 {
			return nil, errors.WithStack(errors.Newf(
				"unexpected lower or duplicate key: %s (prev key: %s)", prettyKey, lastKey))
		}
		lastKey = startKey
		var endKey roachpb.Key
		if i < len(splits)-1 {
			prettyKey := splits[i+1].key
			endKey, err = keyScanner.Scan(prettyKey)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse key: %s", prettyKey)
			}
		} else {
			endKey = roachpb.KeyMax
		}

		rd := roachpb.RangeDescriptor{
			RangeID:       roachpb.RangeID(i + 1), // IDs start at 1
			StartKey:      keys.MustAddr(startKey),
			EndKey:        keys.MustAddr(endKey),
			NextReplicaID: roachpb.ReplicaID(2), // IDs start at 1, so Next starts at 2.
		}

		reps := strings.Split(split.stores, " ")
		for _, rep := range reps {
			if rep == "" {
				continue
			}
			// The format is a numeric storeID, optionally followed by a single character
			// indicating the replica type.
			re := regexp.MustCompile("([0-9]+)(.?)")
			spec := re.FindStringSubmatch(rep)
			if len(spec) != 3 {
				return nil, errors.Errorf("bad replica spec: %s", rep)
			}
			storeID, err := strconv.Atoi(spec[1])
			if err != nil {
				return nil, err
			}
			storeDesc, err := findStore(storeID)
			if err != nil {
				return nil, err
			}
			// Figure out the type of the replica. It's VOTER_FULL by default, unless
			// another option was specified.
			typ := roachpb.VOTER_FULL
			if spec[2] != "" {
				replicaType := spec[2]
				switch replicaType {
				case "v":
					typ = roachpb.VOTER_FULL
				case "i":
					typ = roachpb.VOTER_INCOMING
				case "o":
					typ = roachpb.VOTER_OUTGOING
				case "d":
					typ = roachpb.VOTER_DEMOTING_NON_VOTER
				case "l":
					typ = roachpb.LEARNER
				default:
					return nil, errors.Errorf("bad replica type: %s", replicaType)
				}
			}

			rd.AddReplica(storeDesc.Node.NodeID, storeDesc.StoreID, typ)
		}

		ranges[i] = rd
	}
	return ranges, nil
}

func makeTableDesc(t table, tableID int, dbID int) (descpb.TableDescriptor, error) {
	if err := t.validate(); err != nil {
		return descpb.TableDescriptor{}, err
	}

	desc := descpb.TableDescriptor{
		ID:       descpb.ID(tableID),
		Name:     t.name,
		ParentID: descpb.ID(dbID),
	}

	for i, idx := range t.indexes {
		idxID := i + 1
		desc.Indexes = append(desc.Indexes, idx.toIndexDescriptor(idxID))
	}
	numCols := 0
	for _, idx := range desc.Indexes {
		c := len(idx.KeyColumnIDs)
		if c > numCols {
			numCols = c
		}
	}
	for i := 0; i < numCols; i++ {
		desc.Columns = append(desc.Columns, descpb.ColumnDescriptor{
			Name: fmt.Sprintf("col%d", i),
			ID:   descpb.ColumnID(i),
			Type: types.Int,
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
func addIndexSubzones(idx index, parent *zonepb.ZoneConfig, idxID int) *zonepb.ZoneConfig {
	res := parent

	ensureParent := func() {
		if res != nil {
			return
		}
		// Create a placeholder zone config.
		res = zonepb.NewZoneConfig()
		res.DeleteTableConfig()
	}

	if idx.zone != nil {
		ensureParent()
		res.SetSubzone(zonepb.Subzone{
			IndexID:       uint32(idxID),
			PartitionName: "",
			Config:        idx.zone.toZoneConfig(),
		})
	}

	for _, p := range idx.partitions {
		if p.zone == nil {
			continue
		}
		ensureParent()
		res.SetSubzone(zonepb.Subzone{
			IndexID:       uint32(idxID),
			PartitionName: p.name,
			Config:        p.zone.toZoneConfig(),
		})
	}
	return res
}

// systemConfigBuilder build a system config. Clients will call some setters and then call build().
type systemConfigBuilder struct {
	kv                []roachpb.KeyValue
	defaultZoneConfig *zonepb.ZoneConfig
	zoneToObject      map[ZoneKey]string
}

func makeSystemConfigBuilder() systemConfigBuilder {
	return systemConfigBuilder{zoneToObject: make(map[ZoneKey]string)}
}

func (b *systemConfigBuilder) addZoneToObjectMapping(k ZoneKey, object string) error {
	if s, ok := b.zoneToObject[k]; ok {
		return errors.Errorf("zone %s already mapped to object %s", k, s)
	}
	for k, v := range b.zoneToObject {
		if v == object {
			return errors.Errorf("object %s already mapped to key %s", object, k)
		}
	}
	b.zoneToObject[k] = object
	return nil
}

func (b *systemConfigBuilder) setDefaultZoneConfig(cfg zonepb.ZoneConfig) error {
	b.defaultZoneConfig = &cfg
	return b.addZoneInner("default", keys.RootNamespaceID, cfg)
}

func (b *systemConfigBuilder) addZoneInner(objectName string, id int, cfg zonepb.ZoneConfig) error {
	k := config.MakeZoneKey(config.SystemTenantObjectID(id))
	var v roachpb.Value
	if err := v.SetProto(&cfg); err != nil {
		panic(err)
	}
	b.kv = append(b.kv, roachpb.KeyValue{Key: k, Value: v})
	return b.addZoneToObjectMapping(MakeZoneKey(config.SystemTenantObjectID(id), NoSubzone), objectName)
}

func (b *systemConfigBuilder) addDatabaseZone(name string, id int, cfg zonepb.ZoneConfig) error {
	return b.addZoneInner(name, id, cfg)
}

func (b *systemConfigBuilder) addTableZone(t descpb.TableDescriptor, cfg zonepb.ZoneConfig) error {
	if err := b.addZoneInner(t.Name, int(t.ID), cfg); err != nil {
		return err
	}
	// Figure out the mapping from all the partition names to zone keys.
	for i, subzone := range cfg.Subzones {
		var idx string
		if subzone.IndexID == 1 {
			// Index 1 is the PK.
			idx = t.Name
		} else {
			idx = fmt.Sprintf("%s.%s", t.Name, t.Indexes[subzone.IndexID-1].Name)
		}
		var object string
		if subzone.PartitionName == "" {
			object = idx
		} else {
			object = fmt.Sprintf("%s.%s", idx, subzone.PartitionName)
		}
		if err := b.addZoneToObjectMapping(
			MakeZoneKey(config.SystemTenantObjectID(t.ID), base.SubzoneIDFromIndex(i)), object,
		); err != nil {
			return err
		}
	}
	return nil
}

// build constructs a SystemConfig containing all the information accumulated in the builder.
func (b *systemConfigBuilder) build() (*config.SystemConfig, map[ZoneKey]string) {
	if b.defaultZoneConfig == nil {
		panic("default zone config not set")
	}

	sort.Slice(b.kv, func(i, j int) bool {
		return bytes.Compare(b.kv[i].Key, b.kv[j].Key) < 0
	})

	cfg := config.NewSystemConfig(b.defaultZoneConfig)
	cfg.SystemConfigEntries.Values = b.kv
	return cfg, b.zoneToObject
}

// addTableDesc adds a table descriptor to the SystemConfig.
func (b *systemConfigBuilder) addTableDesc(id int, tableDesc descpb.TableDescriptor) {
	if tableDesc.ParentID == 0 {
		panic(fmt.Sprintf("parent not set for table %q", tableDesc.Name))
	}
	// Write the table to the SystemConfig, in the descriptors table.
	k := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(id))
	desc := &descpb.Descriptor{
		Union: &descpb.Descriptor_Table{
			Table: &tableDesc,
		},
	}
	// Use a bogus timestamp for the descriptor modification time.
	ts := hlc.Timestamp{WallTime: 123}
	descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(desc, ts)
	var v roachpb.Value
	if err := v.SetProto(desc); err != nil {
		panic(err)
	}
	b.kv = append(b.kv, roachpb.KeyValue{Key: k, Value: v})
}

// addTableDesc adds a database descriptor to the SystemConfig.
func (b *systemConfigBuilder) addDBDesc(id int, dbDesc catalog.DatabaseDescriptor) {
	// Write the table to the SystemConfig, in the descriptors table.
	k := catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(id))
	var v roachpb.Value
	if err := v.SetProto(dbDesc.DescriptorProto()); err != nil {
		panic(err)
	}
	b.kv = append(b.kv, roachpb.KeyValue{Key: k, Value: v})
}
