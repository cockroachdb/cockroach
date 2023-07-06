// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/partitionccl/partitionccltestcases"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestInitialPartitioningRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	specs := partitionccltestcases.RandomizedPartitioningTestSpecs(rng)

	for _, spec := range specs {
		if !spec.HasScans() {
			continue
		}
		t.Run(spec.Name, func(t *testing.T) {
			RunInitialPartitioningTestCase(t, spec)
		})
	}
}

//go:generate go run partition_test_generator.go --out partition_generated_test.go

// partitioningTest represents a single test case used in the various
// partitioning-related tests.
type partitioningTest struct {
	partitionccltestcases.PartitioningTestSpec

	// The following are all filled in by `parse()`.
	parsed struct {
		parsed bool

		// tableName is `name` but escaped for use in SQL.
		tableName string

		// createStmt is `schema` with a table name of `tableName`
		createStmt string

		// renamePrimaryStmt renames the table's primary index to `primary`
		renamePrimaryStmt string

		// tableDesc is the TableDescriptor created by `createStmt`.
		tableDesc *tabledesc.Mutable

		// zoneConfigStmt contains SQL that effects the zone configs described
		// by `configs`.
		zoneConfigStmts string

		// subzones are the `configs` shorthand Parsed into Subzones.
		subzones []zonepb.Subzone

		// generatedSpans are the `generatedSpans` with @primary replaced with
		// the actual primary key name.
		generatedSpans []string

		// Scans are the `Scans` with @primary replaced with
		// the actual primary key name.
		scans map[string]string
	}
}

// Parse fills in the various fields of `partitioningTest.Parsed`.
func (pt *partitioningTest) parse() error {
	if pt.parsed.parsed {
		return nil
	}

	pt.parsed.tableName = tree.NameStringP(&pt.Name)
	pt.parsed.createStmt = fmt.Sprintf(pt.Schema, pt.parsed.tableName)
	const pkfmt = "ALTER INDEX %s@%s_pkey RENAME TO primary"
	pt.parsed.renamePrimaryStmt = fmt.Sprintf(pkfmt, pt.parsed.tableName, pt.parsed.tableName)

	{
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext()
		stmt, err := parser.ParseOne(pt.parsed.createStmt)
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, pt.parsed.createStmt)
		}
		_, err = parser.ParseOne(pt.parsed.renamePrimaryStmt)
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, pt.parsed.renamePrimaryStmt)
		}
		createTable, ok := stmt.AST.(*tree.CreateTable)
		if !ok {
			return errors.Errorf("expected *tree.CreateTable got %T", stmt)
		}
		st := cluster.MakeTestingClusterSettings()
		parentID, tableID := descpb.ID(bootstrap.TestingUserDescID(0)), descpb.ID(bootstrap.TestingUserDescID(1))
		mutDesc, err := importer.MakeTestingSimpleTableDescriptor(
			ctx, &semaCtx, st, createTable, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, timeutil.Now().UnixNano())
		if err != nil {
			return err
		}
		mutDesc.PrimaryIndex.Name = "primary"
		pt.parsed.tableDesc = mutDesc
		if err := desctestutils.TestingValidateSelf(pt.parsed.tableDesc); err != nil {
			return err
		}
	}

	pt.parsed.generatedSpans = make([]string, len(pt.GeneratedSpans))
	copy(pt.parsed.generatedSpans, pt.GeneratedSpans)
	pt.parsed.scans = make(map[string]string, len(pt.Scans))
	for k, v := range pt.Scans {
		pt.parsed.scans[k] = v
	}

	var zoneConfigStmts bytes.Buffer
	// TODO(dan): Can we run all the zoneConfigStmts in a txn?
	for _, c := range pt.Configs {
		var subzoneShort, constraints string
		configParts := strings.Split(c, `:`)
		switch len(configParts) {
		case 1:
			subzoneShort = configParts[0]
		case 2:
			subzoneShort, constraints = configParts[0], configParts[1]
		default:
			panic(errors.Errorf("unsupported config: %s", c))
		}

		var indexName string
		var subzone zonepb.Subzone
		subzoneParts := strings.Split(subzoneShort, ".")
		switch len(subzoneParts) {
		case 1:
			indexName = subzoneParts[0]
		case 2:
			if subzoneParts[0] == "" {
				indexName = "@primary"
			} else {
				indexName = subzoneParts[0]
			}
			subzone.PartitionName = subzoneParts[1]
		default:
			panic(errors.Errorf("unsupported config: %s", c))
		}
		if !strings.HasPrefix(indexName, "@") {
			panic(errors.Errorf("unsupported config: %s", c))
		}
		idx, err := catalog.MustFindIndexByName(pt.parsed.tableDesc, indexName[1:])
		if err != nil {
			return errors.Wrapf(err, "could not find Index %s", indexName)
		}
		subzone.IndexID = uint32(idx.GetID())
		if len(constraints) > 0 {
			if subzone.PartitionName == "" {
				fmt.Fprintf(&zoneConfigStmts,
					`ALTER INDEX %s@%s CONFIGURE ZONE USING constraints = '[%s]';`,
					pt.parsed.tableName, idx.GetName(), constraints,
				)
			} else {
				fmt.Fprintf(&zoneConfigStmts,
					`ALTER PARTITION %s OF INDEX %s@%s CONFIGURE ZONE USING constraints = '[%s]';`,
					subzone.PartitionName, pt.parsed.tableName, idx.GetName(), constraints,
				)
			}
		}

		var parsedConstraints zonepb.ConstraintsList
		if err := yaml.UnmarshalStrict([]byte("["+constraints+"]"), &parsedConstraints); err != nil {
			return errors.Wrapf(err, "parsing constraints: %s", constraints)
		}
		subzone.Config.Constraints = parsedConstraints.Constraints
		subzone.Config.InheritedConstraints = parsedConstraints.Inherited

		pt.parsed.subzones = append(pt.parsed.subzones, subzone)
	}
	pt.parsed.zoneConfigStmts = zoneConfigStmts.String()
	pt.parsed.parsed = true

	return nil
}

// verifyScansFn returns a closure that runs the test's `Scans` and returns a
// descriptive error if any of them fail. It is not required for `Parse` to have
// been called.
func (pt *partitioningTest) verifyScansFn(
	ctx context.Context, t *testing.T, db *gosql.DB,
) func() error {
	return func() error {
		for where, expectedNodes := range pt.parsed.scans {
			query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s`, tree.NameStringP(&pt.Name), where)
			log.Infof(ctx, "query: %s", query)
			if err := verifyScansOnNode(ctx, t, db, query, expectedNodes); err != nil {
				if log.V(1) {
					log.Errorf(ctx, "scan verification failed: %s", err)
				}
				return err
			}
		}
		return nil
	}
}

func verifyScansOnNode(
	ctx context.Context, t *testing.T, db *gosql.DB, query string, node string,
) error {
	// TODO(dan): This is a stopgap. At some point we should have a syntax for
	// doing this directly (running a query and getting back the nodes it ran on
	// and attributes/localities of those nodes). Users will also want this to
	// be sure their partitioning is working.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to create conn: %v", err)
	}
	sqlDB := sqlutils.MakeSQLRunner(conn)
	defer func() { _ = conn.Close() }()
	sqlDB.Exec(t, fmt.Sprintf(`SET tracing = on; %s; SET tracing = off`, query))
	rows := sqlDB.Query(t, `SELECT concat(tag, ' ', message) FROM [SHOW TRACE FOR SESSION]`)
	defer rows.Close()
	var scansWrongNode []string
	var traceLines []string
	var traceLine gosql.NullString
	for rows.Next() {
		if err := rows.Scan(&traceLine); err != nil {
			t.Fatal(err)
		}
		traceLines = append(traceLines, traceLine.String)
		if strings.Contains(traceLine.String, "read completed") {
			if strings.Contains(traceLine.String, "SystemCon") || strings.Contains(traceLine.String, "NamespaceTab") {
				// Ignore trace lines for the system config range (abbreviated as
				// "SystemCon" in pretty printing of the range descriptor). A read might
				// be performed to the system config range to update the table lease.
				//
				// Also ignore trace lines for the system.namespace table, which is a
				// system table that resides outside the system config range. (abbreviated
				// as "NamespaceTab" in pretty printing of the range descriptor).
				continue
			}
			if !strings.Contains(traceLine.String, node) {
				scansWrongNode = append(scansWrongNode, traceLine.String)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "unexpected error querying traces")
	}
	if len(scansWrongNode) > 0 {
		err := errors.Newf("expected to scan on %s: %s", node, query)
		err = errors.WithDetailf(err, "scans:\n%s", strings.Join(scansWrongNode, "\n"))
		var trace strings.Builder
		for _, traceLine := range traceLines {
			trace.WriteString("\n  ")
			trace.WriteString(traceLine)
		}
		err = errors.WithDetailf(err, "trace:%s", trace.String())
		return err
	}
	return nil
}

func RunInitialPartitioningTestCase(t *testing.T, spec partitionccltestcases.PartitioningTestSpec) {
	if !spec.HasScans() {
		return
	}
	test := partitioningTest{PartitioningTestSpec: spec}
	require.NoError(t, test.parse())

	withPartitioningTestCluster(t, func(ctx context.Context, db *gosql.DB,
		sqlDB *sqlutils.SQLRunner) {
		sqlDB.Exec(t, test.parsed.createStmt)
		sqlDB.Exec(t, test.parsed.renamePrimaryStmt)
		sqlDB.Exec(t, test.parsed.zoneConfigStmts)
		testutils.SucceedsSoon(t, test.verifyScansFn(ctx, t, db))
	})
}

func RunInitialPartitioningTest(t *testing.T, name string) {
	for _, spec := range partitionccltestcases.DeterministicPartitioningTestSpecs() {
		if spec.Name == name {
			RunInitialPartitioningTestCase(t, spec)
			return
		}
	}
	panic(errors.Errorf("deterministic initial partitioning test %q not found", name))
}

func RunRepartitioningTestCase(t *testing.T, spec partitionccltestcases.RepartitioningTestSpec) {
	told := partitioningTest{PartitioningTestSpec: spec.Old}
	require.NoError(t, told.parse())
	tnew := partitioningTest{PartitioningTestSpec: spec.New}
	require.NoError(t, tnew.parse())

	withPartitioningTestCluster(t, func(ctx context.Context, db *gosql.DB,
		sqlDB *sqlutils.SQLRunner) {
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS data`)
		sqlDB.Exec(t, `CREATE DATABASE data`)
		sqlDB.Exec(t, told.parsed.createStmt)
		sqlDB.Exec(t, told.parsed.renamePrimaryStmt)
		sqlDB.Exec(t, told.parsed.zoneConfigStmts)
		testutils.SucceedsSoon(t, told.verifyScansFn(ctx, t, db))

		sqlDB.Exec(t, fmt.Sprintf("ALTER TABLE %s RENAME TO %s",
			told.parsed.tableName, tnew.parsed.tableName))

		testIndex, err := catalog.MustFindIndexByName(tnew.parsed.tableDesc, spec.Index)
		require.NoError(t, err)

		var repartition bytes.Buffer
		if testIndex.GetID() == tnew.parsed.tableDesc.GetPrimaryIndexID() {
			fmt.Fprintf(&repartition, `ALTER TABLE %s `, tnew.parsed.tableName)
		} else {
			fmt.Fprintf(&repartition, `ALTER INDEX %s@%s `, tnew.parsed.tableName, spec.Index)
		}
		if testIndex.PartitioningColumnCount() == 0 {
			repartition.WriteString(`PARTITION BY NOTHING`)
		} else {
			if err := sql.ShowCreatePartitioning(
				&tree.DatumAlloc{}, keys.SystemSQLCodec, tnew.parsed.tableDesc, testIndex,
				testIndex.GetPartitioning(), &repartition, 0 /* indent */, 0, /* colOffset */
				false, /* redactableValues */
			); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		sqlDB.Exec(t, repartition.String())

		// Verify that repartitioning removes zone configs for partitions that
		// have been removed.
		newPartitionNames := map[string]struct{}{}
		for _, index := range tnew.parsed.tableDesc.NonDropIndexes() {
			_ = index.GetPartitioning().ForEachPartitionName(func(name string) error {
				newPartitionNames[name] = struct{}{}
				return nil
			})
		}
		for _, row := range sqlDB.QueryStr(
			t, "SELECT partition_name FROM crdb_internal.zones WHERE partition_name IS NOT NULL") {
			partitionName := row[0]
			if _, ok := newPartitionNames[partitionName]; !ok {
				t.Errorf("zone config for removed partition %q exists after repartitioning", partitionName)
			}
		}

		// NB: Not all old zone configurations are removed. This statement will
		// overwrite any with the same name and the repartitioning removes any
		// for partitions that no longer exist, but there could still be some
		// sitting around (e.g., when a repartitioning preserves a partition but
		// does not apply a new zone config). This is fine.
		sqlDB.Exec(t, tnew.parsed.zoneConfigStmts)
		testutils.SucceedsSoon(t, tnew.verifyScansFn(ctx, t, db))
	})
}

func RunRepartitioningTest(t *testing.T, name string) {
	for _, spec := range partitionccltestcases.AllRepartitioningTestSpecs() {
		if spec.Name == name {
			RunRepartitioningTestCase(t, spec)
			return
		}
	}
	panic(errors.Errorf("repartitioning test %q not found", name))
}

func RunGenerateSubzoneSpansTestCase(
	t *testing.T, spec partitionccltestcases.PartitioningTestSpec,
) {
	if !spec.HasGeneratedSpans() {
		// The randomized partition tests don't have generatedSpans, and
		// wouldn't be very interesting to test.
		return
	}
	test := partitioningTest{PartitioningTestSpec: spec}
	require.NoError(t, test.parse())
	clusterID := uuid.MakeV4()
	hasNewSubzones := false
	spans, err := sql.GenerateSubzoneSpans(
		cluster.NoSettings, clusterID, keys.SystemSQLCodec, test.parsed.tableDesc, test.parsed.subzones, hasNewSubzones)
	if err != nil {
		t.Fatalf("generating subzone spans: %+v", err)
	}

	var actual []string
	for _, span := range spans {
		subzone := test.parsed.subzones[span.SubzoneIndex]
		idx, err := catalog.MustFindIndexByID(test.parsed.tableDesc, descpb.IndexID(subzone.IndexID))
		if err != nil {
			t.Fatalf("could not find Index with ID %d: %+v", subzone.IndexID, err)
		}

		directions := []encoding.Direction{encoding.Ascending /* Index ID */}
		for i := 0; i < idx.NumKeyColumns(); i++ {
			cd := idx.GetKeyColumnDirection(i)
			ed, err := catalogkeys.IndexColumnEncodingDirection(cd)
			if err != nil {
				t.Fatal(err)
			}
			directions = append(directions, ed)
		}

		var subzoneShort string
		if len(subzone.PartitionName) > 0 {
			subzoneShort = "." + subzone.PartitionName
		} else {
			subzoneShort = "@" + idx.GetName()
		}

		// Verify that we're always doing the space savings when we can.
		var buf redact.StringBuilder
		var buf2 redact.StringBuilder
		if span.Key.PrefixEnd().Equal(span.EndKey) {
			encoding.PrettyPrintValue(&buf, directions, span.Key, "/")
			encoding.PrettyPrintValue(&buf2, directions, span.EndKey, "/")
			t.Errorf("endKey should be omitted when equal to key.PrefixEnd [%s, %s)",
				buf.String(),
				buf2.String())
		}
		if len(span.EndKey) == 0 {
			span.EndKey = span.Key.PrefixEnd()
		}

		// TODO(dan): Check that spans are sorted.
		encoding.PrettyPrintValue(&buf, directions, span.Key, "/")
		encoding.PrettyPrintValue(&buf2, directions, span.EndKey, "/")

		actual = append(actual, fmt.Sprintf("%s %s-%s", subzoneShort,
			buf.String(),
			buf2.String()))
	}

	if len(actual) != len(test.parsed.generatedSpans) {
		t.Fatalf("got \n    %v\n expected \n    %v", actual, spec.GeneratedSpans)
	}
	for i := range actual {
		if expected := strings.TrimSpace(test.parsed.generatedSpans[i]); actual[i] != expected {
			t.Errorf("%d: got [%s] expected [%s]", i, actual[i], expected)
		}
	}
}

func RunGenerateSubzoneSpansTest(t *testing.T, name string) {
	for _, spec := range partitionccltestcases.DeterministicPartitioningTestSpecs() {
		if spec.Name == name {
			RunGenerateSubzoneSpansTestCase(t, spec)
			return
		}
	}
	panic(errors.Errorf("repartitioning test %q not found", name))
}

func withPartitioningTestCluster(
	t *testing.T, fn func(ctx context.Context, db *gosql.DB, sqlDB *sqlutils.SQLRunner),
) {
	// These tests are too slow to run under nightly race stress.
	skip.UnderStressRace(t)

	ctx := context.Background()
	cfg := zonepb.DefaultZoneConfig()
	cfg.NumReplicas = proto.Int32(1)

	tsArgs := func(attr string) base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable LBS because when the scan is happening at the rate it's happening
					// below, it's possible that one of the system ranges trigger a split.
					DisableLoadBasedSplitting: true,
				},
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride: &cfg,
				},
			},
			ScanInterval: 100 * time.Millisecond,
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{attr}}},
			},
			UseDatabase:       "data",
			DefaultTestTenant: base.TestTenantDisabled,
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs("n1"),
		1: tsArgs("n2"),
		2: tsArgs("n3"),
	}}
	tc := testcluster.StartTestCluster(t, 3, tcArgs)
	defer func() {
		tc.Stopper().Stop(context.Background())
	}()

	db := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE data`)
	fn(ctx, db, sqlDB)
}

func TestPrimaryKeyChangeZoneConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Write a table with some partitions into the database,
	// and change its primary key.
	for _, stmt := range []string{
		`CREATE DATABASE t`,
		`USE t`,
		`CREATE TABLE t (
  x INT PRIMARY KEY,
  y INT NOT NULL,
  z INT,
  w INT,
  INDEX i1 (z),
  INDEX i2 (w),
  FAMILY (x, y, z, w)
)`,
		`ALTER INDEX t@i1 PARTITION BY LIST (z) (
  PARTITION p1 VALUES IN (1)
)`,
		`ALTER INDEX t@i2 PARTITION BY LIST (w) (
  PARTITION p2 VALUES IN (3)
)`,
		`ALTER PARTITION p1 OF INDEX t@i1 CONFIGURE ZONE USING gc.ttlseconds = 15210`,
		`ALTER PARTITION p2 OF INDEX t@i2 CONFIGURE ZONE USING gc.ttlseconds = 15213`,
		`ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (y)`,
	} {
		if _, err := sqlDB.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	// Get the zone config corresponding to the table.
	table := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "t")
	kv, err := kvDB.Get(ctx, config.MakeZoneKey(keys.SystemSQLCodec, table.GetID()))
	if err != nil {
		t.Fatal(err)
	}
	var zone zonepb.ZoneConfig
	if err := kv.ValueProto(&zone); err != nil {
		t.Fatal(err)
	}

	// Our subzones should be spans prefixed with dropped copy of i1,
	// dropped copy of i2, new copy of i1, and new copy of i2.
	// These have ID's 2, 3, 8 and 10 respectively.
	expectedSpans := []roachpb.Key{
		table.IndexSpan(keys.SystemSQLCodec, 2 /* indexID */).Key,
		table.IndexSpan(keys.SystemSQLCodec, 3 /* indexID */).Key,
		table.IndexSpan(keys.SystemSQLCodec, 8 /* indexID */).Key,
		table.IndexSpan(keys.SystemSQLCodec, 10 /* indexID */).Key,
	}
	if len(zone.SubzoneSpans) != len(expectedSpans) {
		t.Fatalf("expected subzones to have length %d", len(expectedSpans))
	}

	// Subzone spans have the table prefix omitted.
	prefix := keys.SystemSQLCodec.TablePrefix(uint32(table.GetID()))
	for i := range expectedSpans {
		// Subzone spans have the table prefix omitted.
		expected := bytes.TrimPrefix(expectedSpans[i], prefix)
		if !bytes.HasPrefix(zone.SubzoneSpans[i].Key, expected) {
			t.Fatalf(
				"expected span to have prefix %s but found %s",
				expected,
				zone.SubzoneSpans[i].Key,
			)
		}
	}
}

func TestRemovePartitioningExpiredLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase:       "d",
		DefaultTestTenant: base.TestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)

	// Create a partitioned table and index.
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
		PARTITION	p1 VALUES IN (1)
	)`)
	sqlDB.Exec(t, `CREATE INDEX i ON t (a) PARTITION BY RANGE (a) (
		PARTITION p34 VALUES FROM (3) TO (4)
	)`)
	sqlDB.Exec(t, `ALTER PARTITION p1 OF TABLE t CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER PARTITION p34 OF INDEX t@i CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`)

	// Remove the enterprise license.
	defer utilccl.TestingDisableEnterprise()()

	const partitionErr = "use of partitions requires an enterprise license"
	const zoneErr = "use of replication zones on indexes or partitions requires an enterprise license"
	expectErr := func(q string, expErr string) {
		t.Helper()
		sqlDB.ExpectErr(t, expErr, q)
	}

	// Partitions and zone configs cannot be modified without a valid license.
	expectErr(`ALTER TABLE t PARTITION BY LIST (a) (PARTITION p2 VALUES IN (2))`, partitionErr)
	expectErr(`ALTER INDEX t@i PARTITION BY RANGE (a) (PARTITION p45 VALUES FROM (4) TO (5))`, partitionErr)
	expectErr(`ALTER PARTITION p1 OF TABLE t CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER PARTITION p34 OF INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)

	// But they can be removed.
	sqlDB.Exec(t, `ALTER TABLE t PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t@i PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t@t_pkey CONFIGURE ZONE DISCARD`)
	sqlDB.Exec(t, `ALTER INDEX t@i CONFIGURE ZONE DISCARD`)

	// Once removed, they cannot be added back.
	expectErr(`ALTER TABLE t PARTITION BY LIST (a) (PARTITION p2 VALUES IN (2))`, partitionErr)
	expectErr(`ALTER INDEX t@i PARTITION BY RANGE (a) (PARTITION p45 VALUES FROM (4) TO (5))`, partitionErr)
	expectErr(`ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)
}

// Test that dropping an enum value fails if there's a concurrent index drop
// for an index partitioned by that enum value. The reason is that it
// would be bad if we rolled back the dropping of the index.
func TestDropEnumValueWithConcurrentPartitionedIndexDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var s serverutils.TestServerInterface
	var sqlDB *gosql.DB

	// Use the dropCh to block any DROP INDEX job until the channel is closed.
	dropCh := make(chan chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s, sqlDB, _ = serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					var isDropJob bool
					if err := sqlDB.QueryRow(`
SELECT count(*) > 0
  FROM [SHOW JOB $1]
 WHERE description LIKE 'DROP INDEX %'
`, jobID).Scan(&isDropJob); err != nil {
						return err
					}
					if !isDropJob {
						return nil
					}
					ch := make(chan struct{})
					select {
					case dropCh <- ch:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case <-ch:
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	defer cancel()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Set up the table to have an index which is partitioned by the enum value
	// we're going to drop.
	for _, stmt := range []string{
		`CREATE TYPE t AS ENUM ('a', 'b', 'c')`,
		`CREATE TABLE tbl (
    i INT8, k t,
    PRIMARY KEY (i, k),
    INDEX idx (k)
        PARTITION BY RANGE (k)
            (PARTITION a VALUES FROM ('a') TO ('b'))
)`,
	} {
		tdb.Exec(t, stmt)
	}
	// Run a transaction to drop the index and the enum value.
	errCh := make(chan error)
	go func() {
		errCh <- crdb.ExecuteTx(ctx, sqlDB, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec("drop index tbl@idx;"); err != nil {
				return err
			}
			_, err := tx.Exec("alter type t drop value 'a';")
			return err
		})
	}()
	// Wait until the dropping of the enum value has finished.
	ch := <-dropCh
	testutils.SucceedsSoon(t, func() error {
		var done bool
		tdb.QueryRow(t, `
SELECT bool_and(done)
  FROM (
        SELECT status NOT IN `+jobs.NonTerminalStatusTupleString+` AS done
          FROM [SHOW JOBS]
         WHERE job_type = 'TYPEDESC SCHEMA CHANGE'
       );`).
			Scan(&done)
		if done {
			return nil
		}
		return errors.Errorf("not done")
	})
	// Allow the dropping of the index to proceed.
	close(ch)
	// Ensure we got the right error.
	require.Regexp(t,
		`could not remove enum value "a" as it is being used in the partitioning of index tbl@idx`,
		<-errCh)
}
