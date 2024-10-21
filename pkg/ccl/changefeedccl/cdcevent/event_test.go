// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcevent

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL,
  e status,
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c)
)`)

	tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	mainFamily := mustGetFamily(t, tableDesc, 0)
	cFamily := mustGetFamily(t, tableDesc, 1)

	for _, tc := range []struct {
		family          *descpb.ColumnFamilyDescriptor
		includeVirtual  bool
		keyOnly         bool
		expectedKeyCols []string
		expectedColumns []string
		expectedUDTCols []string
	}{
		{
			family:          mainFamily,
			includeVirtual:  false,
			keyOnly:         false,
			expectedKeyCols: []string{"b", "a"},
			expectedColumns: []string{"a", "b", "e"},
			expectedUDTCols: []string{"e"},
		},
		{
			family:         mainFamily,
			includeVirtual: true,
			keyOnly:        false,

			expectedKeyCols: []string{"b", "a"},
			expectedColumns: []string{"a", "b", "d", "e"},
			expectedUDTCols: []string{"e"},
		},
		{
			family:          mainFamily,
			includeVirtual:  false,
			keyOnly:         true,
			expectedKeyCols: []string{"b", "a"},
			expectedColumns: []string{"a", "b"},
		},
		{
			family:          mainFamily,
			includeVirtual:  true,
			keyOnly:         true,
			expectedKeyCols: []string{"b", "a"},
			expectedColumns: []string{"a", "b"},
		},
		{
			family:          cFamily,
			includeVirtual:  false,
			keyOnly:         false,
			expectedKeyCols: []string{"b", "a"},
			expectedColumns: []string{"c"},
		},
		{
			family:          cFamily,
			includeVirtual:  true,
			keyOnly:         false,
			expectedKeyCols: []string{"b", "a"},
			expectedColumns: []string{"c", "d"},
		},
	} {
		t.Run(fmt.Sprintf("%s/includeVirtual=%t/keyOnly=%t", tc.family.Name, tc.includeVirtual, tc.keyOnly), func(t *testing.T) {
			ed, err := NewEventDescriptor(tableDesc, tc.family, tc.includeVirtual, tc.keyOnly, s.Clock().Now())
			require.NoError(t, err)

			// Verify Metadata information for event descriptor.
			require.Equal(t, tableDesc.GetID(), ed.TableID)
			require.Equal(t, tableDesc.GetName(), ed.TableName)
			require.Equal(t, tc.family.Name, ed.FamilyName)
			require.True(t, ed.HasOtherFamilies)

			// Verify primary key and family columns are as expected.
			r := Row{EventDescriptor: ed}
			require.Equal(t, expectResultColumns(t, tableDesc, tc.expectedKeyCols), slurpColumns(t, r.ForEachKeyColumn()))
			require.Equal(t, expectResultColumns(t, tableDesc, tc.expectedColumns), slurpColumns(t, r.ForEachColumn()))
			require.Equal(t, expectResultColumns(t, tableDesc, tc.expectedUDTCols), slurpColumns(t, r.ForEachUDTColumn()))
		})
	}
}

// TestEventDescriptorWithSchemaChanges validates that complex schema changes
// will produce valid event descriptors.
func TestEventDescriptorWithSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var hookEnabled atomic.Bool
	var validationFn func(stage string) error
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				BeforeStage: func(p scplan.Plan, stageIdx int) error {
					if !hookEnabled.Load() {
						return nil
					}
					return validationFn(fmt.Sprintf("%s/stage=%d", p.Params.ExecutionPhase, stageIdx))
				},
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING NOT NULL,
  d STRING AS (concat(b, c)) VIRTUAL,
  e status NOT NULL,
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c)
)`)

	for _, tc := range []struct {
		schemaChange string
		// Each new primary index generated during the test will pause at each stage
		// of the schema change. Then it will try to see if the current index matches
		// a expected state, if it doesn't it will validate the against the next expected
		// state assuming a transition has occurred (i.e. the primary index has been
		// replaced).
		expectedKeyCols [][]string
		expectedColumns [][]string
		expectedUDTCols [][]string
	}{
		{
			// Replace the primary index with one that uses hash sharding, we will see
			// the following states:
			// 1) the original primary key
			// 2) the new primary key, since the new primary key is hash sharded,
			//		we are going to have a new key column (crdb_internal_a_b_shard_16)
			//		that is hashing the primary key columns.
			schemaChange:    "ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS(b, a) USING HASH",
			expectedKeyCols: [][]string{{"b", "a"}, {"crdb_internal_a_b_shard_16", "b", "a"}},
			expectedColumns: [][]string{{"a", "b", "e"}, {"a", "b", "e"}},
			expectedUDTCols: [][]string{{"e"}, {"e"}},
		},
		{
			// Replace the primary index with a new one with a hash. This will lead
			// to 3 indexes observed:
			// 1) The original from the previous test, including the hash sharding
			//	  columns.
			// 2) An index without the hash sharding column and just c + e
			// 3) An index with c + e and a new hash sharding column.
			schemaChange:    "ALTER TABLE foo ALTER PRIMARY KEY USING COLUMNS(c, e) USING HASH",
			expectedKeyCols: [][]string{{"crdb_internal_a_b_shard_16", "b", "a"}, {"c", "e"}, {"crdb_internal_c_e_shard_16", "c", "e"}},
			expectedColumns: [][]string{{"a", "b", "e"}, {"a", "b", "e"}, {"a", "b", "e"}},
			expectedUDTCols: [][]string{{"e"}, {"e"}, {"e"}},
		},
		{
			// We are going to execute a mix of add, drop and alter primary key operations,
			// this will result in 3 primary indexes being swapped.
			// 1) The first primary index key will be the same as previous test
			// 2) The second primary key will use the column "a", without a hash
			//    sharding column since that needs to be created next.
			// 3) The final primary key will "a" and have hash sharding on it.
			schemaChange:    "ALTER TABLE foo ADD COLUMN j INT DEFAULT 32, DROP COLUMN d, DROP COLUMN crdb_internal_a_b_shard_16, DROP COLUMN b, ALTER PRIMARY KEY USING COLUMNS(a) USING HASH",
			expectedKeyCols: [][]string{{"crdb_internal_c_e_shard_16", "c", "e"}, {"a"}, {"crdb_internal_a_shard_16", "a"}},
			expectedColumns: [][]string{{"a", "b", "e"}, {"a", "b", "e"}, {"a", "e", "j"}},
			expectedUDTCols: [][]string{{"e"}, {"e"}, {"e"}},
		},
	} {
		t.Run(tc.schemaChange, func(t *testing.T) {
			validateCh := make(chan string)
			validationComplete := make(chan struct{})
			validationFn = func(stage string) error {
				validateCh <- stage
				<-validationComplete
				return nil
			}
			hookEnabled.Store(true)
			defer hookEnabled.Store(false)

			// Execute the schema change
			grp := ctxgroup.WithContext(context.Background())
			grp.GoCtx(func(ctx context.Context) error {
				_, err := sqlDB.DB.ExecContext(ctx, tc.schemaChange)
				hookEnabled.Store(false)
				close(validateCh)
				close(validationComplete)
				return err
			})
			currentIdx := 0
			for stageName := range validateCh {
				t.Run(stageName, func(t *testing.T) {
					defer func() {
						validationComplete <- struct{}{}
						if t.Failed() {
							hookEnabled.Swap(false)
						}
					}()
					tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
					mainFamily := mustGetFamily(t, tableDesc, 0)
					ed, err := NewEventDescriptor(tableDesc, mainFamily, false, false, s.Clock().Now())
					require.NoError(t, err)

					// Verify Metadata information for event descriptor.
					require.Equal(t, tableDesc.GetID(), ed.TableID)
					require.Equal(t, tableDesc.GetName(), ed.TableName)
					require.True(t, ed.HasOtherFamilies)

					// Verify primary key and family columns are as expected.
					r := Row{EventDescriptor: ed}

					compareFunc := func(required bool, expected, actual interface{}) bool {
						if !assert.ObjectsAreEqual(expected, actual) {
							if required {
								require.Equal(t, expected, actual)
							}
							return false
						}
						return true
					}
					for i := 0; i < 2 && currentIdx < len(tc.expectedKeyCols); i, currentIdx = i+1, currentIdx+1 {
						required := i > 0 || (currentIdx+1) >= len(tc.expectedKeyCols)
						if !compareFunc(required, expectResultColumnsWithFamily(t, tableDesc, tc.expectedKeyCols[currentIdx], mainFamily), slurpColumns(t, r.ForEachKeyColumn())) {
							continue
						}
						if !compareFunc(required, expectResultColumnsWithFamily(t, tableDesc, tc.expectedColumns[currentIdx], mainFamily), slurpColumns(t, r.ForEachColumn())) {
							continue
						}
						if !compareFunc(required, expectResultColumnsWithFamily(t, tableDesc, tc.expectedUDTCols[currentIdx], mainFamily), slurpColumns(t, r.ForEachUDTColumn())) {
							continue
						}
						break
					}
				})
			}

			require.NoError(t, grp.Wait())

		})
	}
}

func TestEventDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL,
  e status DEFAULT 'inactive',
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c)
)`)

	tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), tableDesc)
	defer cleanup()

	type decodeExpectation struct {
		expectUnwatchedErr bool

		// current value expectations.
		deleted   bool
		keyValues []string
		allValues []string

		// previous value expectations.
		prevDeleted   bool
		prevAllValues []string
	}

	for _, tc := range []struct {
		testName          string
		familyName        string // Must be set if targetType ChangefeedTargetSpecification_COLUMN_FAMILY
		includeVirtual    bool
		keyOnly           bool
		actions           []string
		expectMainFamily  []decodeExpectation
		expectOnlyCFamily []decodeExpectation
	}{
		{
			testName:   "main/primary_cols",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'first test')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"first test", "1"},
					allValues:   []string{"1", "first test", "inactive"},
					prevDeleted: true,
				},
			},
		},
		{
			testName:       "main/primary_cols_with_virtual",
			familyName:     "main",
			actions:        []string{"INSERT INTO foo (a, b) VALUES (1, 'second test')"},
			includeVirtual: true,
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"second test", "1"},
					allValues:   []string{"1", "second test", "NULL", "inactive"},
					prevDeleted: true,
				},
			},
		},
		{
			testName:   "main/all_cols",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b, c, e) VALUES (1, 'third test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"third test", "1"},
					allValues:   []string{"1", "third test", "open"},
					prevDeleted: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
		},
		{
			testName:   "c_family/all_cols",
			familyName: "only_c",
			actions:    []string{"INSERT INTO foo (a, b, c, e) VALUES (1, 'fourth test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					keyValues:   []string{"fourth test", "1"},
					allValues:   []string{"c value"},
					prevDeleted: true,
				},
			},
		},
		{
			testName: "all_families/all_cols",
			actions:  []string{"INSERT INTO foo (a, b, c, e) VALUES (1, 'fifth test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"fifth test", "1"},
					allValues:   []string{"1", "fifth test", "open"},
					prevDeleted: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					keyValues:   []string{"fifth test", "1"},
					allValues:   []string{"c value"},
					prevDeleted: true,
				},
			},
		},
		{
			// This test verifies that altering an enum works (i.e. we successfully refresh type information).
			testName:   "main/alter_enum",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b, e) VALUES (1, '6th test',  'open')",
				"ALTER TYPE status ADD value 'review'",
				"UPDATE foo SET e = 'review' WHERE a=1 and b='6th test'",
			},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"6th test", "1"},
					allValues:   []string{"1", "6th test", "open"},
					prevDeleted: true,
				},
				{
					keyValues:     []string{"6th test", "1"},
					allValues:     []string{"1", "6th test", "review"},
					prevAllValues: []string{"1", "6th test", "open"},
				},
			},
		},
		{
			testName:   "main/update_and_delete",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (1, '7th test')",
				"UPDATE foo SET e = 'open' WHERE a = 1 and b = '7th test'",
				"DELETE FROM foo WHERE a = 1 and b = '7th test'",
			},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"7th test", "1"},
					allValues:   []string{"1", "7th test", "inactive"},
					prevDeleted: true,
				},
				{
					keyValues:     []string{"7th test", "1"},
					allValues:     []string{"1", "7th test", "open"},
					prevAllValues: []string{"1", "7th test", "inactive"},
				},
				{
					deleted:       true,
					keyValues:     []string{"7th test", "1"},
					prevAllValues: []string{"1", "7th test", "open"},
				},
			},
		},
		{
			testName:       "main/key_only_cols_with_virtual",
			familyName:     "main",
			keyOnly:        true,
			includeVirtual: true,
			actions:        []string{"INSERT INTO foo (a, b, c, e) VALUES (1, '8th test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"8th test", "1"},
					allValues:   []string{"1", "8th test"},
					prevDeleted: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
		},
		{
			testName:   "main/key_only_cols",
			familyName: "main",
			keyOnly:    true,
			actions:    []string{"INSERT INTO foo (a, b, c, e) VALUES (1, '9th test', 'c value', 'open')"},
			expectMainFamily: []decodeExpectation{
				{
					keyValues:   []string{"9th test", "1"},
					allValues:   []string{"1", "9th test"},
					prevDeleted: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			targetType := jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			if tc.familyName != "" {
				targetType = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			}

			for _, action := range tc.actions {
				sqlDB.Exec(t, action)
			}

			targets := changefeedbase.Targets{}
			targets.Add(changefeedbase.Target{
				Type:       targetType,
				TableID:    tableDesc.GetID(),
				FamilyName: tc.familyName,
			})
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			ctx := context.Background()
			decoder, err := NewEventDecoder(ctx, &execCfg, targets, tc.includeVirtual, tc.keyOnly)
			require.NoError(t, err)
			expectedEvents := len(tc.expectMainFamily) + len(tc.expectOnlyCFamily)
			for i := 0; i < expectedEvents; i++ {
				v := popRow(t)

				eventFamilyID, err := TestingGetFamilyIDFromKey(decoder, v.Key, v.Timestamp())
				require.NoError(t, err)

				var expect decodeExpectation
				if eventFamilyID == 0 {
					expect, tc.expectMainFamily = tc.expectMainFamily[0], tc.expectMainFamily[1:]
				} else {
					expect, tc.expectOnlyCFamily = tc.expectOnlyCFamily[0], tc.expectOnlyCFamily[1:]
				}
				updatedRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.Value}, CurrentRow, v.Timestamp(), tc.keyOnly)

				if expect.expectUnwatchedErr {
					require.ErrorIs(t, err, ErrUnwatchedFamily)
					continue
				}

				require.NoError(t, err)
				require.True(t, updatedRow.IsInitialized())
				if expect.deleted {
					require.True(t, updatedRow.IsDeleted())
				} else {
					require.Equal(t, expect.keyValues, slurpDatums(t, updatedRow.ForEachKeyColumn()))
					require.Equal(t, expect.allValues, slurpDatums(t, updatedRow.ForEachColumn()))
				}

				prevRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.PrevValue}, PrevRow, v.Timestamp(), tc.keyOnly)
				require.NoError(t, err)

				// prevRow always has key columns initialized.
				require.Equal(t, expect.keyValues, slurpDatums(t, prevRow.ForEachKeyColumn()))

				if expect.prevDeleted {
					require.True(t, prevRow.IsDeleted())
				} else {
					require.Equal(t, expect.prevAllValues, slurpDatums(t, prevRow.ForEachColumn()))
				}
			}
		})
	}
}

func TestEventColumnOrderingWithSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)
	skip.UnderStress(t)

	ctx := context.Background()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	sqlDB := sqlutils.MakeSQLRunner(db)
	// Use alter column type to force column reordering.
	sqlDB.Exec(t, `SET enable_experimental_alter_column_type_general = true`)
	// TODO(spilchen): force the legacy schema changer. When run with the DSC,
	// the ordering changes in the column family. This needs to be revisited in 133040.
	sqlDB.Exec(t, `SET use_declarative_schema_changer = 'off'`)

	type decodeExpectation struct {
		expectUnwatchedErr bool

		keyValues []string
		allValues []string

		refreshDescriptor bool
	}

	for _, tc := range []struct {
		testName         string
		familyName       string // Must be set if targetType ChangefeedTargetSpecification_COLUMN_FAMILY
		includeVirtual   bool
		actions          []string
		expectMainFamily []decodeExpectation
		expectECFamily   []decodeExpectation
	}{
		{
			testName:   "main/main_cols",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (i,j,a,b) VALUES (0,1,'2002-05-02','b0')",
				"ALTER TABLE foo ALTER COLUMN a SET DATA TYPE DATE USING a::DATE",
				"INSERT INTO foo (i,j,a,b) VALUES (1,2,'2021-01-01','b1')",
			},
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"1", "0"},
					allValues: []string{"0", "1", "2002-05-02", "b0"},
				},
				{
					keyValues: []string{"1", "0"},
					allValues: []string{"0", "1", "2002-05-02", "b0"},
				},
				{
					keyValues: []string{"1", "0"},
					allValues: []string{"0", "1", "2002-05-02", "b0"},
				},
				{
					keyValues: []string{"2", "1"},
					allValues: []string{"1", "2", "2021-01-01", "b1"},
				},
			},
		},
		{
			testName:   "ec/ec_cols",
			familyName: "ec",
			actions: []string{
				"INSERT INTO foo (i,j,e,c) VALUES (2,3,'e2','2024-08-02')",
				"ALTER TABLE foo ALTER COLUMN c SET DATA TYPE DATE USING c::DATE",
				"INSERT INTO foo (i,j,e,c) VALUES (3,4,'e3','2024-05-21')",
			},
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
				{
					expectUnwatchedErr: true,
				},
			},
			expectECFamily: []decodeExpectation{
				{
					keyValues: []string{"3", "2"},
					allValues: []string{"2024-08-02", "e2"},
				},
				{
					keyValues: []string{"3", "2"},
					allValues: []string{"2024-08-02", "e2"},
				},
				{
					keyValues: []string{"3", "2"},
					allValues: []string{"2024-08-02", "e2"},
				},
				{
					keyValues: []string{"4", "3"},
					allValues: []string{"2024-05-21", "e3"},
				},
			},
		},
		{
			testName:   "ec/ec_cols_with_virtual",
			familyName: "ec",
			actions: []string{
				"INSERT INTO foo (i,j,e,c) VALUES (4,5,'e4','2012-11-06')",
				"ALTER TABLE foo ALTER COLUMN c SET DATA TYPE DATE USING c::DATE",
				"INSERT INTO foo (i,j,e,c) VALUES (5,6,'e5','2014-05-06')",
			},
			includeVirtual: true,
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
				{
					expectUnwatchedErr: true,
				},
			},
			expectECFamily: []decodeExpectation{
				{
					keyValues: []string{"5", "4"},
					allValues: []string{"2012-11-06", "NULL", "e4"},
				},
				{
					keyValues:         []string{"5", "4"},
					allValues:         []string{"2012-11-06", "NULL", "e4"},
					refreshDescriptor: true,
				},
				{
					keyValues: []string{"5", "4"},
					allValues: []string{"2012-11-06", "NULL", "e4"},
				},
				{
					keyValues: []string{"6", "5"},
					allValues: []string{"2014-05-06", "NULL", "e5"},
				},
			},
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlDB.Exec(t, `
				CREATE TABLE foo (
					i INT,
					j INT,
					a STRING,
					b STRING,
					c STRING,
					d STRING AS (concat(e, '.')) VIRTUAL,
					e STRING,
					PRIMARY KEY(j,i),
					FAMILY main (i,j,a,b),
					FAMILY ec (e,c)
			)`)

			tableDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
			popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), tableDesc)
			defer cleanup()

			targetType := jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			if tc.familyName != "" {
				targetType = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			}

			for _, action := range tc.actions {
				sqlDB.Exec(t, action)
			}

			targets := changefeedbase.Targets{}
			targets.Add(changefeedbase.Target{
				Type:       targetType,
				TableID:    tableDesc.GetID(),
				FamilyName: tc.familyName,
			})
			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			ctx := context.Background()
			decoder, err := NewEventDecoder(ctx, &execCfg, targets, tc.includeVirtual, false)
			require.NoError(t, err)

			expectedEvents := len(tc.expectMainFamily) + len(tc.expectECFamily)
			for i := 0; i < expectedEvents; i++ {
				v := popRow(t)

				eventFamilyID, err := TestingGetFamilyIDFromKey(decoder, v.Key, v.Timestamp())
				require.NoError(t, err)

				var expect decodeExpectation
				if eventFamilyID == 0 {
					expect, tc.expectMainFamily = tc.expectMainFamily[0], tc.expectMainFamily[1:]
				} else {
					expect, tc.expectECFamily = tc.expectECFamily[0], tc.expectECFamily[1:]
				}
				updatedRow, err := decoder.DecodeKV(
					ctx, roachpb.KeyValue{Key: v.Key, Value: v.Value}, CurrentRow, v.Timestamp(), false)

				if expect.expectUnwatchedErr {
					require.ErrorIs(t, err, ErrUnwatchedFamily)
					continue
				}

				require.NoError(t, err)
				require.True(t, updatedRow.IsInitialized())

				require.Equal(t, expect.keyValues, slurpDatums(t, updatedRow.ForEachKeyColumn()), "row %d", i)
				require.Equal(t, expect.allValues, slurpDatums(t, updatedRow.ForEachColumn()), "row %d", i)
			}
			sqlDB.Exec(t, `DROP TABLE foo`)
		})
	}
}

func mustGetFamily(
	t *testing.T, desc catalog.TableDescriptor, familyID descpb.FamilyID,
) *descpb.ColumnFamilyDescriptor {
	t.Helper()
	f, err := catalog.MustFindFamilyByID(desc, familyID)
	require.NoError(t, err)
	return f
}

func expectResultColumns(
	t *testing.T, desc catalog.TableDescriptor, colNames []string,
) (res []ResultColumn) {
	t.Helper()

	// Map the column names to their expected ordinal positions.
	//
	// The ordinal positions in EventDescriptor.keyCols, EventDescriptor.valueCols,
	// and EventDescriptor.udtCols (which are indexes into a rowenc.EncDatumRow)
	// are calculated in the following manner: Start with catalog.TableDescriptor.PublicColumns()
	// and enumerate any (i) primary key columns and (ii) columns in a specified family.
	// All remaining columns are filtered out.
	//
	// This test helper function generates ordinal positions in the same manner,
	// except it uses colNames instead of a column family descriptor when filtering columns.
	colNamesSet := make(map[string]int)
	for _, colName := range colNames {
		colNamesSet[colName] = -1
	}
	ord := 0
	for _, col := range desc.PublicColumns() {
		colName := string(col.ColName())
		if _, ok := colNamesSet[colName]; ok {
			switch {
			case col.IsVirtual():
				colNamesSet[colName] = virtualColOrd
			default:
				colNamesSet[colName] = ord
				ord++
			}
		} else if desc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(col.GetID()) {
			// Primary index column that's not part of colNames.
			ord++
		}
	}

	for _, colName := range colNames {
		col, err := catalog.MustFindColumnByName(desc, colName)
		require.NoError(t, err)
		res = append(res, ResultColumn{
			ResultColumn: colinfo.ResultColumn{
				Name:           col.GetName(),
				Typ:            col.GetType(),
				TableID:        desc.GetID(),
				PGAttributeNum: uint32(col.GetPGAttributeNum()),
			},
			Computed:  col.IsComputed(),
			ord:       colNamesSet[colName],
			sqlString: col.ColumnDesc().SQLStringNotHumanReadable(),
		})
	}
	return res
}

func expectResultColumnsWithFamily(
	t *testing.T,
	desc catalog.TableDescriptor,
	colNames []string,
	family *descpb.ColumnFamilyDescriptor,
) (res []ResultColumn) {
	t.Helper()

	// Map the column names to their expected ordinal positions.
	//
	// The ordinal positions in EventDescriptor.keyCols, EventDescriptor.valueCols,
	// and EventDescriptor.udtCols (which are indexes into a rowenc.EncDatumRow)
	// are calculated in the following manner: Start with catalog.TableDescriptor.PublicColumns()
	// and enumerate any (i) primary key columns and (ii) columns in a specified family.
	// All remaining columns are filtered out.
	//
	// This test helper function generates ordinal positions in the same manner,
	// except it uses colNames instead of a column family descriptor when filtering columns.
	colNamesSet := make(map[string]int)
	for _, colName := range family.ColumnNames {
		colNamesSet[colName] = -1
	}
	ord := 0
	for _, col := range desc.PublicColumns() {
		colName := string(col.ColName())
		if _, ok := colNamesSet[colName]; ok {
			switch {
			case col.IsVirtual():
				colNamesSet[colName] = virtualColOrd
			default:
				colNamesSet[colName] = ord
				ord++
			}
		} else if desc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(col.GetID()) {
			// Primary index column that's not part of column family, but
			// will still be indexed.
			colNamesSet[colName] = ord
			ord++
		}
	}

	for _, colName := range colNames {
		col, err := catalog.MustFindColumnByName(desc, colName)
		require.NoError(t, err)
		res = append(res, ResultColumn{
			ResultColumn: colinfo.ResultColumn{
				Name:           col.GetName(),
				Typ:            col.GetType(),
				TableID:        desc.GetID(),
				PGAttributeNum: uint32(col.GetPGAttributeNum()),
			},
			Computed:  col.IsComputed(),
			ord:       colNamesSet[colName],
			sqlString: col.ColumnDesc().SQLStringNotHumanReadable(),
		})
	}
	return res
}

func slurpColumns(t *testing.T, it Iterator) (res []ResultColumn) {
	t.Helper()
	require.NoError(t,
		it.Col(func(col ResultColumn) error {
			res = append(res, col)
			return nil
		}))
	return res
}

func slurpDatums(t *testing.T, it Iterator) (res []string) {
	t.Helper()
	require.NoError(t,
		it.Datum(func(d tree.Datum, col ResultColumn) error {
			res = append(res, tree.AsStringWithFlags(d, tree.FmtExport))
			return nil
		}))
	return res
}

func TestMakeRowFromTuple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	i := tree.NewDInt(1234)
	f := tree.NewDFloat(12.34)
	s := tree.NewDString("testing")
	typ := types.MakeTuple([]*types.T{types.Int, types.Float, types.String})
	unlabeledTuple := tree.NewDTuple(typ, i, f, s)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	rowFromUnlabeledTuple := MakeRowFromTuple(context.Background(), &evalCtx, unlabeledTuple)
	expectedCols := []struct {
		name        string
		typ         *types.T
		valAsString string
	}{
		{name: "col1", typ: types.Int, valAsString: "1234"},
		{name: "col2", typ: types.Float, valAsString: "12.34"},
		{name: "col3", typ: types.String, valAsString: "testing"},
	}

	remainingCols := expectedCols

	require.NoError(t, rowFromUnlabeledTuple.ForEachColumn().Datum(func(d tree.Datum, col ResultColumn) error {
		current := remainingCols[0]
		remainingCols = remainingCols[1:]
		require.Equal(t, current.name, col.Name)
		require.Equal(t, current.typ, col.Typ)
		require.Equal(t, current.valAsString, tree.AsStringWithFlags(d, tree.FmtExport))
		return nil
	}))

	require.Empty(t, remainingCols)

	typ.InternalType.TupleLabels = []string{"a", "b", "c"}
	labeledTuple := tree.NewDTuple(typ, i, f, s)

	expectedCols[0].name = "a"
	expectedCols[1].name = "b"
	expectedCols[2].name = "c"

	remainingCols = expectedCols

	rowFromLabeledTuple := MakeRowFromTuple(context.Background(), &evalCtx, labeledTuple)

	require.NoError(t, rowFromLabeledTuple.ForEachColumn().Datum(func(d tree.Datum, col ResultColumn) error {
		current := remainingCols[0]
		remainingCols = remainingCols[1:]
		require.Equal(t, current.name, col.Name)
		require.Equal(t, current.typ, col.Typ)
		require.Equal(t, current.valAsString, tree.AsStringWithFlags(d, tree.FmtExport))
		return nil
	}))
}

func BenchmarkEventDecoder(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	b.StopTimer()
	srv, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(b, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	sqlDB.Exec(b, `
CREATE TABLE foo (
  a INT,
  b STRING,
  c STRING,
  PRIMARY KEY (b, a)
)`)

	tableDesc := cdctest.GetHydratedTableDescriptor(b, s.ExecutorConfig(), "foo")
	popRow, cleanup := cdctest.MakeRangeFeedValueReader(b, s.ExecutorConfig(), tableDesc)
	sqlDB.Exec(b, "INSERT INTO foo VALUES (5, 'hello', 'world')")
	v := popRow(b)
	cleanup()

	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		TableID: tableDesc.GetID(),
	})
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ctx := context.Background()
	decoder, err := NewEventDecoder(ctx, &execCfg, targets, false, false)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := decoder.DecodeKV(
			ctx, roachpb.KeyValue{Key: v.Key, Value: v.Value}, CurrentRow, v.Timestamp(), false)
		if err != nil {
			b.Fatal(err)
		}
	}
}
