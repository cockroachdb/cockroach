// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func buildTestTable(tableID catid.DescID, tableName string) catalog.MutableTableDescriptor {
	return tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:       tableID,
		Name:     tableName,
		Version:  1,
		ParentID: 242,
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "id", Type: types.Int},
			{ID: 2, Name: "encoding", Type: types.PGVector},
		},
		Families: []descpb.ColumnFamilyDescriptor{
			{
				ID:              0,
				Name:            "primary",
				ColumnNames:     []string{"id", "encoding"},
				ColumnIDs:       []descpb.ColumnID{1, 2},
				DefaultColumnID: 1,
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID:                  1,
			Name:                "pk",
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			StoreColumnIDs:      []descpb.ColumnID{2},
			StoreColumnNames:    []string{"encoding"},
			EncodingType:        catenumpb.PrimaryIndexEncoding,
			Version:             descpb.LatestIndexDescriptorVersion,
			ConstraintID:        1,
		},
		Indexes: []descpb.IndexDescriptor{
			{
				ID:                  2,
				Name:                "vec",
				Type:                idxtype.VECTOR,
				KeyColumnIDs:        []descpb.ColumnID{2},
				KeyColumnNames:      []string{"encoding"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				KeySuffixColumnIDs:  []descpb.ColumnID{1},
				EncodingType:        catenumpb.SecondaryIndexEncoding,
				Version:             descpb.LatestIndexDescriptorVersion,
				VecConfig: vecpb.Config{
					Dims:             2,
					Seed:             342,
					MinPartitionSize: 2,
					MaxPartitionSize: 8,
					BuildBeamSize:    4,
					IsDeterministic:  true,
					RotAlgorithm:     vecpb.RotGivens,
					DistanceMetric:   vecpb.CosineDistance,
				},
			},
		},
		Privileges:       catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
		NextColumnID:     3,
		NextConstraintID: 2,
		NextFamilyID:     1,
		NextIndexID:      3,
		NextMutationID:   1,
		FormatVersion:    descpb.InterleavedFormatVersion,
	}).BuildCreatedMutableTable()
}

func TestVectorManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	internalDB := srv.ApplicationLayer().InternalDB().(descs.DB)
	codec := srv.ApplicationLayer().Codec()
	stopper := srv.Stopper()
	defer stopper.Stop(ctx)

	newDB := dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
		ID:         242,
		Name:       "never_stop",
		Version:    1,
		State:      descpb.DescriptorState_PUBLIC,
		Privileges: catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
	}).BuildCreatedMutable()

	err := internalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		defer func() {
			if err == nil {
				err = txn.KV().Commit(ctx)
			}
		}()
		b := txn.KV().NewBatch()

		err = txn.Descriptors().WriteDescToBatch(ctx, false, newDB, b)
		if err != nil {
			return err
		}
		for i := range 11 {
			err = txn.Descriptors().WriteDescToBatch(
				ctx,
				false,
				buildTestTable(catid.DescID(140+i), "test_table"+strconv.Itoa(i)),
				b,
			)
			if err != nil {
				return err
			}
		}
		return txn.KV().Run(ctx, b)
	})
	require.NoError(t, err)

	settings := srv.ApplicationLayer().ClusterSettings()
	vectorMgr := vecindex.NewManager(ctx, stopper, &settings.SV, codec, internalDB)

	t.Run("test index options", func(t *testing.T) {
		idx, err := vectorMgr.Get(ctx, catid.DescID(140), 2)
		require.NoError(t, err)
		require.Equal(t, 2, idx.Options().MinPartitionSize)
		require.Equal(t, 8, idx.Options().MaxPartitionSize)
		require.Equal(t, 4, idx.Options().BaseBeamSize)
		require.Equal(t, vecpb.CosineDistance, idx.Quantizer().GetDistanceMetric())
	})

	t.Run("test metrics", func(t *testing.T) {
		idx, err := vectorMgr.Get(ctx, catid.DescID(140), 2)
		require.NoError(t, err)
		idx.ForceSplit(ctx, nil, 0, cspann.RootKey, false /* singleStep */)

		metrics := vectorMgr.Metrics().(*vecindex.Metrics)
		require.Equal(t, int64(1), metrics.PendingSplitsMerges.Value())
		idx.ProcessFixups()
		require.Eventually(t, func() bool {
			return metrics.PendingSplitsMerges.Value() == 0
		}, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("test single threaded functionality", func(t *testing.T) {
		// Pull all the indexes.
		for i := range 10 {
			_, err = vectorMgr.Get(ctx, catid.DescID(140+i), 2)
			require.NoError(t, err)
		}
		// Pull an index a second time.
		_, err = vectorMgr.Get(ctx, 142, 2)
		require.NoError(t, err)
		// Attempt to pull a non-existent table.
		_, err = vectorMgr.Get(ctx, 161, 2)
		require.Error(t, err)
		// Attempt to pull the PK from one of the tables.
		_, err = vectorMgr.Get(ctx, 142, 1)
		require.Error(t, err)
		// Attempt to pull a nonexistent index.
		_, err = vectorMgr.Get(ctx, 142, 3)
		require.Error(t, err)
	})

	t.Run("test GetWithDesc functionality", func(t *testing.T) {
		var tableDesc catalog.TableDescriptor
		err := internalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			var err error
			tableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, 141)
			return err
		})
		require.NoError(t, err)
		var idxDesc catalog.Index
		for _, desc := range tableDesc.DeletableNonPrimaryIndexes() {
			if desc.GetID() == 2 {
				idxDesc = desc
				break
			}
		}
		// Pull an index using descriptors.
		_, err = vectorMgr.GetWithDesc(ctx, tableDesc, idxDesc)
		require.NoError(t, err)
		// Pull the index again.
		_, err = vectorMgr.GetWithDesc(ctx, tableDesc, idxDesc)
		require.NoError(t, err)
	})

	t.Run("test multiple threaded functionality", func(t *testing.T) {
		pullDelayer := sync.WaitGroup{}
		pullDelayer.Add(10)
		testingKnobs := vecindex.VecIndexTestingKnobs{
			DuringVecIndexPull: func() {
				pullDelayer.Wait()
			},
			BeforeVecIndexWait: func() {
				pullDelayer.Done()
			},
		}
		vectorMgr.SetTestingKnobs(&testingKnobs)
		wg := sync.WaitGroup{}
		for range 11 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := vectorMgr.Get(ctx, 142, 2)
				require.NoError(t, err)
			}()
		}
		wg.Wait()
		vectorMgr.SetTestingKnobs(nil)
	})

	t.Run("test multiple threaded erroring", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pullDelayer := sync.WaitGroup{}
		pullDelayer.Add(10)
		testingKnobs := vecindex.VecIndexTestingKnobs{
			DuringVecIndexPull: func() {
				pullDelayer.Wait()
			},
			BeforeVecIndexWait: func() {
				pullDelayer.Done()
			},
		}
		errs := make([]error, 11)
		vectorMgr.SetTestingKnobs(&testingKnobs)
		wg := sync.WaitGroup{}
		for i := range 11 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = vectorMgr.Get(ctx, 142, 4)
			}(i)
		}
		wg.Wait()
		require.Error(t, errs[0])
		for i := 1; i < 11; i++ {
			require.Error(t, errs[i])
			require.Equal(t, errs[0], errs[i])
		}
		vectorMgr.SetTestingKnobs(nil)
	})
}
