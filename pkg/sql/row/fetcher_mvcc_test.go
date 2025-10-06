// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package row_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func slurpUserDataKVs(t testing.TB, e storage.Engine, codec keys.SQLCodec) []roachpb.KeyValue {
	t.Helper()

	// Scan meta keys directly from engine. We put this in a retry loop
	// because the application of all of a transactions committed writes
	// is not always synchronous with it committing.
	var kvs []roachpb.KeyValue
	testutils.SucceedsSoon(t, func() error {
		kvs = nil
		it, err := e.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: codec.TenantEndKey()})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		for it.SeekGE(storage.MVCCKey{Key: bootstrap.TestingUserTableDataMin(codec)}); ; it.NextKey() {
			ok, err := it.Valid()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if !it.UnsafeKey().IsValue() {
				return errors.Errorf("found intent key %v", it.UnsafeKey())
			}
			mvccValue, err := storage.DecodeMVCCValueAndErr(it.Value())
			if err != nil {
				t.Fatal(err)
			}
			value := mvccValue.Value
			value.Timestamp = it.UnsafeKey().Timestamp
			kv := roachpb.KeyValue{Key: it.UnsafeKey().Key.Clone(), Value: value}
			kvs = append(kvs, kv)
		}
		return nil
	})
	return kvs
}

func TestRowFetcherMVCCMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec := s.Codec()
	store, _ := srv.StorageLayer().GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `USE d`)
	sqlDB.Exec(t, `CREATE TABLE parent (
		a STRING PRIMARY KEY, b STRING, c STRING, d STRING,
		FAMILY (a, b, c), FAMILY (d)
	)`)
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, `d`, `parent`)
	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&spec, codec, desc, desc.GetPrimaryIndex(), desc.PublicColumnIDs(),
	); err != nil {
		t.Fatal(err)
	}
	var rf row.Fetcher
	if err := rf.Init(
		ctx,
		row.FetcherInitArgs{
			WillUseKVProvider: true,
			Alloc:             &tree.DatumAlloc{},
			Spec:              &spec,
		},
	); err != nil {
		t.Fatal(err)
	}
	type rowWithMVCCMetadata struct {
		PrimaryKey      []string
		RowIsDeleted    bool
		RowLastModified string
	}
	kvsToRows := func(kvs []roachpb.KeyValue) []rowWithMVCCMetadata {
		t.Helper()
		for _, kv := range kvs {
			log.Infof(ctx, "%v %v %v", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
		}

		if err := rf.ConsumeKVProvider(ctx, &row.KVProvider{KVs: kvs}); err != nil {
			t.Fatal(err)
		}
		var rows []rowWithMVCCMetadata
		for {
			datums, err := rf.NextRowDecoded(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if datums == nil {
				break
			}
			row := rowWithMVCCMetadata{
				RowIsDeleted:    rf.RowIsDeleted(),
				RowLastModified: eval.TimestampToDecimalDatum(rf.RowLastModified()).String(),
			}
			for _, datum := range datums {
				if datum == tree.DNull {
					row.PrimaryKey = append(row.PrimaryKey, `NULL`)
				} else {
					row.PrimaryKey = append(row.PrimaryKey, string(*datum.(*tree.DString)))
				}
			}
			rows = append(rows, row)
		}
		return rows
	}

	var ts1 string
	sqlDB.QueryRow(t, `BEGIN;
		INSERT INTO parent VALUES ('1', 'a', 'a', 'a'), ('2', 'b', 'b', 'b');
		SELECT cluster_logical_timestamp();
	END;`).Scan(&ts1)

	if actual, expected := kvsToRows(slurpUserDataKVs(t, store.TODOEngine(), codec)), []rowWithMVCCMetadata{
		{[]string{`1`, `a`, `a`, `a`}, false, ts1},
		{[]string{`2`, `b`, `b`, `b`}, false, ts1},
	}; !reflect.DeepEqual(expected, actual) {
		t.Errorf(`expected %v got %v`, expected, actual)
	}

	var ts2 string
	sqlDB.QueryRow(t, `BEGIN;
		UPDATE parent SET b = NULL, c = NULL, d = NULL WHERE a = '1';
		UPDATE parent SET d = NULL WHERE a = '2';
		SELECT cluster_logical_timestamp();
	END;`).Scan(&ts2)

	if actual, expected := kvsToRows(slurpUserDataKVs(t, store.TODOEngine(), codec)), []rowWithMVCCMetadata{
		{[]string{`1`, `NULL`, `NULL`, `NULL`}, false, ts2},
		{[]string{`2`, `b`, `b`, `NULL`}, false, ts2},
	}; !reflect.DeepEqual(expected, actual) {
		t.Errorf(`expected %v got %v`, expected, actual)
	}

	var ts3 string
	sqlDB.QueryRow(t, `BEGIN;
		DELETE FROM parent WHERE a = '1';
		SELECT cluster_logical_timestamp();
	END;`).Scan(&ts3)
	if actual, expected := kvsToRows(slurpUserDataKVs(t, store.TODOEngine(), codec)), []rowWithMVCCMetadata{
		{[]string{`1`, `NULL`, `NULL`, `NULL`}, true, ts3},
		{[]string{`2`, `b`, `b`, `NULL`}, false, ts2},
	}; !reflect.DeepEqual(expected, actual) {
		t.Errorf(`expected %v got %v`, expected, actual)
	}
}
