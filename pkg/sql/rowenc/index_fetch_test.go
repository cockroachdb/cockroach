// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowenc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v2"
)

func TestInitIndexFetchSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109390),
	})
	defer srv.Stopper().Stop(context.Background())
	codec := srv.ApplicationLayer().Codec()

	if _, err := db.Exec(`CREATE DATABASE testdb; USE testdb;`); err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(
		t, datapathutils.TestDataPath(t, "index-fetch"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec":
				if _, err := db.Exec(d.Input); err != nil {
					d.Fatalf(t, "%+v", err)
				}
				return ""

			case "index-fetch":
				var params struct {
					Table   string
					Index   string
					Columns []string
				}
				if err := yaml.UnmarshalStrict([]byte(d.Input), &params); err != nil {
					d.Fatalf(t, "failed to parse index-fetch params: %v", err)
				}
				table := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "testdb", params.Table)
				index, err := catalog.MustFindIndexByName(table, params.Index)
				if err != nil {
					d.Fatalf(t, "%+v", err)
				}

				fetchColumnIDs := make([]descpb.ColumnID, len(params.Columns))
				for i, name := range params.Columns {
					col, err := catalog.MustFindColumnByName(table, name)
					if err != nil {
						d.Fatalf(t, "%+v", err)
					}
					fetchColumnIDs[i] = col.GetID()
				}

				var spec fetchpb.IndexFetchSpec
				if err := rowenc.InitIndexFetchSpec(&spec, codec, table, index, fetchColumnIDs); err != nil {
					d.Fatalf(t, "%+v", err)
				}
				res, err := json.MarshalIndent(&spec, "", "  ")
				if err != nil {
					d.Fatalf(t, "%+v", err)
				}
				return string(res)

			default:
				d.Fatalf(t, "unknown command '%s'", d.Cmd)
				return ""
			}
		},
	)
}
