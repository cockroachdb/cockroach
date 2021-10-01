// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importccl_test

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"

	//"github.com/stretchr/testify/assert"
	//"io"
	"path/filepath"
	"testing"
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	//"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	//"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go-source/local"


)

type testStudent struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
	Id      int64   `parquet:"name=id, type=INT64"`
	Height  float32 `parquet:"name=height, type=FLOAT"`
	Smart     bool    `parquet:"name=smart, type=BOOLEAN"`
	//Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
}

func TestExportParquet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// MB IF USING, ENSURE these look like export csv
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)

	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)

	// Set up dummy accounts table with NULL value
	db.Exec(t, `
		CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);
		INSERT INTO accounts VALUES (1, NULL), (2, 8);
	`)

	//sqlDB.Exec(t, `CREATE TABLE (Name string, Age, Id, Height, Smart) t AS VALUES

	//sqlDB.Exec(t, `EXPORT INTO PARQUET 'nodelocal://0/t/output.parquet' FROM SELECT * FROM t`)

	f, err := os.Open(filepath.Join(dir, "t", "output.parquet"))
	require.NoError(t, err)

	defer f.Close()

	fr, err := local.NewLocalFileReader("nodelocal://0/t/output.parquet")
	if err != nil {
		fmt.Println("Can't open file")
		t.Fatal(err)
	}

	// MB: use parquet package to load in file
	reader.NewParquetReader(fr,nil,3)
}
