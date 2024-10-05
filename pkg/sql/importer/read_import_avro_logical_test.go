// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// createTableLogical
func createTableLogicalTypes(tableName string, avroCols []avroLogicalInfo) string {
	var createStmt strings.Builder
	createStmt.WriteString(fmt.Sprintf("CREATE TABLE %s (", tableName))
	comma := ""
	for _, col := range avroCols {
		createStmt.WriteString(comma)
		createStmt.WriteString(fmt.Sprintf("%s %s", col.name, col.crdbType))
		if !col.nullable {
			createStmt.WriteString(" NOT NULL")
		}
		comma = ", "
	}
	createStmt.WriteString(")")
	return createStmt.String()
}

// avroLogicalInfo contains metadata on a CRL column we'll write to an avro file.
type avroLogicalInfo struct {
	// name is the name of the column
	name string

	// crdbType is the crdb column type
	crdbType string

	// avroType is the logical avro type corresponding to the crdb type
	avroType string

	// nullable is set to false if the column is created with the NOT NULL constraint
	nullable bool
}

func logicalEncoder(datum tree.Datum, avroType string) (ans interface{}, err error) {
	if datum.ResolvedType().Family() == types.UnknownFamily {
		return nil, nil
	}
	switch datum.ResolvedType().Family() {

	case types.DateFamily:
		ans = datum.(*tree.DDate).Date.UnixEpochDays()
	case types.TimestampFamily:
		ans = datum.(*tree.DTimestamp).Time
	case types.TimeFamily:
		// CRDB's TimeFamily stores time as Microseconds since midnight, while the
		// goAvro package creates columns of logical type Time using go's
		// time.Duration or int types. I decided to use ints as I couldn't
		// figure out how to get the test to pass when I fed the goAvro writer
		// time.duration. I'm either incompetent or there's a bug in goAvro's code.
		// Source:  https://github.com/linkedin/goavro/blob/master/logical_type.go

		t := *datum.(*tree.DTime)

		if avroType == "int.time-millis" {
			// for int.time-millis type, goAvro expects the milliseconds since midnight as an int32
			ans = int32(int64(t) / 1000)
		} else {
			// for long.time-micros, goAvro expects microseconds since midnight
			ans = int64(t)
		}

	default:
		return nil, errors.New("type not supported")
	}
	return ans, err
}

// logicalAvroExec holds information for creating an avro file from CRL datums
type logicalAvroExec struct {
	// name is the name of the avroFile
	name string

	// encoder maps a CRL datum to a native go type which gets fed into goAvro's writer
	encoder func(datum tree.Datum, avroType string) (interface{}, error)

	// stringify is true if all datums will get encoded as strings.
	stringify bool
}

// createAvroFromDatums creates an avro binary.
func (e logicalAvroExec) createAvroDataFromDatums(
	t *testing.T, avroCols []avroLogicalInfo, datums []tree.Datums,
) (avroData string, err error) {

	var (
		// avroField defines the avro file's schema
		avroField []map[string]interface{}

		// avroRows contains the data to write to the avro file
		avroRows []map[string]interface{}

		// avroTypes is a helper struct that contains the non-unioned avro type
		avroTypes []string
	)

	// Iterate through each avroCol to define the avro file's schema
	for j := range avroCols {
		avroTypes = append(avroTypes, avroCols[j].avroType)
		if e.stringify {
			avroTypes[j] = "string"
		}
		var avroUnionType interface{} = avroTypes[j]
		if avroCols[j].nullable {
			avroUnionType = &[]string{"null", avroTypes[j]}
		}
		avroField = append(avroField, map[string]interface{}{
			"name": avroCols[j].name,
			"type": avroUnionType,
		})
	}

	for i, row := range datums {
		avroRows = append(avroRows, map[string]interface{}{})
		for j, val := range row {
			colName := avroCols[j].name
			val, err := e.encoder(val, avroTypes[j])
			if err != nil {
				return "", err
			}
			avroRows[i][colName] = val

			// An avro value gets encoded differently if the column's type is unioned
			// with null. For more details, checkout the doc string above
			// nativeToDatum in read_import_avro.go.
			if avroCols[j].nullable && val != nil {
				avroRows[i][colName] = map[string]interface{}{avroTypes[j]: val}
			}
		}
	}
	return importer.CreateAvroData(t, e.name, avroField, avroRows), nil
}

// roundtripStringer pretty prints the datum's value as string, allowing the
// parser in certain decoders to work.
func roundtripStringer(d tree.Datum) string {
	fmtCtx := tree.NewFmtCtx(tree.FmtBareStrings)
	d.Format(fmtCtx)
	return fmtCtx.CloseAndGetString()
}

// TestImportAvroLogicalType tests that an avro file with logical avro types
// populates a CRL table identically to an avro file with the same data encoded
// as strings. Here's the high level approach of this test:
//
// 1. Create and randomly populate a CRL table with data types that logical avro types
// should import into
// 2. Extract the CRL datums from the table to create an Avro file with a logical
// encoding of the table, and another Avro file with a stringed encoding
// 3. Import both Avro tables and test they are identical to the original table.
func TestImportAvroLogicalTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dbName := "log"
	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: dbName,
	})

	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))

	// avroCols helps define a CRL table schema we'll use to create a CRL table
	// and consequently write to an avro file. Each element in avroCols
	// corresponds to a column in the CRL table we'll create.
	avroCols := []avroLogicalInfo{
		{
			name:     "dt",
			avroType: "int.date",
			crdbType: "DATE",
			nullable: true,
		},
		{
			name:     "time_micros",
			avroType: "long.time-micros",
			crdbType: "TIME",
			nullable: true,
		},
		{
			name:     "time_millis",
			avroType: "int.time-millis",
			crdbType: "TIME(3)",
			nullable: true,
		},
		{
			name:     "ts_micros",
			avroType: "long.timestamp-micros",
			crdbType: "TIMESTAMP",
			nullable: true,
		},
		{
			name:     "ts_millis",
			avroType: "long.timestamp-millis",
			crdbType: "TIMESTAMP(3)",
			nullable: true,
		},
		{
			name:     "dt_not_null",
			avroType: "int.date",
			crdbType: "DATE",
			nullable: false,
		},
		{
			name:     "time_micros_not_null",
			avroType: "long.time-micros",
			crdbType: "TIME",
			nullable: false,
		},
		{
			name:     "time_millis_not_null",
			avroType: "int.time-millis",
			crdbType: "TIME(3)",
			nullable: false,
		},
		{
			name:     "ts_micros_not_null",
			avroType: "long.timestamp-micros",
			crdbType: "TIMESTAMP",
			nullable: false,
		},
		{
			name:     "ts_millis_not_null",
			avroType: "long.timestamp-millis",
			crdbType: "TIMESTAMP(3)",
			nullable: false,
		},
	}
	origTableName := "orig"
	tableCreateStmt := createTableLogicalTypes(origTableName, avroCols)
	sqlDB.Exec(t, tableCreateStmt)

	// AVRO files store dates as int32, so to make this test roundtrippable,
	// ensure the random data generator adds dates that can get encoded as an
	// int32
	for _, dtCol := range []string{"dt", "dt_not_null"} {
		sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT no_out_of_bounds_dates_%s CHECK (%s BETWEEN '%v' AND '%v')`, origTableName, dtCol, dtCol, pgdate.LowDate, pgdate.HighDate))
	}

	// IMPORT INTO only works if timestamps are within certain bounds; thus,
	// only generate data within these bounds.
	// TODO(MB): figure out these exact bounds, as tree.MinSupportTime & tree.
	// MaxSuppportedTime make the test fail
	maxTime := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	minTime := time.Date(-2000, time.January, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	for _, tsCol := range []string{"ts_micros", "ts_micros_not_null", "ts_millis", "ts_millis_not_null"} {
		sqlDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT no_out_of_bounds_ts_%s CHECK (%s BETWEEN '%v' AND '%v')`, origTableName, tsCol, tsCol, minTime, maxTime))
	}

	// Try at most 5 times to populate this table, else fail.
	rng, _ := randutil.NewTestRand()
	success := false
	for i := 1; i <= 5; i++ {
		numRowsInserted, err := randgen.PopulateTableWithRandData(rng, db, origTableName, 30, nil)
		require.NoError(t, err)
		if numRowsInserted > 5 {
			success = true
			break
		}
	}
	require.Equal(t, true, success, "failed to generate random data after 5 attempts")

	ie := srv.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig).InternalDB.Executor()
	datums, _, err := ie.QueryBufferedExWithCols(
		ctx,
		"",
		nil,
		sessiondata.InternalExecutorOverride{
			User:     username.NodeUserName(),
			Database: "log"},
		fmt.Sprintf("SELECT * FROM %s", origTableName))
	require.NoError(t, err, "failed to pull datums from table")

	execParams := []logicalAvroExec{{
		name: "stringed",
		encoder: func(datum tree.Datum, avroTypes string) (interface{}, error) {
			val := roundtripStringer(datum)
			if val == "NULL" {
				return nil, nil
			}
			return val, nil
		},
		stringify: true},
		{
			name:      "logical",
			encoder:   logicalEncoder,
			stringify: false,
		}}

	var data string
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer httpSrv.Close()
	truth := sqlDB.QueryStr(t, fmt.Sprintf("SELECT * FROM %s", origTableName))

	for _, e := range execParams {
		data, err = e.createAvroDataFromDatums(t, avroCols, datums)
		require.NoError(t, err, "failed to convert datums into an avro binary")

		newCreateStmt := strings.Replace(tableCreateStmt, origTableName, e.name, 1)
		sqlDB.Exec(t, newCreateStmt)

		sqlDB.Exec(t, fmt.Sprintf(`IMPORT INTO %s AVRO DATA ($1)`, e.name), httpSrv.URL)
		sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s", e.name), truth)
	}
}
