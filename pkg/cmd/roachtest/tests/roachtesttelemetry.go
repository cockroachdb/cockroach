// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	projectID     = "cockroach-dev-inf"
	bucketName    = "cockroach-snowflake-backup"
	EncodedKeyTag = "e_key"
	SFUser        = "SFUSER"
	SFPassword    = "SFPASSWORD"
	THRESHHOLD    = 0.5
)

func registerRoachtestTelemetry(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "roachtesttelemetry",
		Owner:           registry.OwnerSQLQueries,
		Timeout:         time.Hour * 1,
		RequiresLicense: true,
		Tags:            nil,
		Cluster:         r.MakeClusterSpec(1),
		NativeLibs:      registry.LibGEOS,
		Run:             runRoachtestTelemetry,
	})
}

// makes connection to snowflake and returns the connection
func getConnect(schema string) (*sql.DB, context.Context, context.CancelFunc) {

	username, ok := os.LookupEnv(SFUser)
	if !ok {
		fmt.Printf("%s not set\n", SFUser)
	}
	password, ok := os.LookupEnv(SFPassword)
	if !ok {
		fmt.Printf("%s not set\n", SFPassword)
	}
	dsn, err := sf.DSN(&sf.Config{
		Account:   "qy03275.us-east-1",
		Database:  "DATA_ENG",
		Password:  password,
		Schema:    schema,
		User:      username,
		Warehouse: "COMPUTE_WH",
	})
	if err != nil {
		log.Fatalf("Could not  contruct connection string %v", err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("Could not open sql connection %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return db, ctx, cancel

}

// execute a snowflake query
func executeQuery(db *sql.DB, ctx context.Context, query string) *sql.Rows {

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("could not execute query %v", err)

	}
	return rows

}

// writes data in rows to gcs "filename". We write to gcs so we can import data from gcs cvs files to cockroachdb
func writeRowsGCS(ctx context.Context, filename string, rows *sql.Rows) string {

	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
		return ""
	}

	wc := client.Bucket(bucketName).Object(filename).NewWriter(ctx)
	wc.ContentType = "text/plain"

	cols, _ := rows.Columns()
	cName := strings.Join(cols[:], ",")
	// will be used to read data while iterating rows
	colPointers := make([]interface{}, len(cols))
	colContainer := make([]string, len(cols))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	// iterate rows and write to gcs file
	for rows.Next() {
		_ = rows.Scan(colPointers...)
		rowData := strings.Join(colContainer[:], ",")
		if _, err := wc.Write([]byte(rowData + "\n")); err != nil {
			log.Fatalf("Failed to write to file: %v", err)
			return ""
		}

	}
	if err := wc.Close(); err != nil {
		log.Fatalf("createFile: unable to close bucket %q, file %q: %v", bucketName, "aaa", err)
		return ""
	}

	return cName

}

func chooseRandomQuery() (string, map[string][]string) {

	rndNumber := rand.Float64()
	var query string
	// Randomly select between a non join or join query. THRESHOLD specifies bias based on how representive join
	// query bank cardinality is of join workloads relative to non join representation
	if rndNumber <= THRESHHOLD {
		fmt.Println("NO JOIN")
		query = "select a.statement,replace(regexp_replace(lower(replace(statement,'\"','')),concat('[\\\\\\S]*\\\\\\.[\\\\\\S]*\\\\\\.',lower(b.table_name)),lower(b.table_name)),'..','') statement_2,ifnull(a.database_name,'') database_name,a.CID,a.TID,a.schema_name,b.table_name,b.table_signature,b.cockroachdb_schema cockroachdb_schema from (select * from (select * from (select *,rank() over (partition by statement,schema_name,database_name,cid,tid order by run_date desc) as rnk from (select * from DATA_ENG.TEST_CONFIG.QUERYBANK where not contains(lower(statement),'join'))) where rnk=1)  sample (1 rows))  a inner join (select * from (select *,rank() over (partition by table_signature,schema_name,database_name,cid,tid  order by run_date desc) as rnk from DATA_ENG.TEST_CONFIG.TABLE_SIGNATURES) where rnk=1) b on a.SCHEMA_NAME=b.SCHEMA_NAME and contains(lower(statement),lower(table_name)) "
	} else {
		query = "select a.statement,replace(regexp_replace(lower(replace(statement,'\"','')),concat('[\\\\\\S]*\\\\\\.[\\\\\\S]*\\\\\\.',lower(b.table_name)),lower(b.table_name)),'..','') statement_2,ifnull(a.database_name,'') database_name,a.CID,a.TID,a.schema_name,b.table_name,b.table_signature,b.cockroachdb_schema cockroachdb_schema from (select * from (select * from (select *,rank() over (partition by statement,schema_name,database_name,cid,tid order by run_date desc) as rnk from (select * from DATA_ENG.TEST_CONFIG.QUERYBANK_SELECT )) where rnk=1)  sample (1 rows))  a inner join (select * from (select *,rank() over (partition by table_signature,schema_name,database_name,cid,tid  order by run_date desc) as rnk from DATA_ENG.TEST_CONFIG.TABLE_SIGNATURES) where rnk=1) b on a.SCHEMA_NAME=b.SCHEMA_NAME and contains(lower(statement),lower(table_name)) "

	}

	db, ctx, cancel := getConnect("test_config")
	rows := executeQuery(db, ctx, query)

	signatures := make(map[string][]string)
	var finalStmt string
	finalStmt = ""
	for rows.Next() {
		var stmt string
		var stmtpostproc string
		var dbname string
		var cid string
		var tid string
		var schemaname string
		var tablename string
		var tablesignature string
		var tableschema string
		err := rows.Scan(&stmt, &stmtpostproc, &dbname, &cid, &tid, &schemaname, &tablename, &tablesignature, &tableschema)

		if finalStmt == "" {
			finalStmt = strings.ReplaceAll(strings.ToLower(stmt), string('"'), string(""))
		}
		d := strings.ToLower(tablename)
		m1 := regexp.MustCompile(`[\S]*\.[\S]*\.` + d)
		finalStmt = m1.ReplaceAllString(finalStmt, d) // standardize statement

		if err != nil {
			fmt.Println(err)
			break
		}

		signatures[tablename] = []string{tablesignature, tableschema}

	}

	cancel()

	return finalStmt, signatures

}

// Create random data in snowflake using signatures that reflect customer workloads. Write snowflake tables
// to GCS and return a mapping of table name to gcs filepath and schema
func createRandomDataSnowflake(signatures map[string][]string) map[string][]string {

	db, ctx, cancel := getConnect("workload_data")

	schemaMap := make(map[string][]string)
	var cName string
	for key, value := range signatures {
		// create random data in snowflake using signature
		executeQuery(db, ctx, value[0])

		// get data from snowflake and write to GCS
		query := "select * from " + key
		rows := executeQuery(db, ctx, query)
		filename := "roachtest/" + fmt.Sprint(time.Now().Unix()) + "_" + key
		cName = writeRowsGCS(ctx, filename, rows)
		// map "table name" to file path of gcs data, column names of data and schema of table
		schemaMap[key] = []string{filename, cName, value[1]}
	}

	cancel()

	return schemaMap

}
func runRoachtestTelemetry(ctx context.Context, t test.Test, c cluster.Cluster) {

	testRunTime := time.Now()
	y, m, d := testRunTime.Date()

	logPath := filepath.Join(t.ArtifactsDir(), fmt.Sprintf("%d_%d_%d_%d.log", y, m, d, testRunTime.Unix()))
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("could not create %d_%d_%d_%d.log,: %v", y, m, d, testRunTime.Unix(), err)
	}
	defer log.Close()

	logTest := func(logStr string, logType string) {
		stmt := strings.TrimSpace(logStr)
		if stmt == "" {
			return
		}
		fmt.Fprint(log, logType+":"+stmt)
		fmt.Fprint(log, "\n")

	}

	// Set up the nodes in the cluster.

	t.L().Printf("uploading cockroach binary to nodes")
	c.Put(ctx, t.Cockroach(), "./cockroach")
	defer func() {
		t.L().Printf("wiping nodes")
		c.Wipe(ctx)
	}()

	// Start CockroachDB.
	t.L().Printf("starting cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
	defer func() {
		t.L().Printf("stopping cockroach")
		c.Stop(ctx, t.L(), option.DefaultStopOpts())
	}()

	// Open a connection to CockroachDB.
	t.L().Printf("connecting to cockroach node 1")
	conn := c.Conn(ctx, t.L(), 1)
	defer func() {
		t.L().Printf("closing connection to cockroach node 1")
		conn.Close()
	}()

	finalStmt, signatures := chooseRandomQuery()
	schemaMaps := createRandomDataSnowflake(signatures)

	// Create tables.

	for key, value := range schemaMaps {
		t.L().Printf("creating table " + key)
		fmt.Println(value[2])
		if _, err := conn.Exec(value[2]); err != nil {
			t.L().Printf("error while creating table: %v", err)
			return
		}
	}

	// Load tables with initial data.

	importStr := ""
	for key, value := range schemaMaps {
		t.L().Printf("inserting rows " + key)
		ENCODED_KEY, ok := os.LookupEnv(EncodedKeyTag)
		if !ok {
			t.L().Printf("%s not set\n", EncodedKeyTag)
			return
		}

		importStr = "IMPORT INTO " + key + " (" + value[1] + ")\n"
		csvStr := " CSV DATA ('gs://" + bucketName + "/" + value[0] + "?AUTH=specified&CREDENTIALS=" + ENCODED_KEY + "');"
		queryStr := importStr + csvStr
		logTest("gs://"+bucketName+"/"+value[0], "TABLE_PATH:")
		if _, err := conn.Exec(queryStr); err != nil {
			t.L().Printf("error while inserting rows: %v", err)
			return
		}

	}

	// Execute queries and iterate over results.
	t.L().Printf("querying")
	t.L().Printf("Testing query: %v", finalStmt)
	logTest(finalStmt, "QUERY:")
	rows, err := conn.Query(finalStmt)

	if err != nil {
		t.L().Printf("error while querying: %v", err)
		return
	}

	cols, _ := rows.Columns()

	colPointers := make([]interface{}, len(cols))
	colContainer := make([]sql.NullString, len(cols))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	dataSize := 0
	for rows.Next() {

		if err := rows.Scan(colPointers...); err != nil {
			t.L().Printf("error while iterating over results: %v", err)
			return
		}
		rowData := colContainer //strings.Join(colContainer[:], ",")
		if dataSize < 10 {
			t.L().Printf("row=: %v", rowData)
		}

		dataSize = dataSize + 1
	}
	t.L().Printf("number of rows=%v", dataSize)
}
