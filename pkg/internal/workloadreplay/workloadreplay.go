// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadreplay

import (
	"context"
	gosql "database/sql"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	sf "github.com/snowflakedb/gosnowflake"
	"google.golang.org/api/option"
)

const (
	accountName  = "lt53838.us-central1.gcp"
	bucketName   = "roachtest-snowflake-costfuzz"
	keyTag       = "COCKROACH_GOOGLE_EPHEMERAL_CREDENTIALS"
	sfUser       = "COCKROACH_SFUSER"
	sfPassword   = "COCKROACH_SFPASSWORD"
	nonJoinRatio = 0.5
)

// getConnect makes connection to snowflake and returns the connection.
func getConnect(schema string) (*gosql.DB, error) {
	username := envutil.EnvOrDefaultString(sfUser, "")
	if username == "" {
		fmt.Printf("%s not set\n", sfUser)
	}
	password := envutil.EnvOrDefaultString(sfPassword, "")
	if password == "" {
		fmt.Printf("%s not set\n", sfPassword)
	}
	dsn, err := sf.DSN(&sf.Config{
		Account:   accountName,
		Database:  "DATA_ENG",
		Password:  password,
		Schema:    schema,
		User:      username,
		Warehouse: "COMPUTE_WH",
	})
	if err != nil {
		fmt.Printf("can not compile connection string")
		return nil, err
	}
	db, err := gosql.Open("snowflake", dsn)
	if err != nil {
		fmt.Printf("can not connect to snowflake")
		return nil, err
	}
	return db, nil
}

// writeRowsGCS writes data in rows to google cloud storage file.
// We write to gcs so we can import data from gcs cvs files to cockroachdb.
func writeRowsGCS(ctx context.Context, filename string, rows *gosql.Rows) (string, error) {
	credKey := envutil.EnvOrDefaultString(keyTag, "")
	if credKey == "" {
		return "", errors.New(keyTag + " not set")
	}
	encodedKey := base64.StdEncoding.EncodeToString([]byte(credKey))
	encodedCredByte, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return "", err
	}

	clientOption := option.WithCredentialsJSON(encodedCredByte)
	client, err := storage.NewClient(context.Background(), clientOption)
	if err != nil {
		return "", err
	}

	wc := client.Bucket(bucketName).Object(filename).NewWriter(ctx)
	wc.ContentType = "text/plain"

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	cName := strings.Join(cols, ",")
	// Will be used to read data while iterating rows.
	colPointers := make([]interface{}, len(cols))
	colContainer := make([]string, len(cols))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	// Iterate rows and write to gcs file.
	for rows.Next() {
		_ = rows.Scan(colPointers...)
		rowData := strings.Join(colContainer, ",")
		if _, err := wc.Write([]byte(rowData + "\n")); err != nil {
			fmt.Printf("can not write to bucket %s file %s", bucketName, filename)
			return "", err
		}

	}
	if err := wc.Close(); err != nil {
		fmt.Printf("can not close bucket %s file %s", bucketName, filename)
		return "", err
	}

	return cName, nil
}

func ChooseRandomQuery(ctx context.Context, log *os.File) (string, map[string][]string, error) {
	rndNumber := rand.Float64()
	var query string
	// Randomly select between a non join or join query.
	// nonJoinRatio specifies bias based on how representative joins
	// cardinality in query bank is of join workloads relative
	// to non join representation.
	if rndNumber <= nonJoinRatio {
		fmt.Fprint(log, "NON JOIN QUERY")
		query = "select a.statement,replace(regexp_replace(lower(replace(statement,'\"','')),concat('[\\\\\\S]*\\\\\\.[\\\\\\S]*\\\\\\.',lower(b.table_name)),lower(b.table_name)),'..','') statement_2,ifnull(a.database_name,'') database_name,a.CID,a.TID,a.schema_name,b.table_name,b.table_signature,b.cockroachdb_schema cockroachdb_schema from (select * from (select * from (select *,rank() over (partition by statement,schema_name,database_name,cid,tid order by run_date desc) as rnk from (select * from DATA_ENG.TEST_CONFIG.QUERYBANK where not contains(lower(statement),'join') and disqualify=0)) where rnk=1)  sample (1 rows))  a inner join (select * from (select *,rank() over (partition by table_signature,schema_name,database_name,cid,tid  order by run_date desc) as rnk from DATA_ENG.TEST_CONFIG.TABLE_SIGNATURES) where rnk=1) b on a.SCHEMA_NAME=b.SCHEMA_NAME and contains(lower(statement),lower(table_name)) "
	} else {
		fmt.Fprint(log, "JOIN QUERY")
		query = "select a.statement,replace(regexp_replace(lower(replace(statement,'\"','')),concat('[\\\\\\S]*\\\\\\.[\\\\\\S]*\\\\\\.',lower(b.table_name)),lower(b.table_name)),'..','') statement_2,ifnull(a.database_name,'') database_name,a.CID,a.TID,a.schema_name,b.table_name,b.table_signature,b.cockroachdb_schema cockroachdb_schema from (select * from (select * from (select *,rank() over (partition by statement,schema_name,database_name,cid,tid order by run_date desc) as rnk from (select * from DATA_ENG.TEST_CONFIG.QUERYBANK_SELECT where disqualify=0)) where rnk=1)  sample (1 rows))  a inner join (select * from (select *,rank() over (partition by table_signature,schema_name,database_name,cid,tid  order by run_date desc) as rnk from DATA_ENG.TEST_CONFIG.TABLE_SIGNATURES) where rnk=1) b on a.SCHEMA_NAME=b.SCHEMA_NAME and contains(lower(statement),lower(table_name)) "
	}

	db, err := getConnect("test_config")
	if err != nil {
		fmt.Fprint(log, err)
		fmt.Fprint(log, "\n")
		return "", nil, err
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprint(log, "Failure in choosing random query:")
		fmt.Fprint(log, err)
		fmt.Fprint(log, "\n")
		// Connection is either invalid or we hit a transient error, so let's close it.
		db.Close()
		return "", nil, err
	}

	signatures := make(map[string][]string)
	var finalStmt string
	for rows.Next() {
		var (
			stmt, stmtpostproc, dbname, cid, tid, schemaname, tablename, tablesignature, tableschema string
		)

		err := rows.Scan(&stmt, &stmtpostproc, &dbname, &cid, &tid, &schemaname, &tablename, &tablesignature, &tableschema)
		if err != nil {
			fmt.Fprint(log, "Failure in retrieving signature skipping rest of signatures:")
			fmt.Fprint(log, err)
			fmt.Fprint(log, "\n")
			break
		}
		if finalStmt == "" {
			finalStmt = strings.ReplaceAll(strings.ToLower(stmt), `"`, "")
		}
		d := strings.ToLower(tablename)
		m1 := regexp.MustCompile(`[\S]*\.[\S]*\.` + d)

		// Standardize statement so that table name does not include
		// database or schema wild card, eg. x.customers or .x.customers
		// gets replaced with customers
		finalStmt = m1.ReplaceAllString(finalStmt, d)

		signatures[tablename] = []string{tablesignature, tableschema}
	}

	return finalStmt, signatures, nil
}

// CreateRandomDataSnowflake creates random data in snowflake
// using signatures that reflect customer workloads.
// Next we write snowflake tables to GCS and return
// a mapping of table name to gcs filepath and schema.
func CreateRandomDataSnowflake(
	ctx context.Context, signatures map[string][]string, log *os.File,
) (map[string][]string, error) {
	db, err := getConnect("workload_data")
	if err != nil {
		fmt.Fprint(log, "COULD NOT CONNECT TO SNOWFLAKE: "+fmt.Sprint(err))
		return nil, err
	}

	schemaMap := make(map[string][]string)
	var cName string
	for tableName, signatureSchema := range signatures {
		// Create random data in snowflake using signature.
		_, err := db.QueryContext(ctx, signatureSchema[0])
		if err != nil {
			fmt.Fprint(log, "COULD NOT GENERATE DATASET - signature execution failed:"+signatureSchema[0])
			// Connection is either invalid or we hit a transient error, so let's close it.
			db.Close()
			return nil, err
		}

		// Get data from snowflake and write to GCS.
		query := "select * from " + tableName
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			fmt.Fprint(log, "COULD NOT RETRIEVE DATA FROM TABLE:"+tableName+"\n")
			fmt.Fprint(log, err)
			fmt.Fprint(log, "\n")
			// Connection is either invalid or we hit a transient error, so let's close it.
			db.Close()
			return nil, err
		}
		filename := "roachtest/" + fmt.Sprint(timeutil.Now().Unix()) + "_" + tableName
		cName, err = writeRowsGCS(ctx, filename, rows)
		if err != nil {
			fmt.Fprint(log, err)
			fmt.Fprint(log, "\n")
			return nil, err
		}
		// Map "table name" to file path of gcs data, column names of data and schema of table.
		schemaMap[tableName] = []string{filename, cName, signatureSchema[1]}
	}

	return schemaMap, nil
}
