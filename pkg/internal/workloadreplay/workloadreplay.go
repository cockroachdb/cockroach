package workloadreplay

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	sf "github.com/snowflakedb/gosnowflake"
)

const (
	projectID     = "cockroach-dev-inf"
	bucketName    = "cockroach-snowflake-backup"
	encodedKeyTag = "e_key"
	sfUser        = "SFUSER"
	sfPassword    = "SFPASSWORD"
	nonjoinratio  = 0.5
)

// getConnect makes connection to snowflake and returns the connection.
func getConnect(schema string) (*sql.DB, context.Context, context.CancelFunc) {
	username, ok := os.LookupEnv(sfUser)
	if !ok {
		fmt.Printf("%s not set\n", sfUser)
	}
	password, ok := os.LookupEnv(sfPassword)
	if !ok {
		fmt.Printf("%s not set\n", sfPassword)
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

// writeRowsGCS writes data in rows to google cloud storage file. We write to gcs so we can import data from
// gcs cvs files to cockroachdb.
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
	// Will be used to read data while iterating rows.
	colPointers := make([]interface{}, len(cols))
	colContainer := make([]string, len(cols))
	for i := range colPointers {
		colPointers[i] = &colContainer[i]
	}
	// Iterate rows and write to gcs file.
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

func ChooseRandomQuery(log *os.File) (string, map[string][]string) {
	rndNumber := rand.Float64()
	var query string
	// Randomly select between a non join or join query. nonjoinratio specifies bias based on how representive join
	// query bank cardinality is of join workloads relative to non join representation.
	if rndNumber <= nonjoinratio {
		fmt.Fprint(log, "NON JOIN QUERY")
		query = "select a.statement,replace(regexp_replace(lower(replace(statement,'\"','')),concat('[\\\\\\S]*\\\\\\.[\\\\\\S]*\\\\\\.',lower(b.table_name)),lower(b.table_name)),'..','') statement_2,ifnull(a.database_name,'') database_name,a.CID,a.TID,a.schema_name,b.table_name,b.table_signature,b.cockroachdb_schema cockroachdb_schema from (select * from (select * from (select *,rank() over (partition by statement,schema_name,database_name,cid,tid order by run_date desc) as rnk from (select * from DATA_ENG.TEST_CONFIG.QUERYBANK where not contains(lower(statement),'join'))) where rnk=1)  sample (1 rows))  a inner join (select * from (select *,rank() over (partition by table_signature,schema_name,database_name,cid,tid  order by run_date desc) as rnk from DATA_ENG.TEST_CONFIG.TABLE_SIGNATURES) where rnk=1) b on a.SCHEMA_NAME=b.SCHEMA_NAME and contains(lower(statement),lower(table_name)) "
	} else {
		fmt.Fprint(log, "JOIN QUERY")
		query = "select a.statement,replace(regexp_replace(lower(replace(statement,'\"','')),concat('[\\\\\\S]*\\\\\\.[\\\\\\S]*\\\\\\.',lower(b.table_name)),lower(b.table_name)),'..','') statement_2,ifnull(a.database_name,'') database_name,a.CID,a.TID,a.schema_name,b.table_name,b.table_signature,b.cockroachdb_schema cockroachdb_schema from (select * from (select * from (select *,rank() over (partition by statement,schema_name,database_name,cid,tid order by run_date desc) as rnk from (select * from DATA_ENG.TEST_CONFIG.QUERYBANK_SELECT )) where rnk=1)  sample (1 rows))  a inner join (select * from (select *,rank() over (partition by table_signature,schema_name,database_name,cid,tid  order by run_date desc) as rnk from DATA_ENG.TEST_CONFIG.TABLE_SIGNATURES) where rnk=1) b on a.SCHEMA_NAME=b.SCHEMA_NAME and contains(lower(statement),lower(table_name)) "

	}

	db, ctx, cancel := getConnect("test_config")
	defer cancel()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprint(log, "Failure in choosing random query:")
		fmt.Fprint(log, err)
		fmt.Fprint(log, "\n")
		return "", nil
	}

	signatures := make(map[string][]string)
	var finalStmt string
	finalStmt = ""
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
			finalStmt = strings.ReplaceAll(strings.ToLower(stmt), string('"'), string(""))
		}
		d := strings.ToLower(tablename)
		m1 := regexp.MustCompile(`[\S]*\.[\S]*\.` + d)
		finalStmt = m1.ReplaceAllString(finalStmt, d) // Standardize statement so that table name does not include
		// database or schema wild card eg. x.customers or .x.customers
		//should be replaced with customers

		signatures[tablename] = []string{tablesignature, tableschema}
	}

	return finalStmt, signatures

}

// CreateRandomDataSnowflake create random data in snowflake using signatures that reflect customer workloads.
// Write snowflake tables to GCS and return a mapping of table name to gcs filepath and schema.
func CreateRandomDataSnowflake(signatures map[string][]string, log *os.File) map[string][]string {
	db, ctx, cancel := getConnect("workload_data")
	defer cancel()

	schemaMap := make(map[string][]string)
	var cName string
	for tableName, signatureSchema := range signatures {
		// Create random data in snowflake using signature.
		_, err := db.QueryContext(ctx, signatureSchema[0])
		if err != nil {
			fmt.Fprint(log, "COULD NOT GENERATE DATASET - signature execution failed:"+signatureSchema[0])
			return nil
		}

		// Get data from snowflake and write to GCS.
		query := "select * from " + tableName
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			fmt.Fprint(log, "COULD NOT RETRIEVE DATA FROM TABLE:"+tableName+"\n")
			fmt.Fprint(log, err)
			fmt.Fprint(log, "\n")
			return nil
		}
		filename := "roachtest/" + fmt.Sprint(time.Now().Unix()) + "_" + tableName
		cName = writeRowsGCS(ctx, filename, rows)
		// Map "table name" to file path of gcs data, column names of data and schema of table.
		schemaMap[tableName] = []string{filename, cName, signatureSchema[1]}
	}

	return schemaMap

}
