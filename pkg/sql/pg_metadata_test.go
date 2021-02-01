// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This test compares a dump generated from PostgreSQL with the pg_catalog
// and compares it with the current pg_catalog at cockroach db skipping
// all the known diffs:
//
// cd pkg/sql
// go test -run TestPGCatalog
//
// If you want to re-create the known (expected) diffs with the current differences
// add -rewrite-diffs flag when running this test:
//
// cd pkg/sql
// go test -run TestPGCatalog -rewrite-diffs
//
// To create the postgres dump file see pkg/cmd/generate-pg-catalog/main.go:
//
// cd pkg/cmd/generate-pg-catalog/
// go run main.go > ../../sql/testdata/pg_catalog_tables.json.
package sql

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors/oserror"
	"github.com/lib/pq/oid"
)

// Test data files.
const (
	catalogDump     = "%s_tables.json"              // PostgreSQL pg_catalog schema
	expectedDiffs   = "%s_test_expected_diffs.json" // Contains expected difference between postgres and cockroach
	testdata        = "testdata"                    // testdata directory
	catalogPkg      = "catalog"
	catconstantsPkg = "catconstants"
	constantsGo     = "constants.go"
	vtablePkg       = "vtable"
	pgCatalogGo     = "pg_catalog.go"
)

// When running test with -rewrite-diffs test will pass and re-create pg_catalog_test-diffs.json
var (
	rewriteFlag = flag.Bool("rewrite-diffs", false, "This will re-create the expected diffs file")
	catalogName = flag.String("catalog", "pg_catalog", "Catalog or namespace, default: pg_catalog")
)

// strings used on constants creations and text manipulation.
const (
	pgCatalogPrefix          = "PgCatalog"
	pgCatalogIDConstant      = "PgCatalogID"
	tableIDSuffix            = "TableID"
	tableDefsDeclaration     = `tableDefs: map[descpb.ID]virtualSchemaDef{`
	tableDefsTerminal        = `},`
	allTableNamesDeclaration = `allTableNames: buildStringSet(`
	allTableNamesTerminal    = `),`
	virtualTablePosition     = `// typOid is the only OID generation approach that does not use oidHasher, because`
	virtualTableTemplate     = `var %s = virtualSchemaTable{
	comment: "%s was created for compatibility and is currently unimplemented",
	schema:  vtable.%s,
	populate: func(ctx context.Context, p *planner, _ *dbdesc.Immutable, addRow func(...tree.Datum) error) error {
		return nil
	},
}

`
)

var addMissingTables = flag.Bool(
	"add-missing-tables",
	false,
	"add-missing-tables will complete pg_catalog tables in the go code",
)

var (
	tableFinderRE = regexp.MustCompile(`(?i)CREATE TABLE pg_catalog\.([^\s]+)\s`)
)

var none = struct{}{}

// summary will keep accountability for any unexpected difference and report it in the log.
type summary struct {
	missingTables        int
	missingColumns       int
	mismatchDatatypesOid int
}

// report will log the amount of diffs for missing table and columns and data type mismatches.
func (sum *summary) report(t *testing.T) {
	if sum.missingTables != 0 {
		errorf(t, "Missing %d tables", sum.missingTables)
	}

	if sum.missingColumns != 0 {
		errorf(t, "Missing %d columns", sum.missingColumns)
	}

	if sum.mismatchDatatypesOid != 0 {
		errorf(t, "Column datatype mismatches: %d", sum.mismatchDatatypesOid)
	}
}

// loadTestData retrieves the pg_catalog from the dumpfile generated from Postgres
func loadTestData(t testing.TB) PGMetadataTables {
	var pgCatalogFile PGMetadataFile
	testdataFile := filepath.Join(testdata, fmt.Sprintf(catalogDump, *catalogName))
	f, err := os.Open(testdataFile)
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	if err = json.Unmarshal(bytes, &pgCatalogFile); err != nil {
		t.Fatal(err)
	}

	return pgCatalogFile.PGMetadata
}

// loadCockroachPgCatalog retrieves pg_catalog schema from cockroach db
func loadCockroachPgCatalog(t testing.TB) PGMetadataTables {
	crdbTables := make(PGMetadataTables)
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlRunner := sqlutils.MakeSQLRunner(db)
	rows := sqlRunner.Query(t, GetPGMetadataSQL, *catalogName)
	defer rows.Close()

	for rows.Next() {
		var tableName, columnName, dataType string
		var dataTypeOid uint32
		if err := rows.Scan(&tableName, &columnName, &dataType, &dataTypeOid); err != nil {
			t.Fatal(err)
		}
		crdbTables.AddColumnMetadata(tableName, columnName, dataType, dataTypeOid)
	}
	return crdbTables
}

// loadExpectedDiffs get all differences that will be skipped by the this test
func loadExpectedDiffs(t *testing.T) (diffs PGMetadataTables) {
	diffs = PGMetadataTables{}

	if *rewriteFlag {
		// For rewrite we want this to be empty and get populated.
		return
	}

	diffFile := filepath.Join(testdata, fmt.Sprintf(expectedDiffs, *catalogName))
	if _, err := os.Stat(diffFile); err != nil {
		if oserror.IsNotExist(err) {
			// File does not exists it means diffs are not expected.
			return
		}

		t.Fatal(err)
	}
	f, err := os.Open(diffFile)
	if err != nil {
		t.Fatal(err)
	}
	defer dClose(f)
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(bytes, &diffs); err != nil {
		t.Fatal(err)
	}

	return
}

// errorf wraps *testing.T Errorf to report fails only when the test doesn't run in rewrite mode.
func errorf(t *testing.T, format string, args ...interface{}) {
	if !*rewriteFlag {
		t.Errorf(format, args...)
	}
}

func rewriteDiffs(t *testing.T, diffs PGMetadataTables, diffsFile string) {
	if !*rewriteFlag {
		return
	}

	if err := diffs.rewriteDiffs(diffsFile); err != nil {
		t.Fatal(err)
	}
}

// fixConstants updates catconstants that are needed for pgCatalog.
func fixConstants(t *testing.T, notImplemented PGMetadataTables) {
	constantsFileName := filepath.Join(".", catalogPkg, catconstantsPkg, constantsGo)
	// pgConstants will contains all the pgCatalog tableID constant adding the new tables and preventing duplicates
	pgConstants := getPgCatalogConstants(t, constantsFileName, notImplemented)
	sort.Strings(pgConstants)

	// Rewrite will place all the pgConstants in alphabetical order after PgCatalogID
	rewriteFile(constantsFileName, func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			text := reader.Text()
			trimText := strings.TrimSpace(text)

			// Skips PgCatalog constants (except PgCatalogID) as these will be written from pgConstants slice
			if strings.HasPrefix(trimText, pgCatalogPrefix) && trimText != pgCatalogIDConstant {
				continue
			}

			output.appendString(text)
			output.appendString("\n")

			if trimText == pgCatalogIDConstant {
				for _, pgConstant := range pgConstants {
					output.appendString("\t")
					output.appendString(pgConstant)
					output.appendString("\n")
				}
			}
		}
	})
}

// fixVtable adds missing table's create table constants.
func fixVtable(t *testing.T, notImplemented PGMetadataTables) {
	fileName := filepath.Join(vtablePkg, pgCatalogGo)

	// rewriteFile first will check existing create table constants to avoid duplicates.
	rewriteFile(fileName, func(input *os.File, output outputFile) {
		existingTables := make(map[string]struct{})
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			text := reader.Text()
			output.appendString(text)
			output.appendString("\n")
			createTable := tableFinderRE.FindStringSubmatch(text)
			if createTable != nil {
				tableName := createTable[1]
				existingTables[tableName] = none
			}
		}

		for tableName, columns := range notImplemented {
			if _, ok := existingTables[tableName]; ok {
				// Table already implemented.
				continue
			}
			createTable, err := createTableConstant(tableName, columns)
			if err != nil {
				// We can not implement this table as this uses types not implemented.
				t.Log(err)
				continue
			}
			output.appendString(createTable)
		}
	})
}

// fixPgCatalogGo will update pgCatalog.allTableNames, pgCatalog.tableDefs and
// will add needed virtualSchemas.
func fixPgCatalogGo(notImplemented PGMetadataTables) {
	allTableNamesText := getAllTableNamesText(notImplemented)
	tableDefinitionText := getTableDefinitionsText(pgCatalogGo, notImplemented)

	rewriteFile(pgCatalogGo, func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			text := reader.Text()
			trimText := strings.TrimSpace(text)
			if trimText == virtualTablePosition {
				//VirtualSchemas doesn't have a particular place to start we just print it before virtualTablePosition
				output.appendString(printVirtualSchemas(notImplemented))
			}
			output.appendString(text)
			output.appendString("\n")

			switch trimText {
			case tableDefsDeclaration:
				printBeforeTerminalString(reader, output, tableDefsTerminal, tableDefinitionText)
			case allTableNamesDeclaration:
				printBeforeTerminalString(reader, output, allTableNamesTerminal, allTableNamesText)
			}
		}
	})
}

// printBeforeTerminalString will skip all the lines and print `s` text when finds the terminal string.
func printBeforeTerminalString(
	reader *bufio.Scanner, output outputFile, terminalString string, s string,
) {
	for reader.Scan() {
		text := reader.Text()
		trimText := strings.TrimSpace(text)

		if strings.HasPrefix(trimText, "//") {
			// As example, see pg_catalog.go where pg_catalog.allTablesNames are
			// defined, after "buildStringSet(" there are comments that will not
			// be replaced with `s` text.
			output.appendString(text)
			output.appendString("\n")
			continue
		}
		if trimText != terminalString {
			continue
		}
		output.appendString(s)
		output.appendString(text)
		output.appendString("\n")
		break
	}
}

// getPgCatalogConstants reads catconstant and retrieves all the constant with `PgCatalog` prefix.
func getPgCatalogConstants(
	t *testing.T, inputFileName string, notImplemented PGMetadataTables,
) []string {
	pgConstantSet := make(map[string]struct{})
	f, err := os.Open(inputFileName)
	if err != nil {
		t.Logf("Problem getting pgCatalogConstants: %v", err)
		t.Fatal(err)
	}
	defer dClose(f)
	reader := bufio.NewScanner(f)
	for reader.Scan() {
		text := strings.TrimSpace(reader.Text())
		if strings.HasPrefix(text, pgCatalogPrefix) {
			if text == pgCatalogIDConstant {
				continue
			}
			pgConstantSet[text] = none
		}
	}
	for tableName := range notImplemented {
		pgConstantSet[constantName(tableName, tableIDSuffix)] = none
	}
	pgConstants := make([]string, 0, len(pgConstantSet))
	for pgConstantName := range pgConstantSet {
		pgConstants = append(pgConstants, pgConstantName)
	}
	return pgConstants
}

// outputFile wraps an *os.file to avoid explicit error checks on every WriteString.
type outputFile struct {
	f *os.File
}

// appendString calls WriteString and panics on error
func (o outputFile) appendString(s string) {
	if _, err := o.f.WriteString(s); err != nil {
		panic(fmt.Errorf("error while writing string: %s: %v", s, err))
	}
}

// rewriteFile recreate a file by using the f func, this creates a temporary
// file to place all the output first then it replaces the original file.
func rewriteFile(fileName string, f func(*os.File, outputFile)) {
	tmpName := fileName + ".tmp"
	updateFile(fileName, tmpName, f)
	defer func() {
		if err := os.Remove(tmpName); err != nil {
			panic(fmt.Errorf("problem removing temp file %s: %e", tmpName, err))
		}
	}()

	updateFile(tmpName, fileName, func(input *os.File, output outputFile) {
		if _, err := io.Copy(output.f, input); err != nil {
			panic(fmt.Errorf("problem at rewriting file %s into %s: %v", tmpName, fileName, err))
		}
	})
}

func updateFile(inputFileName, outputFileName string, f func(input *os.File, output outputFile)) {
	input, err := os.Open(inputFileName)
	if err != nil {
		panic(fmt.Errorf("error opening file %s: %v", inputFileName, err))
	}
	defer dClose(input)

	output, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Errorf("error opening file %s: %v", outputFileName, err))
	}
	defer dClose(output)

	f(input, outputFile{output})
}

// dClose is a helper that eliminates the need of error checking and defer the
// io.Closer Close() and pass lint checks.
func dClose(f io.Closer) {
	err := f.Close()
	if err != nil {
		panic(err)
	}
}

var acronyms = map[string]struct{}{
	"acl": none,
	"id":  none,
}

// constantName create constant names for pg_catalog fixableTables following
// constant names standards.
func constantName(tableName string, suffix string) string {
	var sb strings.Builder
	snakeWords := strings.Split(tableName, "_")[1:]
	sb.WriteString("PgCatalog")

	for _, word := range snakeWords {
		if _, ok := acronyms[word]; ok {
			sb.WriteString(strings.ToUpper(word))
		} else {
			sb.WriteString(strings.ToUpper(word[:1]))
			sb.WriteString(word[1:])
		}
	}

	sb.WriteString(suffix)
	return sb.String()
}

// createTableConstant formats the text for vtable constants.
func createTableConstant(tableName string, columns PGMetadataColumns) (string, error) {
	var sb strings.Builder
	constName := constantName(tableName, "")
	if notImplementedTypes := columns.getUnimplementedTypes(); len(notImplementedTypes) > 0 {
		return "", fmt.Errorf("not all types are implemented %s: %v", tableName, notImplementedTypes)
	}

	sb.WriteString("\n//")
	sb.WriteString(constName)
	sb.WriteString(" is an empty table in the pg_catalog that is not implemented yet\n")
	sb.WriteString("const ")
	sb.WriteString(constName)
	sb.WriteString(" = `\n")
	sb.WriteString("CREATE TABLE pg_catalog.")
	sb.WriteString(tableName)
	sb.WriteString(" (\n")
	prefix := ""
	for columnName, columnType := range columns {
		formatColumn(&sb, prefix, columnName, columnType)
		prefix = ",\n"
	}
	sb.WriteString("\n)`\n")
	return sb.String(), nil
}

func formatColumn(
	sb *strings.Builder, prefix, columnName string, columnType *PGMetadataColumnType,
) {
	typeOid := oid.Oid(columnType.Oid)
	typeName := types.OidToType[typeOid].Name()
	if !strings.HasPrefix(typeName, `"char"`) {
		typeName = strings.ToUpper(typeName)
	}
	sb.WriteString(prefix)
	sb.WriteString("\t")
	sb.WriteString(columnName)
	sb.WriteString(" ")
	sb.WriteString(typeName)
}

// printVirtualSchemas formats the golang code to create the virtualSchema
// structure.
func printVirtualSchemas(newTableNameList PGMetadataTables) string {
	var sb strings.Builder
	for tableName := range newTableNameList {
		variableName := "p" + constantName(tableName, "Table")[1:]
		vTableName := constantName(tableName, "")
		sb.WriteString(fmt.Sprintf(virtualTableTemplate, variableName, tableName, vTableName))
	}
	return sb.String()
}

// getAllTableNamesText retrieves pgCatalog.allTableNames, then it merges the
// new table names and formats the replacement text.
func getAllTableNamesText(notImplemented PGMetadataTables) string {
	newTableNameSet := make(map[string]struct{})
	for tableName := range pgCatalog.allTableNames {
		newTableNameSet[tableName] = none
	}
	for tableName := range notImplemented {
		newTableNameSet[tableName] = none
	}
	newTableList := make([]string, 0, len(newTableNameSet))
	for tableName := range newTableNameSet {
		newTableList = append(newTableList, tableName)
	}
	sort.Strings(newTableList)
	return formatAllTableNamesText(newTableList)
}

func formatAllTableNamesText(newTableNameList []string) string {
	var sb strings.Builder
	for _, tableName := range newTableNameList {
		sb.WriteString("\t\t\"")
		sb.WriteString(tableName)
		sb.WriteString("\",\n")
	}
	return sb.String()
}

// getTableDefinitionsText creates the text that will replace current
// definition of pgCatalog.tableDefs (at pg_catalog.go), by adding the new
// table definitions.
func getTableDefinitionsText(fileName string, notImplemented PGMetadataTables) string {
	tableDefs := make(map[string]string)
	maxLength := 0
	f, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Errorf("could not open file %s: %v", fileName, err))
	}
	defer dClose(f)
	reader := bufio.NewScanner(f)
	for reader.Scan() {
		text := strings.TrimSpace(reader.Text())
		if text == tableDefsDeclaration {
			break
		}
	}
	for reader.Scan() {
		text := strings.TrimSpace(reader.Text())
		if text == tableDefsTerminal {
			break
		}
		def := strings.Split(text, ":")
		defName := strings.TrimSpace(def[0])
		defValue := strings.TrimRight(strings.TrimSpace(def[1]), ",")
		tableDefs[defName] = defValue
		length := len(defName)
		if length > maxLength {
			maxLength = length
		}
	}

	for tableName := range notImplemented {
		defName := "catconstants." + constantName(tableName, tableIDSuffix)
		if _, ok := tableDefs[defName]; ok {
			// Not overriding existing tableDefinitions
			delete(notImplemented, tableName)
			continue
		}
		defValue := "p" + constantName(tableName, "Table")[1:]
		tableDefs[defName] = defValue
		length := len(defName)
		if length > maxLength {
			maxLength = length
		}
	}

	return formatTableDefinitionText(tableDefs, maxLength)
}

func formatTableDefinitionText(tableDefs map[string]string, maxLength int) string {
	var sbAll strings.Builder
	sortedDefKeys := getSortedDefKeys(tableDefs)
	for _, defKey := range sortedDefKeys {
		var sb strings.Builder
		sb.WriteString("\t\t")
		sb.WriteString(defKey)
		sb.WriteString(":")
		for sb.Len() < maxLength+4 {
			sb.WriteString(" ")
		}
		sb.WriteString(tableDefs[defKey])
		sb.WriteString(",\n")
		sbAll.WriteString(sb.String())
	}
	return sbAll.String()
}

func getSortedDefKeys(tableDefs map[string]string) []string {
	keys := make([]string, 0, len(tableDefs))
	for constName := range tableDefs {
		keys = append(keys, constName)
	}
	sort.Strings(keys)
	return keys
}

// TestPGCatalog is the pg_catalog diff tool test which compares pg_catalog
// with postgres and cockroach.
func TestPGCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			t.Fatal(err)
		}
	}()

	if *addMissingTables && *catalogName != "pg_catalog" {
		t.Fatal("--add-missing-tables only work for pg_catalog")
	}

	pgTables := loadTestData(t)
	crdbTables := loadCockroachPgCatalog(t)
	diffs := loadExpectedDiffs(t)
	sum := &summary{}

	for pgTable, pgColumns := range pgTables {
		t.Run(fmt.Sprintf("Table=%s", pgTable), func(t *testing.T) {
			crdbColumns, ok := crdbTables[pgTable]
			if !ok {
				if !diffs.isExpectedMissingTable(pgTable) {
					errorf(t, "Missing table `%s`", pgTable)
					diffs.addMissingTable(pgTable)
					sum.missingTables++
				}
				return
			}

			for expColumnName, expColumn := range pgColumns {
				gotColumn, ok := crdbColumns[expColumnName]
				if !ok {
					if !diffs.isExpectedMissingColumn(pgTable, expColumnName) {
						errorf(t, "Missing column `%s`", expColumnName)
						diffs.addMissingColumn(pgTable, expColumnName)
						sum.missingColumns++
					}
					continue
				}

				if diffs.isDiffOid(pgTable, expColumnName, expColumn, gotColumn) {
					sum.mismatchDatatypesOid++
					errorf(t, "Column `%s` expected data type oid `%d` (%s) but found `%d` (%s)",
						expColumnName,
						expColumn.Oid,
						expColumn.DataType,
						gotColumn.Oid,
						gotColumn.DataType,
					)
					diffs.addDiff(pgTable, expColumnName, expColumn, gotColumn)
				}
			}
		})
	}

	sum.report(t)
	rewriteDiffs(t, diffs, filepath.Join(testdata, fmt.Sprintf(expectedDiffs, *catalogName)))

	if *addMissingTables {
		unimplemented := diffs.getUnimplementedTables(pgTables)
		fixConstants(t, unimplemented)
		fixVtable(t, unimplemented)
		fixPgCatalogGo(unimplemented)
	}
}
