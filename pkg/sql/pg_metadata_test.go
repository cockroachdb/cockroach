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
	"go/ast"
	goParser "go/parser"
	"go/token"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

// When running test with -rewrite-diffs test will pass and re-create pg_catalog_test-diffs.json.
var (
	rewriteFlag = flag.Bool("rewrite-diffs", false, "This will re-create the expected diffs file")
	catalogName = flag.String("catalog", "pg_catalog", "Catalog or namespace, default: pg_catalog")
)

// strings used on constants creations and text manipulation.
const (
	pgCatalogPrefix            = "PgCatalog"
	pgCatalogIDConstant        = "PgCatalogID"
	tableIDSuffix              = "TableID"
	tableDefsDeclaration       = `tableDefs: map[descpb.ID]virtualSchemaDef{`
	tableDefsTerminal          = `},`
	undefinedTablesDeclaration = `undefinedTables: buildStringSet(`
	undefinedTablesTerminal    = `),`
	virtualTablePosition       = `// typOid is the only OID generation approach that does not use oidHasher, because`
	virtualTableSchemaField    = "schema"
	virtualTablePopulateField  = "populate"
)

// virtualTableTemplate is used to create new virtualSchemaTable objects when
// adding new tables.
var virtualTableTemplate = fmt.Sprintf(`var %s = virtualSchemaTable{
	comment: "%s was created for compatibility and is currently unimplemented",
	%s:  vtable.%s,
	%s: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return nil
	},
	unimplemented: true,
}

`, "%s", "%s", virtualTableSchemaField, "%s", virtualTablePopulateField)

// Name of functions that may need modifications when new columns are added.
const (
	makeAllRelationsVirtualTableWithDescriptorIDIndexFunc = "makeAllRelationsVirtualTableWithDescriptorIDIndex"
)

var addMissingTables = flag.Bool(
	"add-missing-tables",
	false,
	"add-missing-tables will complete pg_catalog tables and columns in the go code",
)

var (
	tableFinderRE = regexp.MustCompile(`(?i)CREATE TABLE pg_catalog\.([^\s]+)\s`)
	constNameRE   = regexp.MustCompile(`const ([^\s]+)\s`)
	indentationRE = regexp.MustCompile(`^(\s*)`)
	indexRE       = regexp.MustCompile(`(?i)INDEX\s*\([^\)]+\)`)
)

// Types.
var (
	virtualSchemaType      = reflect.TypeOf((*virtualSchema)(nil)).Elem()
	virtualSchemaTableType = reflect.TypeOf((*virtualSchemaTable)(nil)).Elem()
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

// loadTestData retrieves the pg_catalog from the dumpfile generated from Postgres.
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

// loadCockroachPgCatalog retrieves pg_catalog schema from cockroach db.
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

// loadExpectedDiffs get all differences that will be skipped by the this test.
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
	// pgConstants will contains all the pgCatalog tableID constant adding the
	// new tables and preventing duplicates.
	pgConstants := getPgCatalogConstants(t, constantsFileName, notImplemented)
	sort.Strings(pgConstants)

	// Rewrite will place all the pgConstants in alphabetical order after
	// PgCatalogID.
	rewriteFile(constantsFileName, func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			text := reader.Text()
			trimText := strings.TrimSpace(text)

			// Skips PgCatalog constants (except PgCatalogID) as these will be
			// written from pgConstants slice.
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
func fixVtable(
	t *testing.T,
	unimplemented PGMetadataTables,
	unimplementedColumns PGMetadataTables,
	pgCode *pgCatalogCode,
) {
	fileName := filepath.Join(vtablePkg, pgCatalogGo)

	// rewriteFile first will check existing create table constants to avoid
	// duplicates.
	rewriteFile(fileName, func(input *os.File, output outputFile) {
		existingTables := make(map[string]struct{})
		reader := bufio.NewScanner(input)
		var constName string
		var tableName string
		fixedTables := make(map[string]struct{})
		var sb strings.Builder

		for reader.Scan() {
			text := reader.Text()
			trimText := strings.TrimSpace(text)
			// vTable file lists a set of constants with the create tables. Constants
			// are referred in virtualSchemaTable, so first we find the constant name
			// to map with the table name later.
			constDecl := constNameRE.FindStringSubmatch(text)
			if constDecl != nil {
				constName = constDecl[1]
			}

			createTable := tableFinderRE.FindStringSubmatch(text)
			if createTable != nil {
				tableName = createTable[1]
				existingTables[tableName] = none
			}

			// nextIsIndex helps to avoid detecting an INDEX line as column name.
			nextIsIndex := indexRE.MatchString(strings.ToUpper(trimText))
			// fixedTables keep track of all the tables which we added new columns.
			if _, fixed := fixedTables[tableName]; !fixed && (text == ")`" || nextIsIndex) {
				missingColumnsText := getMissingColumnsText(constName, tableName, nextIsIndex, unimplementedColumns, pgCode)
				if len(missingColumnsText) > 0 {
					// Right parenthesis is already printed in the output, but we need to
					// add new columns before that.
					output.seekRelative(-1)
				}
				output.appendString(missingColumnsText)
				fixedTables[tableName] = none
				pgCode.addRowPositions.removeIfNoMissingColumns(constName)
				pgCode.addRowPositions.reportNewColumns(&sb, constName, tableName)
			}

			output.appendString(text)
			output.appendString("\n")
		}

		first := true
		for tableName, columns := range unimplemented {
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
			reportNewTable(&sb, tableName, &first)
			output.appendString(createTable)
		}
	})
}

func reportNewTable(sb *strings.Builder, tableName string, first *bool) {
	if *first {
		sb.WriteString("New Tables:\n")
		*first = false
	}
	sb.WriteString("\t")
	sb.WriteString(tableName)
	sb.WriteString("\n")
}

// getMissingColumnsText creates the text used by a vTable constant to add the
// new (or missing) columns.
func getMissingColumnsText(
	constName string,
	tableName string,
	nextIsIndex bool,
	unimplementedColumns PGMetadataTables,
	pgCode *pgCatalogCode,
) string {
	nilPopulateTables := pgCode.fixableTables
	if _, fixable := nilPopulateTables[constName]; !fixable {
		return ""
	}
	columns, found := unimplementedColumns[tableName]
	if !found {
		return ""
	}
	var sb strings.Builder
	prefix := ",\n"
	if nextIsIndex {
		// Previous line already had comma.
		prefix = "\n"
	}
	for columnName, columnType := range columns {
		formatColumn(&sb, prefix, columnName, columnType)
		pgCode.addRowPositions.addMissingColumn(constName, columnName)
		prefix = ",\n"
	}
	if nextIsIndex {
		sb.WriteString(",")
	}
	sb.WriteString("\n")
	return sb.String()
}

// fixPgCatalogGo will update pgCatalog.undefinedTables, pgCatalog.tableDefs and
// will add needed virtualSchemas.
func fixPgCatalogGo(t *testing.T, notImplemented PGMetadataTables) {
	undefinedTablesText, err := getUndefinedTablesText(notImplemented, pgCatalog)
	if err != nil {
		t.Fatal(err)
	}
	tableDefinitionText := getTableDefinitionsText(pgCatalogGo, notImplemented)

	rewriteFile(pgCatalogGo, func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		for reader.Scan() {
			text := reader.Text()
			trimText := strings.TrimSpace(text)
			if trimText == virtualTablePosition {
				//VirtualSchemas doesn't have a particular place to start we just print
				// it before virtualTablePosition.
				output.appendString(printVirtualSchemas(notImplemented))
			}
			output.appendString(text)
			output.appendString("\n")

			switch trimText {
			case tableDefsDeclaration:
				printBeforeTerminalString(reader, output, tableDefsTerminal, tableDefinitionText)
			case undefinedTablesDeclaration:
				printBeforeTerminalString(reader, output, undefinedTablesTerminal, undefinedTablesText)
			}
		}
	})
}

func fixPgCatalogGoColumns(positions addRowPositionList) {
	rewriteFile(pgCatalogGo, func(input *os.File, output outputFile) {
		reader := bufio.NewScanner(input)
		scannedUntil := 0
		currentPosition := 0
		for reader.Scan() {
			text := reader.Text()
			count := len(text) + 1

			if currentPosition < len(positions) && int64(scannedUntil+count) > positions[currentPosition].insertPosition {
				relativeIndex := int(positions[currentPosition].insertPosition-int64(scannedUntil)) - 1
				left := text[:relativeIndex]
				indentation := indentationRE.FindStringSubmatch(text)[1] //The way it is it should at least give ""
				if len(strings.TrimSpace(left)) > 0 {
					// Parenthesis is right after the last variable in this case
					// indentation is correct.
					output.appendString(left)
					output.appendString(",\n")
				} else {
					// Parenthesis is after a new line, we got to add one tab.
					indentation += "\t"
				}

				output.appendString(indentation)
				output.appendString("// These columns were automatically created by pg_catalog_test's missing column generator.")
				output.appendString("\n")

				for _, columnName := range positions[currentPosition].missingColumns {
					output.appendString(indentation)
					output.appendString("tree.DNull,")
					output.appendString(" // ")
					output.appendString(columnName)
					output.appendString("\n")
				}

				output.appendString(indentation[:len(indentation)-1])
				output.appendString(text[relativeIndex:])
				currentPosition++
			} else {
				// No insertion point, just write what-ever have been read.
				output.appendString(text)
			}
			output.appendString("\n")
			scannedUntil += count
		}
	})
}

// printBeforeTerminalString will skip all the lines and print `s` text when
// finds the terminal string.
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

// getPgCatalogConstants reads catconstant and retrieves all the constant with
// `PgCatalog` prefix.
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

// outputFile wraps an *os.file to avoid explicit error checks on every
// WriteString.
type outputFile struct {
	f *os.File
}

// appendString calls WriteString and panics on error.
func (o outputFile) appendString(s string) {
	if _, err := o.f.WriteString(s); err != nil {
		panic(fmt.Errorf("error while writing string: %s: %v", s, err))
	}
}

// seekRelative Allows outputFile wrapper to use Seek function in the wrapped
// file.
func (o outputFile) seekRelative(offset int64) {
	if _, err := o.f.Seek(offset, io.SeekCurrent); err != nil {
		panic(fmt.Errorf("could not seek file"))
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

	output, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
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

// getTableNameFromCreateTable uses pkg/sql/parser to analyze CREATE TABLE
// statement to retrieve table name.
func getTableNameFromCreateTable(createTableText string) (string, error) {
	stmt, err := parser.ParseOne(createTableText)
	if err != nil {
		return "", err
	}

	create := stmt.AST.(*tree.CreateTable)
	return create.Table.Table(), nil
}

// getUndefinedTablesText retrieves pgCatalog.undefinedTables, then it merges the
// new table names and formats the replacement text.
func getUndefinedTablesText(newTables PGMetadataTables, vs virtualSchema) (string, error) {
	newTableList, err := getUndefinedTablesList(newTables, vs)
	if err != nil {
		return "", err
	}
	return formatUndefinedTablesText(newTableList), nil
}

// getUndefinedTablesList checks undefinedTables in the virtualSchema and makes
// sure they are not defined in tableDefs or are newTables to implement.
func getUndefinedTablesList(newTables PGMetadataTables, vs virtualSchema) ([]string, error) {
	var undefinedTablesList []string
	removeTables := make(map[string]struct{})
	for _, table := range vs.tableDefs {
		tableName, err := getTableNameFromCreateTable(table.getSchema())
		if err != nil {
			return nil, err
		}

		removeTables[tableName] = struct{}{}
	}

	for tableName := range newTables {
		removeTables[tableName] = struct{}{}
	}

	for tableName := range vs.undefinedTables {
		if _, ok := removeTables[tableName]; !ok {
			undefinedTablesList = append(undefinedTablesList, tableName)
		}
	}

	sort.Strings(undefinedTablesList)
	return undefinedTablesList, nil
}

// formatUndefinedTablesText gets the text to be printed as undefinedTables.
func formatUndefinedTablesText(newTableNameList []string) string {
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

// goParsePgCatalogGo parses pg_catalog.go using go/parser to get the list of
// tables that are unimplemented (return zero rows) and get where to insert
// new columns (if needed) by mapping all the addRow calls with the table.
func goParsePgCatalogGo(t *testing.T) *pgCatalogCode {
	fs := token.NewFileSet()
	f, err := goParser.ParseFile(fs, pgCatalogGo, nil, goParser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	srcFile, err := os.Open(pgCatalogGo)
	if err != nil {
		t.Fatal(err)
	}
	defer dClose(srcFile)
	pgCode := &pgCatalogCode{
		fixableTables:   make(map[string]struct{}),
		addRowPositions: make(map[string]addRowPositionList),
		schemaParam:     -1, // This value will be calculated later and once
		srcFile:         srcFile,
		t:               t,
	}

	ast.Walk(&pgCatalogCodeVisitor{
		pgCode:         pgCode,
		schema:         "",
		addRowFuncName: "",
		bodyStmts:      0,
	}, f)

	return pgCode
}

// addRowPosition describes an insertion point for new columns.
type addRowPosition struct {
	schema         string
	argSize        int
	insertPosition int64
	missingColumns []string
}

type addRowPositionList []*addRowPosition

type mappedRowPositions map[string]addRowPositionList

// pgCatalogCode describes pg_catalog.go insertion points for addRow calls and
// virtualSchemaTables which its populate func returns nil.
type pgCatalogCode struct {
	fixableTables   map[string]struct{}
	addRowPositions mappedRowPositions
	schemaParam     int // Index where we expect to find schema at makeAllRelationsVirtualTableWithDescriptorIDIndex.
	srcFile         *os.File
	t               *testing.T
}

// pgCatalogCodeVisitor implements ast.Visitor for traversing pg_catalog.go.
type pgCatalogCodeVisitor struct {
	pgCode         *pgCatalogCode
	schema         string
	addRowFuncName string
	bodyStmts      int
}

// nextWithSchema will set the schema for inner nodes visitors.
func (v pgCatalogCodeVisitor) nextWithSchema(schema string) pgCatalogCodeVisitor {
	next := v
	next.schema = schema
	return next
}

// Visit implements ast.Visitor and sets the rules for detecting schema,
// matching schema with addRow calls and finding which schemas have "return
// nil" at populate function.
//
// The code is parsed and creating a tree structure that is being traversed by
// ast.Walk() function, to visit a particular node this method is being called
// from the ast.Visitor.
//
// The nodes may have different kind of token types (like variable or functions
// declarations, function calls, arguments, types, etc) so this Visitor is
// looking at specific tokens to find calls of "addRow" in populate function
// from virtualSchemaTable, and match this calls and positions to a table
// schema which later is used to modify addRows if there are new columns to
// add.
func (v pgCatalogCodeVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *ast.KeyValueExpr:
		// KeyValueExpr nodes contains attribute setting on structures. Here we are
		// looking at specific attributes contained at virtualSchemaTable struct,
		// like schema and populate function.
		key, ok := n.Key.(*ast.Ident)
		if !ok {
			return v
		}
		switch key.Name {
		case virtualTableSchemaField:
			// Schema value usually comes with SelectorExpr (something like
			// vtable.PGCatalogRange) which constant name string is relevant to us
			// (PGCatalogRange) addRow calls are being matched to this constant name.
			val, ok := n.Value.(*ast.SelectorExpr)
			if !ok {
				return v
			}
			v.schema = val.Sel.String()
		case virtualTablePopulateField:
			// FuncLit is a function definition, we can look for addRow if this is
			// a function definition.
			val, ok := n.Value.(*ast.FuncLit)
			if !ok {
				return v
			}
			v.bodyStmts = len(val.Body.List)
			index := v.findAddRowFuncName(val)
			if index <= -1 {
				v.pgCode.t.Fatal("populate function does not have a parameter with 'func(...tree.Datum) error' signature")
			}
		}
	case *ast.ReturnStmt:
		result, ok := n.Results[0].(*ast.Ident)
		if !ok {
			return v
		}

		// This validates the ReturnStmt comes from populate function and it is the
		// only statement.
		if result.String() == "nil" && v.schema != "" && v.bodyStmts == 1 {
			// Populate function just returns nil.
			v.pgCode.fixableTables[v.schema] = none
		}
	case *ast.CallExpr:
		// These are actual function calls, we look for specifc function calls
		// to see where to insert new columns.
		fun, ok := n.Fun.(*ast.Ident)
		if !ok {
			return v
		}

		switch fun.Name {
		case v.addRowFuncName:
			if v.schema == "" || v.addRowFuncName == "" {
				// Could not match addRow with schema
				return v
			}

			if _, ok = v.pgCode.fixableTables[v.schema]; !ok {
				v.pgCode.fixableTables[v.schema] = none
			}
			if _, ok = v.pgCode.addRowPositions[v.schema]; !ok {
				// Capacity of 3 is decided based on the maximum number of calls of
				// addRow function at any populate function to cover most of the cases.
				v.pgCode.addRowPositions[v.schema] = make([]*addRowPosition, 0, 3)
			}

			// missingColumns capacity of 5 is based on the maximum number of new
			// columns to cover most of the cases without growing the internal array
			// in the missingColumns slice.
			addRow := &addRowPosition{
				schema:         v.schema,
				argSize:        len(n.Args), // Number of arguments must match with amount of columns
				insertPosition: int64(n.Rparen),
				missingColumns: make([]string, 0, 5), // This will be filled when fixing vtable and used to comment nils
			}
			addRowList := v.pgCode.addRowPositions[v.schema]
			v.pgCode.addRowPositions[v.schema] = append(addRowList, addRow)
		case makeAllRelationsVirtualTableWithDescriptorIDIndexFunc:
			// This special case when the table definition is in this function and
			// passed the populate function as an argument.
			schemaIndex := v.findSchemaIndex(n)
			if schemaIndex < 0 || len(n.Args) <= schemaIndex {
				// This is not probably that happen but just in case to avoid hitting an index out of bounds
				return v
			}
			val, ok := n.Args[schemaIndex].(*ast.SelectorExpr)
			if !ok || val == nil {
				return v
			}

			return v.nextWithSchema(val.Sel.String())
		}
	}

	return v
}

// singleSortedList will retrieve all the positions at ascending order to fix
// columns sequentially by reading the file.
func (m mappedRowPositions) singleSortedList() addRowPositionList {
	positions := make(addRowPositionList, 0, len(m))
	for _, val := range m {
		positions = append(positions, val...)
	}
	sort.Slice(positions, func(i int, j int) bool {
		return positions[i].insertPosition < positions[j].insertPosition
	})
	return positions
}

// removeIfNoMissingColumns will clean up addRow calls positions that doesn't
// require any column adding.
func (m mappedRowPositions) removeIfNoMissingColumns(constName string) {
	if addRowList, ok := m[constName]; ok && len(addRowList) > 0 {
		addRow := addRowList[0]
		if len(addRow.missingColumns) == 0 {
			delete(m, constName)
		}
	}
}

// addMissingColumn adds columnName for specific constName (schema).
func (m mappedRowPositions) addMissingColumn(constName, columnName string) {
	if addRows, ok := m[constName]; ok {
		for _, addRow := range addRows {
			addRow.missingColumns = append(addRow.missingColumns, columnName)
		}
	}
}

func (m mappedRowPositions) reportNewColumns(sb *strings.Builder, constName, tableName string) {
	if addRowList, ok := m[constName]; ok && len(addRowList) > 0 {
		addRow := addRowList[0]
		if len(addRow.missingColumns) > 0 {
			sb.WriteString("New columns in table ")
			sb.WriteString(tableName)
			sb.WriteString(":\n")
			for _, columnName := range addRow.missingColumns {
				sb.WriteString("\t")
				sb.WriteString(columnName)
				sb.WriteString("\n")
			}
		}
	}
}

// findSchemaIndex is a helper function to retrieve what is the parameter index for schemaDef at function
// makeAllRelationsVirtualTableWithDescriptorIDIndex.
func (v *pgCatalogCodeVisitor) findSchemaIndex(call *ast.CallExpr) int {
	if v.pgCode.schemaParam != -1 {
		return v.pgCode.schemaParam
	}

	fun, ok := call.Fun.(*ast.Ident)
	if !ok {
		return -1
	}
	decl, ok := fun.Obj.Decl.(*ast.FuncDecl)
	if !ok {
		return -1
	}
	_, index := v.findInFunctionParameters(decl.Type, "schemaDef", "string")
	if index > -1 {
		v.pgCode.schemaParam = index
	}

	return index
}

// findAddRowFuncName will retrieve the function name used by populate function
// with func(...tree.Datum) error signature, which is used to populate
// virtualSchemaTable.
func (v *pgCatalogCodeVisitor) findAddRowFuncName(funcLit *ast.FuncLit) int {
	paramName, index := v.findInFunctionParameters(funcLit.Type, "", "func(...tree.Datum) error")
	if index > -1 {
		v.addRowFuncName = paramName
	}

	return index
}

// findInFunctionParameters search for name and type of the parameters used
// by the given function, use empty string if search for type only. Return
// gives the name of the parameter and index or number of the parameter. If
// parameter not found returns empty string and -1.
func (v *pgCatalogCodeVisitor) findInFunctionParameters(
	funcType *ast.FuncType, name, paramTypeName string,
) (string, int) {
	var paramTypeString string
	var err error
	for index, param := range funcType.Params.List {
		if len(param.Names) != 1 || param.Names[0] == nil {
			continue
		}

		switch paramType := param.Type.(type) {
		case *ast.Ident:
			paramTypeString = paramType.String()
		case *ast.FuncType:
			// When it is a function type there is no direct way to get the parameter
			// type as string, in this case this access the source code file using
			// token positions to retrieve the text that defines the type.
			paramTypeString, err = readSourcePortion(v.pgCode.srcFile, paramType.Pos(), paramType.End())
			if err != nil {
				panic(err)
			}
		default:
			continue
		}

		if (name == "" || name == param.Names[0].String()) && (paramTypeString == paramTypeName) {
			return param.Names[0].String(), index
		}
	}

	return "", -1
}

// readSourcePortion uses direct access to specific position (given by tokens)
// to access a portion of the source code
func readSourcePortion(srcFile *os.File, start, end token.Pos) (string, error) {
	length := int(end - start)
	bytes := make([]byte, end-start)
	// Whence zero to Seek from beginning of the file.
	_, err := srcFile.Seek(int64(start)-1, io.SeekStart)
	if err != nil {
		return "", err
	}
	read, err := srcFile.Read(bytes)
	if err != nil {
		return "", nil
	}
	if read != length {
		return "", fmt.Errorf("expected to read %d but read %d", length, read)
	}
	return string(bytes), nil
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
	diffs.removeImplementedColumns(crdbTables)
	rewriteDiffs(t, diffs, filepath.Join(testdata, fmt.Sprintf(expectedDiffs, *catalogName)))

	if *addMissingTables {
		validateUndefinedTablesField(t)
		unimplemented := diffs.getUnimplementedTables(pgTables)
		unimplementedColumns := diffs.getUnimplementedColumns(pgTables)
		pgCode := goParsePgCatalogGo(t)
		fixConstants(t, unimplemented)
		fixVtable(t, unimplemented, unimplementedColumns, pgCode)
		fixPgCatalogGoColumns(pgCode.addRowPositions.singleSortedList())
		fixPgCatalogGo(t, unimplemented)
	}
}

// TestPGMetadataCanFixCode checks for parts of the code this file is checking with
// add-missing-tables flag to verify that a potential refactoring does not
// break the code.
func TestPGMetadataCanFixCode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// rewrite undefinedTables.
	validateUndefinedTablesField(t)
	validateTableDefsField(t)
	validateVirtualSchemaTable(t)
	validateFunctionNames(t)
}

// validateUndefinedTablesField checks the definition of virtualSchema objects
// (pg_catalog and information_schema) have a undefinedTables field which can
// be rewritten by this code.
func validateUndefinedTablesField(t *testing.T) {
	propertyIndex := strings.IndexRune(undefinedTablesDeclaration, ':')
	property := undefinedTablesDeclaration[:propertyIndex]
	// Using pgCatalog but information_schema is a virtualSchema as well.
	assertProperty(t, property, virtualSchemaType)
}

// validateTableDefsField checkes the definition of virtualSchema that
// have a tableDefs field which can be rewritten by this code.
func validateTableDefsField(t *testing.T) {
	propertyIndex := strings.IndexRune(tableDefsDeclaration, ':')
	property := tableDefsDeclaration[:propertyIndex]
	assertProperty(t, property, virtualSchemaType)
}

// validateVirtualSchemaTable checks that the template on virtualTableTemplate
// can produce a valid object.
func validateVirtualSchemaTable(t *testing.T) {
	lines := strings.Split(virtualTableTemplate, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		semicolonIndex := strings.IndexRune(line, ':')
		switch {
		case strings.HasPrefix(line, "var"):
			equalIndex := strings.IndexRune(line, '=')
			bracketIndex := strings.IndexRune(line, '{')
			templateTypeName := line[equalIndex+1 : bracketIndex]
			templateTypeName = strings.TrimSpace(templateTypeName)
			actualTypeName := virtualSchemaTableType.Name()
			if actualTypeName != templateTypeName {
				t.Fatalf(
					"virtualTableTemplate have a wrong name of type %s but actual type name is %s",
					templateTypeName,
					actualTypeName,
				)
			}
		case semicolonIndex != -1:
			property := strings.TrimSpace(line[:semicolonIndex])
			assertProperty(t, property, virtualSchemaTableType)
		}
	}
}

// validateFunctionNames prevents refactoring function names without changing
// the constants that looks for these names.
func validateFunctionNames(t *testing.T) {
	name := runtime.FuncForPC(reflect.ValueOf(makeAllRelationsVirtualTableWithDescriptorIDIndex).Pointer()).Name()
	// removing 'github.com/cockroachdb/cockroach/pkg/sql.' from the name.
	lastDotIndex := strings.LastIndex(name, ".")
	name = name[lastDotIndex+1:]
	if name != makeAllRelationsVirtualTableWithDescriptorIDIndexFunc {
		t.Fatalf("makeAllRelationsVirtualTableWithDescriptorIDIndexFunc constant value should be %s", name)
	}
}

// assertProperty checks the property (or field) exists in the given interface.
func assertProperty(t *testing.T, property string, rtype reflect.Type) {
	t.Run(fmt.Sprintf("assertProperty/%s", property), func(t *testing.T) {
		_, ok := rtype.FieldByName(property)
		if !ok {
			t.Fatalf("field %s is not a field of type %s", property, rtype.Name())
		}
	})
}
