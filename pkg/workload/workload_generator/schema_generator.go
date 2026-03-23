// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// Define regex patterns for identifying identifiers in SQL
	ident = `(?:"[^"]+"|[A-Za-z_][\w]*)` // Matches quoted or unquoted identifiers

	// Pattern to extract the column block from a CREATE TABLE DDL, tries to extract the part after the table name
	topLevelBlockPattern = `(?s)\((.*)\)\s*([^)]*)$`

	// Pattern to extract column properties from column definitions
	colPatternStr = `(?i)^\s*("?[^"]+"|[\w-]+)"?\s+([^\s]+)(?:\s+(NOT\s+NULL|NULL))?(?:\s+DEFAULT\s+((?:\([^\)]*\)|[^\s,]+)))?(?:\s+PRIMARY\s+KEY)?(?:\s+UNIQUE)?(?:\s+REFERENCES\s+([\w\.]+)\s*\(\s*([\w]+)\s*\))?(?:\s+CHECK\s*\(\s*(.*?)\s*\))?`

	// Patterns for inline constraints
	checkInlinePattern = `(?i)\bCHECK\s*\(`
	primaryKeyPattern  = `(?i)\bPRIMARY\s+KEY\b`
	uniquePattern      = `(?i)\bUNIQUE\b`
	computedPattern    = `(?i)\b(AS)\b`

	// Patterns for table-level constraints
	tablePrimaryKeyPattern = `\((.*?)\)`
	tableUniquePattern     = `\((.*?)\)`
	tableForeignKeyPattern = `(?i)FOREIGN\s+KEY\s*\(([^\)]*)\)\s+REFERENCES\s+((?:"[^"]+"|[\w]+)(?:\.(?:"[^"]+"|[\w]+))*)\s*\(([^\)]*)\)`
	tableCheckPattern      = `CHECK\s*\((.*)\)`

	databaseName    = "database_name"
	createStatement = "create_statement"
	constSchemaName = "schema_name"
	descriptorType  = "descriptor_type"
	descriptorName  = "descriptor_name"
)

var (
	// Fully qualified identifier (up to 3 parts)
	fullIdent = fmt.Sprintf(`(%s(?:\.%s){0,2})`, ident, ident)
	// Regex to match CREATE TABLE statements and capture table name
	tablePattern = regexp.MustCompile(`(?i)CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+` + fullIdent)
	// Compiled regexes for reuse
	bodyRe     = regexp.MustCompile(topLevelBlockPattern)
	colPattern = regexp.MustCompile(colPatternStr)
	//following regexes are for inline constraints
	checkInlineRe = regexp.MustCompile(checkInlinePattern)
	primaryKeyRe  = regexp.MustCompile(primaryKeyPattern)
	uniqueRe      = regexp.MustCompile(uniquePattern)
	computedRe    = regexp.MustCompile(computedPattern)
	//following regexes are for table constraint handling
	tablePrimaryKeyRe = regexp.MustCompile(tablePrimaryKeyPattern)
	tableUniqueRe     = regexp.MustCompile(tableUniquePattern)
	tableForeignKeyRe = regexp.MustCompile(tableForeignKeyPattern)
	tableCheckRe      = regexp.MustCompile(tableCheckPattern)

	//requiredColumns are the required columns in the create statements TSV file
	requiredColumns = []string{
		databaseName,
		createStatement,
		constSchemaName,
		descriptorType,
		descriptorName,
	}

	//regexes for normalizing CREATE TABLE statements
	ifNotExistsRe = regexp.MustCompile(`(?i)IF\s+NOT\s+EXISTS`)
	createTableRe = regexp.MustCompile(`(?i)^(CREATE\s+TABLE\s+)`)
)

// generateDDLs extracts and processes DDL statements from a CockroachDB debug zip file.
// It reads the create_statements.txt file from the zip directory, filters statements
// for the specified database, and writes them to an output file. It also parses each
// DDL statement into a TableSchema object and returns a map of table names to their schemas.
//
// Parameters:
//   - zipDir: Directory containing the debug zip contents
//   - dbName: Name of the database to extract DDLs for
//   - anonymize: Flag for future anonymization feature (currently unused)
//
// Returns:
//   - map[string]*TableSchema: Map of table names to their schema objects
//   - map[string]string createStmts: Map of short table names to their CREATE TABLE statements
//   - error: Any error encountered during processing
//
// TODO: The "anonymize" parameter is unused for now.
func generateDDLs(
	zipDir,
	dbName, ddlFile string, anonymize bool,
) (allSchemas map[string]*TableSchema, createStmts map[string]string, retErr error) {

	if ddlFile != "" {
		// DDL file location is present. We will use this instead of the debug zip.
		f, err := os.Open(ddlFile)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to open DDL file")
		}
		defer func() {
			if cerr := f.Close(); cerr != nil && retErr == nil {
				retErr = errors.Wrap(cerr, "failed to close input DDL file")
			}
		}()
		return generateDDLFromDDLFile(bufio.NewReader(f), dbName, anonymize)
	}
	f, err := openCreateStatementsTSV(zipDir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open TSV file")
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = errors.Wrap(cerr, "failed to close input TSV file")
		}
	}()

	return generateDDLFromCSV(bufio.NewReader(f), dbName, anonymize)
}

// generateDDLFromDDLFile reads DDL statements from a SQL dump file
// and returns a map of table names to their schemas and a map of
// short table names to their CREATE TABLE statements.
// The file can be generated by running the following:
//
//	cockroach sql --url='postgresql://<url>/<db name>' --execute="SHOW CREATE ALL TABLES;" > ddl_file.sql
func generateDDLFromDDLFile(
	reader *bufio.Reader, dbName string, anonymize bool,
) (map[string]*TableSchema, map[string]string, error) {
	// the results are stored in these Maps.
	tableStatements := make(map[string]string)
	order := make([]string, 0)
	seen := make(map[string]struct{})

	// Buffer accumulates the SQL statements
	var currentStmt strings.Builder
	// inStatement helps handling multi line statements
	inStatement := false

	// The file is read line by line
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, nil, errors.Wrap(err, "failed while reading SQL file")
		}

		// Empty lines and comments are skipped
		trimmedLine := strings.TrimSpace(line)
		if !inStatement {
			// The generated statement has a quote at the start of the statement. This is trimmed.
			trimmedLine = strings.TrimLeft(trimmedLine, "\"")
		}
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "--") ||
			strings.HasPrefix(trimmedLine, "create_statement") {
			continue
		}

		// A new statement is expected to start with CREATE TABLE.
		if strings.HasPrefix(strings.ToUpper(trimmedLine), "CREATE TABLE") {
			// If we were already in a statement, the previous statement is processed
			if inStatement {
				tableStatements, order, seen = processStatement(currentStmt.String(), tableStatements, order, seen, dbName)
			}

			// A new statement is started.
			currentStmt.Reset()
			currentStmt.WriteString(trimmedLine)
			inStatement = true
		} else if strings.HasPrefix(strings.ToUpper(trimmedLine), "ALTER TABLE") {
			// If we were in a CREATE TABLE statement, the statement is processed
			if inStatement {
				tableStatements, order, seen = processStatement(currentStmt.String(), tableStatements, order, seen, dbName)
			}

			// A new ALTER TABLE statement is started.
			currentStmt.Reset()
			currentStmt.WriteString(trimmedLine)
			inStatement = true
		} else if inStatement {
			if strings.HasSuffix(trimmedLine, ";\"") {
				// The generated statement has a quote at the end of the statement. This needs to be trimmed.
				trimmedLine = strings.TrimRight(trimmedLine, "\"")
			}
			// The current statement is accumulated.
			currentStmt.WriteString(trimmedLine)

			// if the statement is complete (ends with semicolon or has closing parenthesis followed by options), it is processed.
			if strings.HasSuffix(trimmedLine, ";") ||
				(strings.Contains(trimmedLine, ");") && !strings.HasPrefix(trimmedLine, "--")) {
				tableStatements, order, seen = processStatement(currentStmt.String(), tableStatements, order, seen, dbName)
				inStatement = false
			}
		}
	}

	// Any remaining statement is processed.
	if inStatement {
		tableStatements, order, _ = processStatement(currentStmt.String(), tableStatements, order, seen, dbName)
	}

	return buildSchemas(order, tableStatements), buildCreateStmts(tableStatements), nil
}

// processStatement processes a single SQL statement and adds it to the tableStatements map if it's a CREATE TABLE statement
// It returns the updated tableStatements, order, and seen maps
func processStatement(
	stmt string,
	tableStatements map[string]string,
	order []string,
	seen map[string]struct{},
	dbName string,
) (map[string]string, []string, map[string]struct{}) {
	// Only process CREATE TABLE statements
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "CREATE TABLE") {
		return tableStatements, order, seen
	}

	// Extract the table name using the tablePattern regex
	tableMatch := tablePattern.FindStringSubmatch(stmt)
	if tableMatch == nil {
		return tableStatements, order, seen
	}

	// Extract and normalize the table name
	tableName := tableMatch[1]
	parts := strings.Split(tableName, ".")
	for i := range parts {
		parts[i] = strings.Trim(parts[i], `"`) // Remove quotes from parts
	}

	// If the table name doesn't have a schema, assume it's "public"
	var schemaName string
	var simpleTableName string

	if len(parts) == 1 {
		schemaName = "public"
		simpleTableName = parts[0]
	} else if len(parts) == 2 {
		schemaName = parts[0]
		simpleTableName = parts[1]
	} else {
		// Skip tables with more complex names
		return tableStatements, order, seen
	}

	// Create a regex for the schema name
	schemaPattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(schemaName) + `\.`)

	// Process the DDL record
	fullTableName, statement := processDDLRecord(dbName, schemaName, simpleTableName, stmt, schemaPattern)

	// Add to the maps if not seen before
	if _, ok := seen[fullTableName]; !ok && fullTableName != "" {
		tableStatements[fullTableName] = statement
		order = append(order, fullTableName)
		seen[fullTableName] = struct{}{}
	}

	return tableStatements, order, seen
}

// generateDDLFromCSV takes a reader for a TSV file containing DDL statements,
// parses the statements, and returns a map of table names to their schemas
// and a map of short table names to their CREATE TABLE statements.
// It has been deigned this way to maek it unit-testable
func generateDDLFromCSV(
	r io.Reader, dbName string, anonymize bool,
) (map[string]*TableSchema, map[string]string, error) {
	reader := csv.NewReader(r)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed reading TSV header")
	}
	colIndex := map[string]int{}
	for i, col := range header {
		colIndex[col] = i
	}
	for _, c := range requiredColumns {
		if _, ok := colIndex[c]; !ok {
			return nil, nil, fmt.Errorf("missing column %s", c)
		}
	}

	// collect raw statements
	tableStatements := make(map[string]string)
	order := make([]string, 0)
	// seen is maintained to ensure that a table is processed only once.
	// There is a possibility that the table was re-created multiple times.
	seen := make(map[string]struct{})
	// schemaReCache is maintained to avoid recompiling regexes for each schema name
	// encountered in the TSV file.
	schemaReCache := map[string]*regexp.Regexp{}

	for {
		rec, err := reader.Read()
		if err != nil {
			if errors.Is(err, os.ErrClosed) || err.Error() == "EOF" {
				break
			}
			if len(rec) == 0 {
				break
			}
			return nil, nil, errors.Wrap(err, "failed while reading TSV rows")
		}
		if len(rec) == 0 {
			break
		}
		// 1) Quick filter
		if rec[colIndex[databaseName]] != dbName ||
			rec[colIndex[descriptorType]] != "table" ||
			rec[colIndex[constSchemaName]] != "public" {
			continue
		}

		schemaName := rec[colIndex[constSchemaName]]
		_, ok := schemaReCache[schemaName]
		if !ok {
			schemaReCache[schemaName] = regexp.MustCompile(`\b` + regexp.QuoteMeta(schemaName) + `\.`)
		}

		// 2) Build identifiers
		fullTableName, statement := processDDLRecord(dbName, schemaName,
			rec[colIndex[descriptorName]],  // table name
			rec[colIndex[createStatement]], // statement
			schemaReCache[schemaName])
		if _, ok := seen[fullTableName]; !ok && fullTableName != "" {
			// 5) Record ordering & statement
			tableStatements[fullTableName] = statement
			order = append(order, fullTableName)
			seen[fullTableName] = struct{}{}
		}
	}
	return buildSchemas(order, tableStatements), buildCreateStmts(tableStatements), nil
}

// ParseDDL converts a "CREATE TABLE ..." DDL statement into a TableSchema.
// It parses the table name, columns, and constraints (primary keys, unique constraints,
// foreign keys, and check constraints) from the DDL statement and returns a structured
// representation of the table schema.
// TODO: (@nameisbhaskar) use sql parser instead of regexes - https://github.com/cockroachdb/cockroach/issues/155173
func ParseDDL(ddl string) (*TableSchema, error) {
	tableMatch := tablePattern.FindStringSubmatch(ddl)
	if tableMatch == nil {
		return nil, errors.New("no table name")
	}
	// Extract and normalize the table name
	tableName := tableMatch[1]
	parts := strings.Split(tableName, ".")
	for i := range parts {
		parts[i] = strings.Trim(parts[i], `"`) // Remove quotes from parts
	}
	rawName := tableName                 // Save the original table name
	tableName = strings.Join(parts, ".") // Reconstruct normalized table name

	// Create a new TableSchema with the normalized name
	table := NewTableSchema(tableName, rawName)

	// Extract the column definitions block from the DDL
	columnBlockMatch := bodyRe.FindStringSubmatch(ddl)
	if columnBlockMatch == nil {
		return nil, errors.New("no column block")
	}
	body := columnBlockMatch[1]

	columnDefs, tableConstraints := splitColumnDefsAndTableConstraints(body)
	// Process column definitions and add them to the table schema
	processColumnDefs(table, columnDefs)
	// Process table-level constraints
	processTableConstraints(table, tableConstraints)
	return table, nil
}

// splitColumnDefsAndTableConstraints takes the raw "(…)" body of a CREATE TABLE
// and returns two slices: one of column definitions, and one of table-level constraints.
func splitColumnDefsAndTableConstraints(body string) (colDefs, tableConstraints []string) {
	// Split the body into parts based on commas, respecting parentheses
	var partsList []string
	buf := ""
	depth := 0
	for _, ch := range body {
		switch ch {
		case '(':
			depth++
			buf += string(ch)
		case ')':
			depth--
			buf += string(ch)
		case ',':
			if depth == 0 {
				partsList = append(partsList, strings.TrimSpace(buf))
				buf = ""
			} else {
				buf += string(ch)
			}
		default:
			buf += string(ch)
		}
	}
	if strings.TrimSpace(buf) != "" {
		partsList = append(partsList, strings.TrimSpace(buf))
	}
	// Separate column definitions from table-level constraints
	for _, p := range partsList {
		up := strings.ToUpper(strings.TrimSpace(p))
		if hasConstrainingPrefix(up) {
			tableConstraints = append(tableConstraints, p)
		} else {
			colDefs = append(colDefs, p)
		}
	}
	return colDefs, tableConstraints
}

// hasConstrainingPrefix reports whether the upper‐cased, trimmed
// line should be treated as a table‐level constraint.
func hasConstrainingPrefix(up string) bool {
	// You could make this a global var if you like, to avoid reallocating the slice.
	prefixes := []string{
		sqlConstraint,
		sqlPrimaryKey,
		sqlUnique,
		sqlForeignKey,
		sqlCheck,
		sqlIndex,
		sqlFamily,
	}
	for _, p := range prefixes {
		// a space is added after the prefix to avoid matches like "checksum" as a field name.
		if strings.HasPrefix(up, p+" ") {
			return true
		}
	}
	return false
}

// processColumnDefs parses column definitions, updates the TableSchema with Columns,
// and sets inline primary keys on the schema.
func processColumnDefs(table *TableSchema, columnDefs []string) {
	inlinePKCols := make([]string, 0)
	// Process each column definition
	for _, columnDef := range columnDefs {
		colMatch := colPattern.FindStringSubmatch(columnDef)
		if colMatch == nil || computedRe.MatchString(columnDef) {
			continue // Skip if pattern doesn't match
		}
		// Extract column properties from regex matches
		name := colMatch[1]             // Column name
		columnType := colMatch[2]       // Column type
		nullSpec := colMatch[3]         // NULL or NOT NULL specification
		defaultVal := colMatch[4]       // DEFAULT value
		foreignKeyTable := colMatch[5]  // Referenced table for foreign keys
		foreignKeyColumn := colMatch[6] // Referenced column for foreign keys
		// While adding columns to the order, surrounding quotes are stripped if any.
		// This is to ensure that column names here match with the column names used as keys in schema maps.
		table.ColumnOrder = append(table.ColumnOrder, strings.Trim(name, `"`))
		// Extract CHECK constraint if present (requires special handling for nested parentheses)
		inlineCheck := ""
		checkIdx := checkInlineRe.FindStringIndex(columnDef)
		if checkIdx != nil {
			start := checkIdx[1]
			depth := 1
			i := start
			for i < len(columnDef) && depth > 0 {
				switch columnDef[i] {
				case '(':
					depth++
				case ')':
					depth--
				}
				i++
			}
			inlineCheck = strings.TrimSpace(columnDef[start : i-1])
		}
		// Determine some column properties
		isNullable := nullSpec == "" || strings.ToUpper(nullSpec) == "NULL"
		isUnique := uniqueRe.MatchString(columnDef)
		isPK := primaryKeyRe.MatchString(columnDef)

		// Handle inline PRIMARY KEY constraints
		if isPK {
			inlinePKCols = append(inlinePKCols, strings.Trim(name, `"`))
			isNullable = false // PRIMARY KEY columns cannot be NULL
			isUnique = true    // PRIMARY KEY columns are implicitly UNIQUE
		}
		// Create and populate the Column object
		col := &Column{
			Name:         strings.Trim(name, `"`),
			ColType:      columnType,
			IsNullable:   isNullable,
			IsPrimaryKey: isPK,
			Default:      strings.TrimSpace(defaultVal),
			IsUnique:     isUnique,
		}
		// Add foreign key information if present
		if foreignKeyTable != "" {
			col.FKTable = foreignKeyTable
			col.FKColumn = foreignKeyColumn
		}
		// Add CHECK constraint if present
		if inlineCheck != "" {
			col.InlineCheck = inlineCheck
			table.CheckConstraints = append(table.CheckConstraints, inlineCheck)
		}
		table.AddColumn(col)
	}

	if len(inlinePKCols) > 0 {
		table.SetPrimaryKeys(inlinePKCols)
	}
}

// applyPrimaryKey processes a table-level PRIMARY KEY constraint and sets it on the TableSchema.
func applyPrimaryKey(table *TableSchema, tableConstraint string) {
	raw := tablePrimaryKeyRe.FindStringSubmatch(tableConstraint)
	if raw != nil {
		cols := make([]string, 0)
		// Extract column names from the constraint
		for _, col := range strings.Split(raw[1], ",") {
			cols = append(cols, strings.Split(strings.TrimSpace(strings.Trim(col, `"`)), " ")[0])
		}
		// Set primary keys at table level
		table.SetPrimaryKeys(cols)
	}
}

// applyUniqueConstraint processes a table-level UNIQUE constraint and sets it on the TableSchema.
func applyUniqueConstraint(table *TableSchema, tableConstraint string) {
	raw := tableUniqueRe.FindStringSubmatch(tableConstraint)
	if raw != nil {
		cols := make([]string, 0)
		// Extract column names from the constraint
		for _, col := range strings.Split(raw[1], ",") {
			cols = append(cols, strings.Split(strings.TrimSpace(strings.Trim(col, `"`)), " ")[0])
		}
		// Add to table's unique constraints
		table.UniqueConstraints = append(table.UniqueConstraints, cols)

		// For single-column unique constraints, mark the column as unique
		isComposite := len(cols) > 1
		for _, c := range cols {
			if !isComposite {
				if col, ok := table.Columns[c]; ok {
					col.IsUnique = true
				}
			}
		}
	}
}

// applyForeignKey processes a table-level FOREIGN KEY constraint and sets it on the TableSchema.
func applyForeignKey(table *TableSchema, tableConstraint string) {
	foreignKeyMatch := tableForeignKeyRe.FindStringSubmatch(tableConstraint)
	if foreignKeyMatch != nil {
		local := make([]string, 0)
		// Extract local columns (referencing columns)
		for _, c := range strings.Split(foreignKeyMatch[1], ",") {
			local = append(local, strings.TrimSpace(c))
		}
		// Extract referenced table name
		tbl := strings.TrimSpace(foreignKeyMatch[2])
		tblRaw := strings.ReplaceAll(tbl, "\"", "")
		// Extract referenced columns
		foreign := make([]string, 0)
		for _, c := range strings.Split(foreignKeyMatch[3], ",") {
			foreign = append(foreign, strings.TrimSpace(c))
		}
		// Add to table's foreign keys
		table.ForeignKeys = append(table.ForeignKeys, [3]interface{}{local, tblRaw, foreign})
	}
}

// applyCheckConstraint processes a table-level CHECK constraint and adds it to the TableSchema.
func applyCheckConstraint(table *TableSchema, tableConstraint string) {
	m2 := tableCheckRe.FindStringSubmatch(tableConstraint)
	if m2 != nil {
		table.CheckConstraints = append(table.CheckConstraints, strings.TrimSpace(m2[1]))
	}
}

// processTableConstraints applies all table-level constraints (PK, UNIQUE, FK, CHECK).
func processTableConstraints(table *TableSchema, tableConstraints []string) {
	for _, tableConstraint := range tableConstraints {
		up := strings.ToUpper(tableConstraint)

		// Handle PRIMARY KEY constraints
		if strings.Contains(up, sqlPrimaryKey) {
			applyPrimaryKey(table, tableConstraint)

			// Handle UNIQUE constraints
		} else if strings.Contains(up, sqlUnique) {
			applyUniqueConstraint(table, tableConstraint)

			// Handle FOREIGN KEY constraints
		} else if strings.Contains(up, sqlForeignKey) {
			applyForeignKey(table, tableConstraint)

			// Handle CHECK constraints
		} else if strings.Contains(up, sqlCheck) {
			applyCheckConstraint(table, tableConstraint)

		} // Skip INDEX definitions (not relevant for schema representation)
	}
}

// buildCreateStmts returns a map of simple table name → raw CREATE TABLE statement.
func buildCreateStmts(tableStatements map[string]string) map[string]string {
	createStmts := make(map[string]string, len(tableStatements))
	for full, stmt := range tableStatements {
		parts := strings.Split(full, ".")
		simple := parts[len(parts)-1]
		createStmts[simple] = stmt
	}
	return createStmts
}

// buildSchemas parses each statement in order, numbers them 0…N-1, and returns
// simple table name → *TableSchema.
func buildSchemas(order []string, tableStatements map[string]string) map[string]*TableSchema {
	schemas := make(map[string]*TableSchema, len(order))
	tableNo := 0
	for _, fullTable := range order {
		stmt := tableStatements[fullTable]
		schema, err := ParseDDL(stmt)
		if err != nil {
			continue
		}
		schema.TableNumber = tableNo
		tableNo++
		name := fullTable[strings.LastIndex(fullTable, ".")+1:]
		schemas[name] = schema
	}
	return schemas
}

// openCreateStatementsTSV opens the debug-zip’s create_statements.txt file,
// verifying its existence and wrapping any errors.
func openCreateStatementsTSV(zipDir string) (*os.File, error) {
	path := filepath.Join(zipDir, "crdb_internal.create_statements.txt")
	if _, err := os.Stat(path); err != nil {
		return nil, errors.Wrap(err, "could not find TSV file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open TSV file")
	}
	return f, nil
}

// removeComputedColumns removes computed columns (columns with AS clause) from a CREATE TABLE statement.
// It parses the statement, identifies computed columns, and reconstructs the statement without them.
// It also removes any indexes or constraints that reference the computed columns.
func removeComputedColumns(stmt string) string {
	columnBlockMatch := bodyRe.FindStringSubmatch(stmt)
	if columnBlockMatch == nil {
		return stmt
	}

	body := columnBlockMatch[1]
	suffix := columnBlockMatch[2]

	// column definitions and table constraints are split based on commas
	var partsList []string
	buf := ""
	depth := 0
	for _, ch := range body {
		switch ch {
		case '(':
			depth++
			buf += string(ch)
		case ')':
			depth--
			buf += string(ch)
		case ',':
			if depth == 0 {
				partsList = append(partsList, strings.TrimSpace(buf))
				buf = ""
			} else {
				buf += string(ch)
			}
		default:
			buf += string(ch)
		}
	}
	if strings.TrimSpace(buf) != "" {
		partsList = append(partsList, strings.TrimSpace(buf))
	}

	// computed column names are collected for reference
	computedColumnNames := make(map[string]struct{})
	for _, p := range partsList {
		// Computed column (contains AS keyword) is identified using the computedRe regex
		// Example: "haserror BOOL NULL AS (errordetail != '':::STRING) VIRTUAL"
		// This will match because it contains "AS"
		// Note: This is a simple heuristic and may need to be improved for complex cases
		// where "AS" might appear in other contexts.
		// The regex is case-insensitive to match "AS", "as", "As", etc.
		// It looks for the word "AS" surrounded by word boundaries to avoid partial matches.
		// This approach assumes that computed columns are defined with the "AS" keyword.
		// If the SQL dialect changes or has different syntax, this may need to be adjusted.
		// The regex is applied to each part of the column block to identify computed columns.
		// If a part matches, the column name is extracted and added to the computedColumnNames map.
		if computedRe.MatchString(p) {
			colMatch := colPattern.FindStringSubmatch(p)
			if len(colMatch) > 1 {
				colName := strings.Trim(colMatch[1], `"`)
				computedColumnNames[colName] = struct{}{}
			}
		}
	}

	// Computed columns and constraints/indexes that reference them are filtered out
	var filteredParts []string
	for _, p := range partsList {
		// Skip computed columns
		if computedRe.MatchString(p) {
			continue
		}

		// If this is an index or constraint that references a computed column, it is skipped.
		shouldSkip := false
		pUpper := strings.ToUpper(p)
		if strings.HasPrefix(pUpper, "INDEX ") || strings.HasPrefix(pUpper, "UNIQUE INDEX ") {
			// Computed column names are checked in the index definition
			// If any computed column is found, the index is skipped
			// This is a simple substring check and may need to be improved for complex cases
			// where column names might be part of other identifiers.
			// For example, if a computed column is "col1", an index on "col10" should not be skipped.
			for colName := range computedColumnNames {
				// A regex pattern to match the column name in the index definition is created.
				pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(colName) + `\b`)
				if pattern.MatchString(p) {
					shouldSkip = true
					break
				}
			}
		}

		if !shouldSkip {
			filteredParts = append(filteredParts, p)
		}
	}

	// If all parts were filtered out, return original statement
	if len(filteredParts) == 0 {
		return stmt
	}

	// The statement is reconstructed without computed columns and related constraints/indexes
	// The original formatting (indentation, line breaks) is preserved as much as possible
	// by joining the filtered parts with commas and new lines.
	tableMatch := tablePattern.FindStringSubmatch(stmt)
	if tableMatch == nil {
		return stmt
	}

	// The CREATE TABLE is the part before the column block
	createTablePart := stmt[:strings.Index(stmt, "(")]
	reconstructed := createTablePart + "(\n\t" + strings.Join(filteredParts, ",\n\t") + "\n)" + suffix

	return reconstructed
}

// processDDLRecord inspects one TSV row and, if it represents a public table
// in dbName, normalizes its CREATE TABLE stmt and appends it to order/statements.
// It returns the fully qualified table name and table statements.
func processDDLRecord(
	dbName, schemaName, tableName, stmt string, pattern *regexp.Regexp,
) (string, string) {
	// 3) Normalize schema-qualified references
	stmt = pattern.ReplaceAllString(stmt, dbName+"."+schemaName+".")

	// 4) Ensure IF NOT EXISTS
	if !ifNotExistsRe.MatchString(stmt) {
		stmt = createTableRe.ReplaceAllString(stmt, "${1}IF NOT EXISTS ")
	}

	// 5) Remove computed columns from the statement
	stmt = removeComputedColumns(stmt)

	return fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName), stmt
}

// buildWorkloadSchema constructs the complete workload schema used for data generation.
// It takes each parsed TableSchema and produces one or more TableBlock entries per table,
// wiring up foreign‐key relationships and scaling row counts appropriately.
//
// The steps are as follows:
//  1. buildInitialBlocks:
//     • Creates a TableBlock for each table with a baseline row count (baseRowCount).
//     • Converts each Column into ColumnMeta (type, null probability, default, etc.).
//     • Collects “seeds” for foreign‐key columns to enable parent→child linkage.
//  2. wireForeignKeys:
//     • Scans each TableSchema’s ForeignKeys and populates the corresponding ColumnMeta. FK,
//     FKMode, Fanout, CompositeID, and ParentSeed entries.
//  3. adjustFanoutForPureFKPKs:
//     • If a table’s every primary‐key column is also a foreign key, drops its fan-out to 1,
//     ensuring exactly one child per parent in that “pure FK–PK” scenario.
//  4. computeRowCounts:
//     • For each table, computes the total row count by multiplying baseRowCount
//     by the smallest product of FK fan-outs, ensuring referential integrity.
//
// Parameters:
//   - allSchemas:    map of simple table name → *TableSchema, parsed from the DDL.
//   - dbName:        the database name (used for qualifying schema references).
//   - baseRowCount:  the initial number of rows per table before applying FK scaling.
//
// Returns:
//   - Schema: the finalized workload schema, mapping each table name to a slice of one
//     or more TableBlock, each of which drives per-batch generators with the
//     correct column metadata and row counts.
func buildWorkloadSchema(
	allSchemas map[string]*TableSchema, dbName string, baseRowCount int,
) Schema {
	// Initialize RNG for seeding and composite IDs
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	// 1) Build initial blocks and capture FK seeds
	blocks, fkSeed := buildInitialBlocks(allSchemas, dbName, rng, baseRowCount)

	applyCheckConstraints(blocks, allSchemas)

	// 2) Wire up foreign-key relationships in the blocks
	wireForeignKeys(blocks, allSchemas, fkSeed, rng)

	// 3) If a table's PK cols are all FKs, drop its fanout to 1
	adjustFanoutForPureFKPKs(blocks)

	// 4) Recompute each block's row count based on FK fanouts
	computeRowCounts(blocks, baseRowCount)

	return blocks
}

// listDatabases scans the debug logs TSV file and returns a list of all unique
// database names found. It filters out system databases (system, postgres, defaultdb).
func listDatabases(zipDir string) ([]string, error) {
	f, err := openCreateStatementsTSV(zipDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open TSV file")
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = errors.Wrap(cerr, "failed to close TSV file")
		}
	}()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = '\t'
	reader.LazyQuotes = true

	// Read the header row to get column indices
	header, err := reader.Read()
	if err != nil {
		return nil, errors.Wrap(err, "failed reading TSV header")
	}
	colIndex := map[string]int{}
	for i, col := range header {
		colIndex[col] = i
	}

	// Check if database_name column exists
	dbColIdx, ok := colIndex[databaseName]
	if !ok {
		return nil, errors.New("missing database_name column in TSV file")
	}

	// Collect unique database names
	dbSet := make(map[string]struct{})
	systemDBs := map[string]struct{}{
		"system":    {},
		"postgres":  {},
		"defaultdb": {},
		"NULL":      {},
	}

	for {
		rec, err := reader.Read()
		if err != nil {
			if errors.Is(err, os.ErrClosed) || err.Error() == "EOF" {
				break
			}
			if len(rec) == 0 {
				break
			}
			return nil, errors.Wrap(err, "failed while reading TSV rows")
		}
		if len(rec) == 0 {
			break
		}

		dbName := rec[dbColIdx]
		// Add databases not in the systemDBs set
		if _, isSystem := systemDBs[dbName]; !isSystem && dbName != "" {
			dbSet[dbName] = struct{}{}
		}
	}

	// Convert set to sorted slice
	databases := make([]string, 0, len(dbSet))
	for db := range dbSet {
		databases = append(databases, db)
	}
	sort.Strings(databases)

	return databases, nil
}
