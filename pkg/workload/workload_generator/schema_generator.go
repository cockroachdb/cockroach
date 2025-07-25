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
	dbName string, anonymize bool,
) (allSchemas map[string]*TableSchema, createStmts map[string]string, retErr error) {

	f, err := openCreateStatementsTSV(zipDir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open TSV file")
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = errors.Wrap(cerr, "failed to close input TSV file")
		}
	}()

	return generateDDLFromReader(bufio.NewReader(f), dbName, anonymize)
}

// generateDDLFromReader takes a reader for a TSV file containing DDL statements,
// parses the statements, and returns a map of table names to their schemas
// and a map of short table names to their CREATE TABLE statements.
// It has been deigned this way to maek it unit-testable
func generateDDLFromReader(
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
	seen := map[string]bool{}
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

		processDDLRecord(
			rec,
			colIndex,
			dbName,
			schemaReCache,
			seen,
			&order,
			tableStatements,
		)
	}
	return buildSchemas(order, tableStatements), buildCreateStmts(tableStatements), nil
}

// ParseDDL converts a "CREATE TABLE ..." DDL statement into a TableSchema.
// It parses the table name, columns, and constraints (primary keys, unique constraints,
// foreign keys, and check constraints) from the DDL statement and returns a structured
// representation of the table schema.
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
		if strings.HasPrefix(up, p) {
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
		if colMatch == nil {
			continue // Skip if pattern doesn't match
		}
		// Extract column properties from regex matches
		name := colMatch[1]             // Column name
		columnType := colMatch[2]       // Column type
		nullSpec := colMatch[3]         // NULL or NOT NULL specification
		defaultVal := colMatch[4]       // DEFAULT value
		foreignKeyTable := colMatch[5]  // Referenced table for foreign keys
		foreignKeyColumn := colMatch[6] // Referenced column for foreign keys

		table.ColumnOrder = append(table.ColumnOrder, name)
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

// processDDLRecord inspects one TSV row and, if it represents a public table
// in dbName, normalizes its CREATE TABLE stmt and appends it to order/statements.
func processDDLRecord(
	rec []string,
	colIndex map[string]int,
	dbName string,
	schemaReCache map[string]*regexp.Regexp,
	seen map[string]bool,
	order *[]string,
	tableStatements map[string]string,
) {
	// 1) Quick filter
	if rec[colIndex[databaseName]] != dbName ||
		rec[colIndex[descriptorType]] != "table" ||
		rec[colIndex[constSchemaName]] != "public" {
		return
	}

	// 2) Build identifiers
	schemaName := rec[colIndex[constSchemaName]]
	stmt := rec[colIndex[createStatement]]
	tableName := rec[colIndex[descriptorName]]
	fullTable := fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName)

	// 3) Normalize schema-qualified references
	pattern, ok := schemaReCache[schemaName]
	if !ok {
		pattern = regexp.MustCompile(`\b` + regexp.QuoteMeta(schemaName) + `\.`)
		schemaReCache[schemaName] = pattern
	}
	stmt = pattern.ReplaceAllString(stmt, dbName+"."+schemaName+".")

	// 4) Ensure IF NOT EXISTS
	if !ifNotExistsRe.MatchString(stmt) {
		stmt = createTableRe.ReplaceAllString(stmt, "${1}IF NOT EXISTS ")
	}

	// 5) Record ordering & statement
	if !seen[fullTable] {
		*order = append(*order, fullTable)
		seen[fullTable] = true
	}
	tableStatements[fullTable] = stmt
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

	// 2) Wire up foreign-key relationships in the blocks
	wireForeignKeys(blocks, allSchemas, fkSeed, rng)

	// 3) If a table's PK cols are all FKs, drop its fanout to 1
	adjustFanoutForPureFKPKs(blocks)

	// 4) Recompute each block's row count based on FK fanouts
	computeRowCounts(blocks, baseRowCount)

	return blocks
}
