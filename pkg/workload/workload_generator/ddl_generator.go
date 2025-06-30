package workload_generator

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	// Define regex patterns for identifying identifiers in SQL
	ident = `(?:"[^"]+"|[A-Za-z_][\w]*)` // Matches quoted or unquoted identifiers

	// Pattern to extract the column block from a CREATE TABLE DDL
	bodyPattern = `(?s)\((.*)\)\s*([^)]*)$`

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
	schemaName      = "schema_name"
	descriptorType  = "descriptor_type"
	descriptorName  = "descriptor_name"
)

var (
	// Fully qualified identifier (up to 3 parts)
	fullIdent = fmt.Sprintf(`(%s(?:\.%s){0,2})`, ident, ident)
	// Regex to match CREATE TABLE statements and capture table name
	tablePattern = regexp.MustCompile(`(?i)CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+` + fullIdent)
	// Compiled regexes for reuse
	bodyRe     = regexp.MustCompile(bodyPattern)
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

	req = []string{
		databaseName,
		createStatement,
		schemaName,
		descriptorType,
		descriptorName,
	}

	ifNotExistsRe = regexp.MustCompile(`(?i)IF\s+NOT\s+EXISTS`)
	createTableRe = regexp.MustCompile(`(?i)^(CREATE\s+TABLE\s+)`)
)

// splitColumnAndConstraints takes the raw "(…)" body of a CREATE TABLE
// and returns two slices: one of column definitions, and one of table-level constraints.
func splitColumnDefsAndTableConstraints(body string) (colDefs, tableConstraints []string) {
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
		if strings.HasPrefix(up, "CONSTRAINT") || strings.HasPrefix(up, "PRIMARY KEY") || strings.HasPrefix(up, "UNIQUE") || strings.HasPrefix(up, "FOREIGN KEY") || strings.HasPrefix(up, "CHECK") || strings.HasPrefix(up, "INDEX") {
			tableConstraints = append(tableConstraints, p)
		} else {
			colDefs = append(colDefs, p)
		}
	}
	return colDefs, tableConstraints
}

// processColumnDefs parses column definitions, updates the TableSchema with Columns,
// and sets inline primary keys on the schema.
func processColumnDefs(table *TableSchema, columnDefs []string) {
	inlinePKCols := make([]string, 0)
	// Process each column definition
	for _, columnDef := range columnDefs {
		m := colPattern.FindStringSubmatch(columnDef)
		if m == nil {
			continue // Skip if pattern doesn't match
		}
		// Extract column properties from regex matches
		name := m[1]             // Column name
		columnType := m[2]       // Column type
		nullSpec := m[3]         // NULL or NOT NULL specification
		defaultVal := m[4]       // DEFAULT value
		foreignKeyTable := m[5]  // Referenced table for foreign keys
		foreignKeyColumn := m[6] // Referenced column for foreign keys

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

func applyPrimaryKey(table *TableSchema, tableConstraint string) {
	raw := tablePrimaryKeyRe.FindStringSubmatch(tableConstraint)
	if raw != nil {
		cols := []string{}
		// Extract column names from the constraint
		for _, col := range strings.Split(raw[1], ",") {
			cols = append(cols, strings.Split(strings.TrimSpace(strings.Trim(col, `"`)), " ")[0])
		}
		// Set primary keys at table level
		table.SetPrimaryKeys(cols)
	}
}

func applyUniqueConstraint(table *TableSchema, tableConstraint string) {
	raw := tableUniqueRe.FindStringSubmatch(tableConstraint)
	if raw != nil {
		cols := []string{}
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

func applyForeignKey(table *TableSchema, tableConstraint string) {
	foreignKeyMatch := tableForeignKeyRe.FindStringSubmatch(tableConstraint)
	if foreignKeyMatch != nil {
		local := []string{}
		// Extract local columns (referencing columns)
		for _, c := range strings.Split(foreignKeyMatch[1], ",") {
			local = append(local, strings.TrimSpace(c))
		}
		// Extract referenced table name
		tbl := strings.TrimSpace(foreignKeyMatch[2])
		tblRaw := strings.ReplaceAll(tbl, "\"", "")
		// Extract referenced columns
		foreign := []string{}
		for _, c := range strings.Split(foreignKeyMatch[3], ",") {
			foreign = append(foreign, strings.TrimSpace(c))
		}
		// Add to table's foreign keys
		table.ForeignKeys = append(table.ForeignKeys, [3]interface{}{local, tblRaw, foreign})
	}
}

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
			continue
		}
		// Handle UNIQUE constraints
		if strings.Contains(up, sqlUnique) {
			applyUniqueConstraint(table, tableConstraint)
			continue
		}
		// Handle FOREIGN KEY constraints
		if strings.Contains(up, sqlForeignKey) {
			applyForeignKey(table, tableConstraint)
			continue
		}
		// Handle CHECK constraints
		if strings.Contains(up, sqlCheck) {
			applyCheckConstraint(table, tableConstraint)
			continue
		}
		// Skip INDEX definitions (not relevant for schema representation)
		if strings.HasPrefix(up, sqlIndex) {
			continue
		}
	}
}

// ParseDDL converts a "CREATE TABLE ..." DDL statement into a TableSchema.
// It parses the table name, columns, and constraints (primary keys, unique constraints,
// foreign keys, and check constraints) from the DDL statement and returns a structured
// representation of the table schema.
func ParseDDL(ddl string) (*TableSchema, error) {
	// Create regex to match CREATE TABLE statements and extract the table name
	m := tablePattern.FindStringSubmatch(ddl)
	if m == nil {
		return nil, errors.New("no table name")
	}
	// Extract and normalize the table name
	tableName := m[1]
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

// GenerateDDLs extracts and processes DDL statements from a CockroachDB debug zip file.
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
// The "anonymize" parameter is unused for now: on TODO list
func GenerateDDLs(
	zipDir,
	dbName string, anonymize bool,
) (allSchemas map[string]*TableSchema, createStmts map[string]string, retErr error) {
	filePath := filepath.Join(zipDir, "crdb_internal.create_statements.txt")
	if _, err := os.Stat(filePath); err != nil {
		return nil, nil, errors.Wrap(err, "could not find TSV file")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open TSV file")
	}
	// Defer closes (input and output) propagate errors if no prior error occurred.
	defer func() {
		if cerr := f.Close(); cerr != nil && retErr == nil {
			retErr = errors.Wrap(cerr, "failed to close input TSV file")
		}
	}()

	reader := csv.NewReader(bufio.NewReader(f))
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

	for _, c := range req {
		if _, ok := colIndex[c]; !ok {
			// Not wrapping an error, so fmt.Errorf is acceptable here.
			return nil, nil, fmt.Errorf("missing column %s", c)
		}
	}
	currTableNo := 0                             // Current table number for internal use
	tableStatements := make(map[string]string)   // Maps full table names to CREATE TABLE statements
	order := []string{}                          // Preserves the order of tables for output
	seen := map[string]bool{}                    // Tracks which tables have been seen to avoid duplicates
	schemaReCache := map[string]*regexp.Regexp{} // Cache for compiled regex patterns to improve performance

	// Read and process each row from the TSV file
	for {
		rec, err := reader.Read()
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				break
			}
			if err.Error() == "EOF" {
				break
			}
			// Only break if line is empty AND error, not if just error (to protect partial final line)
			if len(rec) == 0 {
				break
			}
			// Any other error (besides EOF) is fatal for TSV import and should be reported
			return nil, nil, errors.Wrap(err, "failed while reading TSV rows")
		}
		if len(rec) == 0 {
			break
		}
		// Filter for tables in the specified database and public schema
		if rec[colIndex[databaseName]] == dbName && rec[colIndex[descriptorType]] == "table" && rec[colIndex[schemaName]] == "public" {
			schemaName := rec[colIndex[schemaName]]
			stmt := rec[colIndex[createStatement]]
			tableName := rec[colIndex[descriptorName]]
			fullTable := fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName)
			// Get or create regex pattern for schema name replacement
			pattern, ok := schemaReCache[schemaName]
			if !ok {
				pattern = regexp.MustCompile(`\b` + regexp.QuoteMeta(schemaName) + `\.`)
				schemaReCache[schemaName] = pattern
			}
			stmt = pattern.ReplaceAllString(stmt, dbName+"."+schemaName+".")
			// Add IF NOT EXISTS to CREATE TABLE statements if not already present
			if !ifNotExistsRe.MatchString(stmt) {
				stmt = createTableRe.ReplaceAllString(stmt, "${1}IF NOT EXISTS ")
			}
			// Track table order and store the statement
			if !seen[fullTable] {
				order = append(order, fullTable)
				seen[fullTable] = true
			}
			tableStatements[fullTable] = stmt
		}
	}

	// build the short‐name → statement map
	createStmts = make(map[string]string, len(tableStatements))
	for full, stmt := range tableStatements {
		parts := strings.Split(full, ".")
		simple := parts[len(parts)-1]
		createStmts[simple] = stmt
	}

	// parse each DDL into a TableSchema and assign its TableNumber
	allSchemas = make(map[string]*TableSchema, len(order))
	for _, fullTable := range order {
		stmt := tableStatements[fullTable]
		schema, err := ParseDDL(stmt)
		if err != nil {
			// non-fatal; skip bad DDL
			continue
		}
		schema.TableNumber = currTableNo
		currTableNo++
		tableName := fullTable[strings.LastIndex(fullTable, ".")+1:]
		allSchemas[tableName] = schema
	}

	return allSchemas, createStmts, nil
}
