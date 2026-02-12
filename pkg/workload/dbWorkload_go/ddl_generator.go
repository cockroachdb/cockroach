// Package dbworkloadgo provides utilities for parsing and generating DDL (Data Definition Language)
// statements for database workloads. It includes functionality to extract schema information
// from database dumps and generate structured representations of tables and columns.

package dbworkloadgo

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// Column stores column level schema information based on input ddl.
type Column struct {
	Name         string // Name of the column
	ColType      string // SQL data type of the column
	IsNullable   bool   // Whether the column allows NULL values
	IsPrimaryKey bool   // Whether the column is part of the primary key
	Default      string // Default value expression for the column
	IsUnique     bool   // Whether the column has a UNIQUE constraint
	FKTable      string // Name of the referenced table if this is a foreign key
	FKColumn     string // Name of the referenced column if this is a foreign key
	InlineCheck  string // CHECK constraint expression if defined inline with the column
}

// TableSchema stores table level schema information based on input ddl.
type TableSchema struct {
	rowCount          int                // Number of rows in the table (used internally)
	TableName         string             // Fully qualified name of the table
	Columns           map[string]*Column // Map of column names to their definitions
	PrimaryKeys       []string           // List of column names that form the primary key
	UniqueConstraints [][]string         // List of unique constraints, each containing a list of column names
	ForeignKeys       [][3]interface{}   // List of foreign keys: (local cols []string, table string, foreign cols []string)
	CheckConstraints  []string           // List of CHECK constraint expressions
	OriginalTable     string             // Original table name as it appears in the DDL
}

// NewTableSchema creates a new TableSchema instance with the given table name and original name.
// It initializes an empty columns map and returns a pointer to the new TableSchema.
func NewTableSchema(name string, original string) *TableSchema {
	return &TableSchema{
		TableName:     name,
		Columns:       make(map[string]*Column),
		OriginalTable: original,
	}
}

// String function converts the Column schema details into a parsable placeholder.
func (c *Column) String() string {
	parts := []string{c.Name, c.ColType}
	if c.IsNullable {
		parts = append(parts, "NULL")
	} else {
		parts = append(parts, "NOT NULL")
	}
	if c.IsPrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}
	if c.Default != "" {
		parts = append(parts, "DEFAULT "+c.Default)
	}
	if c.IsUnique {
		parts = append(parts, "UNIQUE")
	}
	if c.FKTable != "" && c.FKColumn != "" {
		parts = append(parts, fmt.Sprintf("FK→%s.%s", c.FKTable, c.FKColumn))
	}
	if c.InlineCheck != "" {
		parts = append(parts, fmt.Sprintf("CHECK(%s)", c.InlineCheck))
	}
	return strings.Join(parts, " ")
}

// String function converts the TableSchema object into a readable format - mostly for symmetry and testing.
func (ts *TableSchema) String() string {
	out := []string{fmt.Sprintf("Table: %s", ts.TableName), " Columns:"}
	for _, col := range ts.Columns {
		out = append(out, "  "+col.String())
	}
	if len(ts.PrimaryKeys) > 0 {
		out = append(out, " PKs: "+strings.Join(ts.PrimaryKeys, ", "))
	}
	if len(ts.UniqueConstraints) > 0 {
		tmp := []string{}
		for _, u := range ts.UniqueConstraints {
			tmp = append(tmp, fmt.Sprintf("(%s)", strings.Join(u, ",")))
		}
		out = append(out, " UNIQUE: "+strings.Join(tmp, "; "))
	}
	if len(ts.ForeignKeys) > 0 {
		tmp := []string{}
		for _, fk := range ts.ForeignKeys {
			l := fk[0].([]string)
			t := fk[1].(string)
			f := fk[2].([]string)
			tmp = append(tmp, fmt.Sprintf("(%s)→%s(%s)", strings.Join(l, ","), t, strings.Join(f, ",")))
		}
		out = append(out, " FKs: "+strings.Join(tmp, "; "))
	}
	if len(ts.CheckConstraints) > 0 {
		out = append(out, " CHECKs: "+strings.Join(ts.CheckConstraints, "; "))
	}
	return strings.Join(out, "\n") + "\n"
}

// AddColumn adds a Column to the TableSchema by storing it in the Columns map
// using the column name as the key.
func (ts *TableSchema) AddColumn(c *Column) {
	ts.Columns[c.Name] = c
}

// SetPrimaryKeys stores primary key information at table level and updates the
// corresponding column properties (IsPrimaryKey, IsNullable, IsUnique) accordingly.
func (ts *TableSchema) SetPrimaryKeys(pks []string) {
	ts.PrimaryKeys = pks
	single := len(pks) == 1
	// Columns labeled as primary key are all set to not nullable.
	// Primary key columns are only marked as unique if they are not part of a composite Primary Key
	for _, pk := range pks {
		if col, ok := ts.Columns[pk]; ok {
			col.IsPrimaryKey = true
			col.IsNullable = false
			if single {
				col.IsUnique = true
			}
		}
	}
}

// ParseDDL converts a "CREATE TABLE ..." DDL statement into a TableSchema.
// It parses the table name, columns, and constraints (primary keys, unique constraints,
// foreign keys, and check constraints) from the DDL statement and returns a structured
// representation of the table schema.
func ParseDDL(ddl string) (*TableSchema, error) {
	// Define regex patterns for identifying identifiers in SQL
	ident := `(?:"[^"]+"|[A-Za-z_][\w]*)`                       // Matches quoted or unquoted identifiers
	fullIdent := fmt.Sprintf(`(%s(?:\.%s){0,2})`, ident, ident) // Matches fully qualified names (up to 3 parts)

	// Create regex to match CREATE TABLE statements and extract the table name
	tablePattern := regexp.MustCompile(`(?i)CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+` + fullIdent)
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
	ts := NewTableSchema(tableName, rawName)

	// Extract the column definitions block from the DDL
	bodyRe := regexp.MustCompile(`(?s)\((.*)\)\s*([^)]*)$`)
	bodyMatch := bodyRe.FindStringSubmatch(ddl)
	if bodyMatch == nil {
		return nil, errors.New("no column block")
	}
	body := bodyMatch[1]

	// Split the column block into individual column definitions and constraints
	// while respecting nested parentheses (for complex expressions)
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
				// Only split on commas at the top level (not inside parentheses)
				partsList = append(partsList, strings.TrimSpace(buf))
				buf = ""
			} else {
				buf += string(ch)
			}
		default:
			buf += string(ch)
		}
	}
	// Add the last part if there's anything left
	if strings.TrimSpace(buf) != "" {
		partsList = append(partsList, strings.TrimSpace(buf))
	}

	// Separate column definitions from table-level constraints
	var colDefs, tableConstraints []string
	for _, p := range partsList {
		up := strings.ToUpper(strings.TrimSpace(p))
		// Identify table-level constraints by their keywords
		if strings.HasPrefix(up, "CONSTRAINT") || strings.HasPrefix(up, "PRIMARY KEY") ||
			strings.HasPrefix(up, "UNIQUE") || strings.HasPrefix(up, "FOREIGN KEY") ||
			strings.HasPrefix(up, "CHECK") || strings.HasPrefix(up, "INDEX") {
			tableConstraints = append(tableConstraints, p)
		} else {
			colDefs = append(colDefs, p)
		}
	}

	// Define regex pattern to extract column properties from column definitions
	colPattern := regexp.MustCompile(`(?i)^\s*("?[^"]+"|[\w-]+)"?\s+([^\s]+)(?:\s+(NOT\s+NULL|NULL))?(?:\s+DEFAULT\s+((?:\([^\)]*\)|[^\s,]+)))?(?:\s+PRIMARY\s+KEY)?(?:\s+UNIQUE)?(?:\s+REFERENCES\s+([\w\.]+)\s*\(\s*([\w]+)\s*\))?(?:\s+CHECK\s*\(\s*(.*?)\s*\))?`)
	inlinePKCols := []string{} // Track columns with inline PRIMARY KEY constraints

	// Process each column definition
	for _, cd := range colDefs {
		m := colPattern.FindStringSubmatch(cd)
		if m == nil {
			continue // Skip if pattern doesn't match
		}

		// Extract column properties from regex matches
		name := m[1]     // Column name
		ctype := m[2]    // Column type
		nullSpec := m[3] // NULL or NOT NULL specification
		defVal := m[4]   // DEFAULT value
		fkTable := m[5]  // Referenced table for foreign keys
		fkCol := m[6]    // Referenced column for foreign keys

		// Extract CHECK constraint if present (requires special handling for nested parentheses)
		inlineCheck := ""
		checkIdx := regexp.MustCompile(`(?i)\bCHECK\s*\(`).FindStringIndex(cd)
		if checkIdx != nil {
			start := checkIdx[1] // Start after the opening parenthesis
			depth := 1
			i := start
			// Find the matching closing parenthesis by tracking nesting depth
			for i < len(cd) && depth > 0 {
				switch cd[i] {
				case '(':
					depth++
				case ')':
					depth--
				}
				i++
			}
			inlineCheck = strings.TrimSpace(cd[start : i-1]) // Extract the CHECK expression
		}

		// Determine column properties
		isNullable := nullSpec == "" || strings.ToUpper(nullSpec) == "NULL"
		isUnique := regexp.MustCompile(`(?i)\bUNIQUE\b`).MatchString(cd)
		isPK := regexp.MustCompile(`(?i)\bPRIMARY\s+KEY\b`).MatchString(cd)

		// Handle inline PRIMARY KEY constraints
		if isPK {
			inlinePKCols = append(inlinePKCols, strings.Trim(name, `"`))
			isNullable = false // PRIMARY KEY columns cannot be NULL
			isUnique = true    // PRIMARY KEY columns are implicitly UNIQUE
		}

		// Create and populate the Column object
		col := &Column{
			Name:         strings.Trim(name, `"`),
			ColType:      ctype,
			IsNullable:   isNullable,
			IsPrimaryKey: isPK,
			Default:      strings.TrimSpace(defVal),
			IsUnique:     isUnique,
		}

		// Add foreign key information if present
		if fkTable != "" {
			col.FKTable = fkTable
			col.FKColumn = fkCol
		}

		// Add CHECK constraint if present
		if inlineCheck != "" {
			col.InlineCheck = inlineCheck
			ts.CheckConstraints = append(ts.CheckConstraints, inlineCheck)
		}

		// Add the column to the table schema
		ts.AddColumn(col)
	}

	// Apply inline primary key constraints if any were found
	if len(inlinePKCols) > 0 {
		ts.SetPrimaryKeys(inlinePKCols)
	}

	// Process table-level constraints
	for _, tc := range tableConstraints {
		up := strings.ToUpper(tc)

		// Handle PRIMARY KEY constraints
		if strings.Contains(up, "PRIMARY KEY") {
			raw := regexp.MustCompile(`\((.*?)\)`).FindStringSubmatch(tc)
			if raw != nil {
				cols := []string{}
				// Extract column names from the constraint
				for _, col := range strings.Split(raw[1], ",") {
					cols = append(cols, strings.Split(strings.TrimSpace(strings.Trim(col, `"`)), " ")[0])
				}
				// Set primary keys at table level
				ts.SetPrimaryKeys(cols)
			}
			continue
		}

		// Handle UNIQUE constraints
		if strings.Contains(up, "UNIQUE") {
			raw := regexp.MustCompile(`\((.*?)\)`).FindStringSubmatch(tc)
			if raw != nil {
				cols := []string{}
				// Extract column names from the constraint
				for _, col := range strings.Split(raw[1], ",") {
					cols = append(cols, strings.Split(strings.TrimSpace(strings.Trim(col, `"`)), " ")[0])
				}
				// Add to table's unique constraints
				ts.UniqueConstraints = append(ts.UniqueConstraints, cols)

				// For single-column unique constraints, mark the column as unique
				isComposite := len(cols) > 1
				for _, c := range cols {
					if !isComposite {
						if col, ok := ts.Columns[c]; ok {
							col.IsUnique = true
						}
					}
				}
			}
			continue
		}

		// Handle FOREIGN KEY constraints
		if strings.Contains(up, "FOREIGN KEY") {
			fkRe := regexp.MustCompile(`(?i)FOREIGN\s+KEY\s*\(([^\)]*)\)\s+REFERENCES\s+((?:"[^"]+"|[\w]+)(?:\.(?:"[^"]+"|[\w]+))*)\s*\(([^\)]*)\)`)
			m2 := fkRe.FindStringSubmatch(tc)
			if m2 != nil {
				// Extract local columns (referencing columns)
				local := []string{}
				for _, c := range strings.Split(m2[1], ",") {
					local = append(local, strings.TrimSpace(c))
				}

				// Extract referenced table name
				tbl := strings.TrimSpace(m2[2])
				tblRaw := strings.ReplaceAll(tbl, "\"", "")

				// Extract referenced columns
				foreign := []string{}
				for _, c := range strings.Split(m2[3], ",") {
					foreign = append(foreign, strings.TrimSpace(c))
				}

				// Add to table's foreign keys
				ts.ForeignKeys = append(ts.ForeignKeys, [3]interface{}{local, tblRaw, foreign})
			}
			continue
		}

		// Handle CHECK constraints
		if strings.Contains(up, "CHECK") {
			m2 := regexp.MustCompile(`CHECK\s*\((.*)\)`).FindStringSubmatch(tc)
			if m2 != nil {
				ts.CheckConstraints = append(ts.CheckConstraints, strings.TrimSpace(m2[1]))
			}
			continue
		}

		// Skip INDEX definitions (not relevant for schema representation)
		if strings.HasPrefix(up, "INDEX") {
			continue
		}
	}

	return ts, nil
}

// GenerateDDLs extracts and processes DDL statements from a CockroachDB debug zip file.
// It reads the create_statements.txt file from the zip directory, filters statements
// for the specified database, and writes them to an output file. It also parses each
// DDL statement into a TableSchema object and returns a map of table names to their schemas.
//
// Parameters:
//   - zipDir: Directory containing the debug zip contents
//   - dbName: Name of the database to extract DDLs for
//   - outputDir: Directory where the output file will be written
//   - outputFileName: Name of the output file
//   - anonymize: Flag for future anonymization feature (currently unused)
//
// Returns:
//   - map[string]*TableSchema: Map of table names to their schema objects
//   - error: Any error encountered during processing
//
// The "anonymize" parameter is unused for now: on TODO list
func GenerateDDLs(
	zipDir, dbName, outputDir, outputFileName string, anonymize bool,
) (allSchemas map[string]*TableSchema, retErr error) {
	filePath := filepath.Join(zipDir, "crdb_internal.create_statements.txt")
	if _, err := os.Stat(filePath); err != nil {
		return nil, errors.Wrap(err, "could not find TSV file")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open TSV file")
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
		return nil, errors.Wrap(err, "failed reading TSV header")
	}
	colIndex := map[string]int{}
	for i, col := range header {
		colIndex[col] = i
	}
	req := []string{"database_name", "create_statement", "schema_name", "descriptor_type", "descriptor_name"}
	for _, c := range req {
		if _, ok := colIndex[c]; !ok {
			// Not wrapping an error, so fmt.Errorf is acceptable here.
			return nil, fmt.Errorf("missing column %s", c)
		}
	}

	// Maps to store table statements and maintain order
	tableStatements := make(map[string]string)   // Maps full table names to CREATE TABLE statements
	order := []string{}                          // Preserves the order of tables for output
	seen := map[string]bool{}                    // Tracks which tables have been seen to avoid duplicates
	schemaReCache := map[string]*regexp.Regexp{} // Cache for compiled regex patterns to improve performance

	// Read and process each row from the TSV file
	for {
		rec, err := reader.Read()
		if err != nil {
			// Handle various error conditions
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
			return nil, errors.Wrap(err, "failed while reading TSV rows")
		}
		if len(rec) == 0 {
			break
		}

		// Filter for tables in the specified database and public schema
		if rec[colIndex["database_name"]] == dbName && rec[colIndex["descriptor_type"]] == "table" && rec[colIndex["schema_name"]] == "public" {
			schemaName := rec[colIndex["schema_name"]]
			stmt := rec[colIndex["create_statement"]]
			tableName := rec[colIndex["descriptor_name"]]
			fullTable := fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName)

			// Get or create regex pattern for schema name replacement
			pattern, ok := schemaReCache[schemaName]
			if !ok {
				pattern = regexp.MustCompile(`\b` + regexp.QuoteMeta(schemaName) + `\.`)
				schemaReCache[schemaName] = pattern
			}

			// Ensure fully qualified table names in the statement
			stmt = pattern.ReplaceAllString(stmt, dbName+"."+schemaName+".")

			// Add IF NOT EXISTS to CREATE TABLE statements if not already present
			if !regexp.MustCompile(`(?i)IF\s+NOT\s+EXISTS`).MatchString(stmt) {
				stmt = regexp.MustCompile(`(?i)^(CREATE\s+TABLE\s+)`).ReplaceAllString(stmt, "${1}IF NOT EXISTS ")
			}

			// Track table order and store the statement
			if !seen[fullTable] {
				order = append(order, fullTable)
				seen[fullTable] = true
			}
			tableStatements[fullTable] = stmt
		}
	}

	// Convert ordered table names to their corresponding CREATE TABLE statements
	statements := make([]string, 0, len(order))
	for _, t := range order {
		statements = append(statements, tableStatements[t])
	}

	// Set default output directory if not specified
	if outputDir == "" {
		outputDir = "."
	}

	// Create output file
	outputPath := filepath.Join(outputDir, outputFileName)
	out, err := os.Create(outputPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create output file at %s", outputPath)
	}
	// Ensure file is closed when function returns
	defer func() {
		if cerr := out.Close(); cerr != nil && retErr == nil {
			retErr = errors.Wrap(cerr, "failed to close output file")
		}
	}()

	// Write database creation statement first
	_, err = fmt.Fprintf(out, "create database if not exists %s;\n\n", dbName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to write database create statement")
	}

	// Initialize map to store parsed schemas
	allSchemas = map[string]*TableSchema{}

	// Process each CREATE TABLE statement
	for _, stmt := range statements {
		// Write the statement to the output file
		_, err1 := fmt.Fprintln(out, stmt+";")
		if err1 != nil {
			return nil, errors.Wrap(err1, "failed to write table statement to output file")
		}
		// Add a blank line after each statement for readability
		_, err2 := fmt.Fprintln(out)
		if err2 != nil {
			return nil, errors.Wrap(err2, "failed to write newline to output file")
		}

		// Parse the DDL statement into a TableSchema object
		schema, err := ParseDDL(stmt)
		if err != nil {
			// Not fatal: log and continue (CockroachDB best practice for non-critical parse errors)
			log.Printf("warning: failed to parse DDL (%v): %v", err, stmt)
			continue
		}

		// Extract the simple table name (without schema) as the map key
		tableName := schema.TableName[strings.LastIndex(schema.TableName, ".")+1:]
		allSchemas[tableName] = schema
	}

	return allSchemas, nil
}
