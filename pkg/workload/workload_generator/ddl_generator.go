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
	var colDefs, tableConstraints []string
	for _, p := range partsList {
		up := strings.ToUpper(strings.TrimSpace(p))
		if strings.HasPrefix(up, "CONSTRAINT") || strings.HasPrefix(up, "PRIMARY KEY") || strings.HasPrefix(up, "UNIQUE") || strings.HasPrefix(up, "FOREIGN KEY") || strings.HasPrefix(up, "CHECK") || strings.HasPrefix(up, "INDEX") {
			tableConstraints = append(tableConstraints, p)
		} else {
			colDefs = append(colDefs, p)
		}
	}
	// Define regex pattern to extract column properties from column definitions
	colPattern := regexp.MustCompile(`(?i)^\s*("?[^"]+"|[\w-]+)"?\s+([^\s]+)(?:\s+(NOT\s+NULL|NULL))?(?:\s+DEFAULT\s+((?:\([^\)]*\)|[^\s,]+)))?(?:\s+PRIMARY\s+KEY)?(?:\s+UNIQUE)?(?:\s+REFERENCES\s+([\w\.]+)\s*\(\s*([\w]+)\s*\))?(?:\s+CHECK\s*\(\s*(.*?)\s*\))?`)
	inlinePKCols := []string{}
	// Process each column definition
	for _, cd := range colDefs {
		m := colPattern.FindStringSubmatch(cd)
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

		ts.ColumnOrder = append(ts.ColumnOrder, name)
		// Extract CHECK constraint if present (requires special handling for nested parentheses)
		inlineCheck := ""
		checkIdx := regexp.MustCompile(`(?i)\bCHECK\s*\(`).FindStringIndex(cd)
		if checkIdx != nil {
			start := checkIdx[1]
			depth := 1
			i := start
			for i < len(cd) && depth > 0 {
				switch cd[i] {
				case '(':
					depth++
				case ')':
					depth--
				}
				i++
			}
			inlineCheck = strings.TrimSpace(cd[start : i-1])
		}
		// Determine some column properties
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
			ts.CheckConstraints = append(ts.CheckConstraints, inlineCheck)
		}
		ts.AddColumn(col)
	}

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
				local := []string{}
				// Extract local columns (referencing columns)
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
//   - anonymize: Flag for future anonymization feature (currently unused)
//
// Returns:
//   - map[string]*TableSchema: Map of table names to their schema objects
//   - map[string]string createStmts: Map of short table names to their CREATE TABLE statements
//   - error: Any error encountered during processing
//
// The "anonymize" parameter is unused for now: on TODO list
func GenerateDDLs(
	zipDir, dbName string, anonymize bool,
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
	req := []string{"database_name", "create_statement", "schema_name", "descriptor_type", "descriptor_name"}
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
