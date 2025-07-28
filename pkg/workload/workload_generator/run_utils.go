// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	"context"
	"database/sql"
	gosql "database/sql"
	"fmt"
	"github.com/lib/pq"
	"gopkg.in/yaml.v2"
	"io"
	"math/rand/v2"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

var (
	// txnRe defines how to separate the transactions from the <dbName.sql> file
	txnRe = regexp.MustCompile(`(?m)^-------Begin Transaction------\s*([\s\S]*?)-------End Transaction-------`)
	// placeholderRe defines the structure of the placeholders that need to be converted into $1, $2...
	placeholderRe = regexp.MustCompile(`"?:-:\|(.+?)\|:-:"?`)
)

// loadYamlData first checks whether the input yaml flag is set or not.
// Accordingly, it loads the schema information into memory from the correct location.
func (w *workloadGeneratorStruct) loadYamlData() (error, bool) {
	var path string
	if w.inputYAML != "" {
		path = w.inputYAML
	} else {
		path = fmt.Sprintf("schema_%s.yaml", w.dbName)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return errors.Wrapf(err, "could not read schema YAML from %s", path), true
	}
	if err := yaml.UnmarshalStrict(raw, &w.workloadSchema); err != nil {
		return errors.Wrapf(err, "couldn't unmarshal schema YAML"), true
	}
	return nil, false
}

// setColumnValue sets the value for a placeholder in the args slice.
func setColumnValue(raw string, placeholder Placeholder, args []interface{}, i int) error {
	var arg interface{}
	// If we got an empty string and this column is nullable, emitting a SQL NULL.
	if raw == "" && placeholder.IsNullable {
		arg = setNullType(placeholder, arg)
	} else {
		// Otherwise the raw string is parsed into the right Go/sql type.
		typedValue, err := setNotNullType(placeholder, raw, arg)
		if err != nil {
			return err
		}
		arg = typedValue
	}

	args[i] = arg
	return nil
}

// setNotNullType converts the raw string value to the appropriate SQL type based on the placeholder's column type.
func setNotNullType(placeholder Placeholder, raw string, arg interface{}) (interface{}, error) {
	switch t := strings.ToUpper(placeholder.ColType); {
	case strings.HasPrefix(t, "INT"): // integer types
		iv, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
		arg = sql.NullInt64{Int64: iv, Valid: true}
	case strings.HasPrefix(t, "FLOAT"), strings.HasPrefix(t, "DECIMAL"), strings.HasPrefix(t, "NUMERIC"), strings.HasPrefix(t, "DOUBLE"): // floating point types
		fv, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		arg = sql.NullFloat64{Float64: fv, Valid: true}
	case t == "BOOL", t == "BOOLEAN": // boolean types
		bv, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, err
		}
		arg = sql.NullBool{Bool: bv, Valid: true}
	// The remaining types are parsed as raw strings.
	default:
		// Everything else is treated as text/varchar/etc.
		arg = sql.NullString{String: raw, Valid: raw != ""}
	}
	return arg, nil
}

// setNullType sets the argument to a SQL NULL value based on the column type of the placeholder.
func setNullType(placeholder Placeholder, arg interface{}) interface{} {
	switch t := strings.ToUpper(placeholder.ColType); {
	case strings.HasPrefix(t, "INT"):
		arg = sql.NullInt64{Valid: false}
	case strings.HasPrefix(t, "FLOAT"), strings.HasPrefix(t, "DECIMAL"), strings.HasPrefix(t, "NUMERIC"), strings.HasPrefix(t, "DOUBLE"):
		arg = sql.NullFloat64{Valid: false}
	case t == "BOOL", t == "BOOLEAN":
		arg = sql.NullBool{Valid: false}
	default:
		arg = sql.NullString{Valid: false}
	}
	return arg
}

// getTableName checks if the name field of placeholder is a column in the TableName column in the allSchema map inside d.
// If yes, then returns that table name. otherwise looks for a table with that column and returns that.
func getTableName(p Placeholder, d *workloadGeneratorStruct) string {
	for _, block := range d.workloadSchema[p.TableName] {
		for colName, _ := range block.Columns {
			if colName == p.Name {
				return p.TableName
			}
		}
	}
	for tableName, blocks := range d.workloadSchema {
		block := blocks[0]
		for colName, _ := range block.Columns {
			if colName == p.Name {
				return tableName
			}
		}
	}
	return p.TableName
}

// getColumnValue retrieves the value for a placeholder based on its clause and whether it has a foreign key dependency.
func getColumnValue(allPksAreFK bool, p Placeholder, d *workloadGeneratorStruct, inserted map[string][]interface{}, raw string, indexes []int, i int) string {
	if allPksAreFK && (p.Clause == insert || p.Clause == update) {
		tableName := getTableName(p, d)
		key := fmt.Sprintf("%s.%s", tableName, p.Name)
		fk := d.columnGens[key].columnMeta.FK
		parts := strings.Split(fk, ".")
		parentCol := parts[len(parts)-1] // The last part is the column name.
		if vals, ok := inserted[parentCol]; ok && len(vals) > 0 {
			raw = vals[0].(string)         // Using the first value from the inserted map.
			inserted[parentCol] = vals[1:] // Remove the first value from the map.
		} else {
			//Fallback that shouldn't really happen.
			raw = d.getRegularColumnValue(p, indexes[i])
		}
	} else {
		raw = d.getRegularColumnValue(p, indexes[i])
	}
	return raw
}

// setCacheIndex sets the indexes for each placeholder in the SQL query.
// If the placeholder is a foreign key, it uses the fkIdx; otherwise, it picks a random index from the cache.
func (t *txnWorker) setCacheIndex(sqlQuery SQLQuery, d *workloadGeneratorStruct, fkIdx int) []int {
	indexes := make([]int, len(sqlQuery.Placeholders))
	for i, p := range sqlQuery.Placeholders {
		tableName := getTableName(p, d)
		key := fmt.Sprintf("%s.%s", tableName, p.Name)
		if d.columnGens[key].columnMeta.HasForeignKey {
			indexes[i] = fkIdx
		} else {
			cacheLen := len(d.columnGens[key].cache)
			indexes[i] = t.rng.IntN(cacheLen)
		}
	}
	return indexes
}

// pickForeignKeyIndex picks a random index from the cache of a foreign key column.
// It helps ensure that all members of a composite foreign key are set to the same parent row.
func (t *txnWorker) pickForeignKeyIndex(sqlQuery SQLQuery, d *workloadGeneratorStruct) int {
	fkIdx := -1
	for _, p := range sqlQuery.Placeholders {
		tableName := getTableName(p, d)
		key := fmt.Sprintf("%s.%s", tableName, p.Name)
		if d.columnGens[key].columnMeta.HasForeignKey {
			cacheLen := len(d.columnGens[key].cache)
			if cacheLen > 0 {
				fkIdx = t.rng.IntN(cacheLen)
			}
			break
		}
	}
	return fkIdx
}

// checkIfAllPkAreFk checks if all primary keys in the SQL query are foreign keys.
func checkIfAllPkAreFk(sqlQuery SQLQuery, d *workloadGeneratorStruct) bool {
	allPksAreFK := true
	for _, p := range sqlQuery.Placeholders {
		tableName := getTableName(p, d)
		key := fmt.Sprintf("%s.%s", tableName, p.Name)
		if p.IsPrimaryKey && !d.columnGens[key].columnMeta.HasForeignKey {
			allPksAreFK = false
			break
		}
	}
	return allPksAreFK
}

// chooseTransaction returns a random transaction of type read or write based on the readPct flag.
func (t *txnWorker) chooseTransaction() Transaction {
	var txn Transaction
	reads := t.readTransactions
	writes := t.writeTransactions

	// If neither set has any transactions, just the zero-Txn is returned.
	if len(reads) == 0 && len(writes) == 0 {
		return txn
	}
	// If there are no read txns, a write transaction is always picked.
	if len(reads) == 0 {
		return writes[t.rng.IntN(len(writes))]
	}
	// If there are no write txns, a read transaction is always picked.
	if len(writes) == 0 {
		return reads[t.rng.IntN(len(reads))]
	}

	// If both are non-empty then txn is chosen based on readPct.
	if t.rng.IntN(100) < t.w.readPct {
		return reads[t.rng.IntN(len(reads))]
	}
	return writes[t.rng.IntN(len(writes))]
}

// setDbName registers the database name in the main workload struct.
// It uses the name provided using the --db flag, otherwise falls back to "workload_generator".
func (w *workloadGeneratorStruct) setDbName() {
	dbName := w.Meta().Name
	if w.connFlags.DBOverride != "" {
		dbName = w.connFlags.DBOverride
	}
	w.dbName = dbName
}

// getRegularColumnValue picks values from the generator or cache depending on sql clause or whether there is a fk dependency.
func (w *workloadGeneratorStruct) getRegularColumnValue(p Placeholder, idx int) string {
	tableName := getTableName(p, w)
	key := fmt.Sprintf("%s.%s", tableName, p.Name)
	rc := w.columnGens[key]
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// For filling in data for where clause or columns with foreign key constraints, we use the cache.
	if p.Clause == "WHERE" || (rc.columnMeta.HasForeignKey == true) {
		if len(rc.cache) > 0 {
			return rc.cache[idx]
		}
	}
	// For insert or update clauses, we use the generator to get a new value.
	v := rc.gen.Next()
	if len(rc.cache) < maxCacheSize {
		rc.cache = append(rc.cache, v)
	} else {
		rc.cache[rand.IntN(len(rc.cache))] = v
	}
	return v
}

// initGenerators seeds d.columnGens with both fresh generators and a cache
// of real values pulled from the live database.
func (w *workloadGeneratorStruct) initGenerators(db *sql.DB) error {
	// globalNumBatches is needed to seed the generators.
	// 0-globalNumBatches-1 was used during initial bulk insertions.
	// So, globalNumBatches can be the batch index for run time generators.
	maxRows := 0
	for _, tblBlocks := range w.workloadSchema {
		if tblBlocks[0].Count > maxRows {
			maxRows = tblBlocks[0].Count
		}
	}
	// TODO: make teh globalBatchNumber dynamic
	globalNumBatches := (maxRows + baseBatchSize - 1) / baseBatchSize

	// 1) The generator + empty cache for every table.col is built.
	w.buildRuntimeGenerators(globalNumBatches)

	// 2) Each cache is primed by selecting up to maxInitialCacheSize existing rows.
	// We do this column-by-column to keep it simple.
	err := w.setCacheValues(db)
	if err != nil {
		return err
	}
	return nil
}

// setCacheValues fills into the runtimeColumn structs the cache data consisting of values generated during the initial bulk insert.
func (w *workloadGeneratorStruct) setCacheValues(db *gosql.DB) error {
	for tableName, blocks := range w.workloadSchema {
		block := blocks[0]
		for _, colName := range block.ColumnOrder {
			key := fmt.Sprintf("%s.%s", tableName, colName)
			rc := w.columnGens[key]

			// Building a query like: SELECT colName FROM tableName LIMIT maxInitialCacheSize
			q := fmt.Sprintf(`SELECT %s FROM %s LIMIT %d`,
				pq.QuoteIdentifier(colName), pq.QuoteIdentifier(tableName), maxCacheSize)
			rows, err := db.QueryContext(context.Background(), q)
			if err != nil {
				return fmt.Errorf("priming cache for %s: %w", key, err)
			}

			for rows.Next() {
				var raw sql.NullString
				if err := rows.Scan(&raw); err != nil {
					rows.Close()
					return fmt.Errorf("scanning cache row for %s: %w", key, err)
				}
				if raw.Valid {
					rc.cache = append(rc.cache, raw.String)
				}
				if len(rc.cache) >= maxCacheSize {
					break
				}
			}
			rows.Close()
		}
	}
	return nil
}

// buildRuntimeGenerators initializes the runtime generators for each column in the workload schema.
func (w *workloadGeneratorStruct) buildRuntimeGenerators(globalNumBatches int) {
	w.columnGens = make(map[string]*runtimeColumn)
	for tableNmae, blocks := range w.workloadSchema {
		block := blocks[0]
		for colName, meta := range block.Columns {
			key := fmt.Sprintf("%s.%s", tableNmae, colName)
			gen := buildGenerator(meta, globalNumBatches, baseBatchSize, w.workloadSchema)
			w.columnGens[key] = &runtimeColumn{
				gen:        gen,
				cache:      make([]string, 0, maxCacheSize),
				columnMeta: meta,
			}
		}
	}
}

// readSQL reads <dbName><read/write>.sql and returns a slice of Transactions.
// It will number placeholders $1…$N separately in each SQL statement.
func readSQL(path, typ string) ([]Transaction, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(bufio.NewReader(f))
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	text := string(data)

	// Each transaction block
	blocks := txnRe.FindAllStringSubmatch(text, -1)

	// Currently we are defining two types of transactions - read and write.
	var txns []Transaction
	// For every transaction block.
	for _, blk := range blocks {
		body := blk[1]
		lines := strings.Split(body, "\n")
		var txn Transaction
		var curr SQLQuery
		// For every sql query line.
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || line == "BEGIN;" || line == "COMMIT;" {
				continue
			}
			// build up the SQL text
			curr.SQL += line + " "
			if strings.HasSuffix(line, ";") {
				// once we hit the end of a statement, re-number from $1
				stmtPos := 1
				var placeholders []Placeholder
				// sqlOut is the reWritten sql where teh placeholders have been replaced with $x.
				sqlOut := placeholderRe.ReplaceAllStringFunc(curr.SQL, makePlaceholderReplacer(&placeholders, &stmtPos))

				curr.SQL = strings.TrimSpace(sqlOut)
				curr.Placeholders = placeholders
				txn.Queries = append(txn.Queries, curr)

				// reset for next statement
				curr = SQLQuery{}
			}
		}
		// Decide whether the transaction is a read type or write type.
		txn.typ = typ
		txns = append(txns, txn)
	}

	return txns, nil
}

// makePlaceholderReplacer returns a ReplaceAllStringFunc that
// appends to placeholders and increments stmtPos.
func makePlaceholderReplacer(placeholders *[]Placeholder, stmtPos *int) func(string) string {
	return func(match string) string {
		inner := placeholderRe.FindStringSubmatch(match)[1]
		parts := splitQuoted(inner)

		var p Placeholder
		//Set all the fields in the placeholder struct based on teh information from the sql.
		p.Name = trimQuotes(parts[0])
		p.ColType = trimQuotes(parts[1])
		p.IsNullable = trimQuotes(parts[2]) == "NULL"
		p.IsPrimaryKey = strings.Contains(trimQuotes(parts[3]), "PRIMARY KEY")
		if d := trimQuotes(parts[4]); d != "" {
			p.Default = &d
		}
		p.IsUnique = trimQuotes(parts[5]) == "UNIQUE"
		if fk := trimQuotes(parts[6]); fk != "" {
			fkParts := strings.Split(strings.TrimPrefix(fk, "FK→"), ".")
			p.FKReference = &FKRef{Table: fkParts[0], Column: fkParts[1]}
		}
		if chk := trimQuotes(parts[7]); chk != "" {
			p.InlineCheck = &chk
		}
		p.Clause = trimQuotes(parts[8])
		p.Position = *stmtPos
		p.TableName = trimQuotes(parts[9])

		*stmtPos++
		*placeholders = append(*placeholders, p)
		return fmt.Sprintf("$%d::%s", p.Position, p.ColType)
	}
}

// splitQuoted splits a string like "'a','b','c'" into ["'a'", "'b'", "'c'"].
func splitQuoted(s string) []string {
	var out []string
	buf := ""
	inQuote := false
	for _, r := range s {
		switch r {
		case '\'':
			inQuote = !inQuote
			buf += string(r)
		case ',':
			if inQuote {
				buf += string(r)
			} else {
				out = append(out, strings.TrimSpace(buf))
				buf = ""
			}
		default:
			buf += string(r)
		}
	}
	if buf != "" {
		out = append(out, strings.TrimSpace(buf))
	}
	return out
}

// trimQuotes removes leading/trailing single-quotes.
func trimQuotes(s string) string {
	return strings.Trim(s, "'")
}
