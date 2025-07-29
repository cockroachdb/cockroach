// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	gosql "database/sql"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	// txnRe defines how to separate the transactions from the <dbName.sql> file
	txnRe = regexp.MustCompile(`(?m)^-------Begin Transaction------\s*([\s\S]*?)-------End Transaction-------`)
	// placeholderRe defines the structure of the placeholders that need to be converted into $1, $2...
	placeholderRe = regexp.MustCompile(`"?:-:\|(.+?)\|:-:"?`)
)

// setColumnValue sets the value for a placeholder in the args slice at index i.
func setColumnValue(raw string, placeholder Placeholder, args []interface{}, i int) error {
	var arg interface{}
	// If we got an empty string and this column is nullable, emitting a SQL NULL.
	if raw == "" && placeholder.IsNullable {
		arg = setNullType(placeholder)
	} else {
		// Otherwise the raw string is parsed into the right Go/sql type.
		typedValue, err := setNotNullType(placeholder, raw)
		if err != nil {
			return err
		}
		arg = typedValue
	}

	args[i] = arg
	return nil
}

// setNotNullType converts the raw string value to the appropriate SQL type based on the placeholder's column type.
func setNotNullType(placeholder Placeholder, raw string) (interface{}, error) {
	var arg interface{}
	switch t := strings.ToUpper(placeholder.ColType); {
	case strings.HasPrefix(t, "INT"): // integer types
		iv, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
		arg = gosql.NullInt64{Int64: iv, Valid: true}
	case strings.HasPrefix(t, "FLOAT"), strings.HasPrefix(t, "DECIMAL"), strings.HasPrefix(t, "NUMERIC"), strings.HasPrefix(t, "DOUBLE"): // floating point types
		fv, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		arg = gosql.NullFloat64{Float64: fv, Valid: true}
	case t == "BOOL", t == "BOOLEAN": // boolean types
		bv, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, err
		}
		arg = gosql.NullBool{Bool: bv, Valid: true}
	// The remaining types are parsed as raw strings.
	default:
		// Everything else is treated as text/varchar/etc.
		arg = gosql.NullString{String: raw, Valid: raw != ""}
	}
	return arg, nil
}

// setNullType sets the argument to a SQL NULL value based on the column type of the placeholder.
func setNullType(placeholder Placeholder) interface{} {
	var arg interface{}
	switch t := strings.ToUpper(placeholder.ColType); {
	case strings.HasPrefix(t, "INT"):
		arg = gosql.NullInt64{Valid: false}
	case strings.HasPrefix(t, "FLOAT"), strings.HasPrefix(t, "DECIMAL"), strings.HasPrefix(t, "NUMERIC"), strings.HasPrefix(t, "DOUBLE"):
		arg = gosql.NullFloat64{Valid: false}
	case t == "BOOL", t == "BOOLEAN":
		arg = gosql.NullBool{Valid: false}
	default:
		arg = gosql.NullString{Valid: false}
	}
	return arg
}

// getTableName checks if the name field of placeholder is a column in the TableName column in the allSchema map inside d.
// If yes, then returns that table name. otherwise looks for a table with that column and returns that.
func getTableName(p Placeholder, d *workloadGenerator) string {
	for _, block := range d.workloadSchema[p.TableName] {
		for colName := range block.Columns {
			if colName == p.Name {
				return p.TableName
			}
		}
	}
	for tableName, blocks := range d.workloadSchema {
		block := blocks[0]
		for colName := range block.Columns {
			if colName == p.Name {
				return tableName
			}
		}
	}
	return p.TableName
}

// getColumnValue retrieves the value for a placeholder based on its clause and whether it has a foreign key dependency.
func getColumnValue(
	allPksAreFK bool,
	p Placeholder,
	d *workloadGenerator,
	inserted map[string][]interface{},
	indexes []int,
	i int,
) string {
	var raw string
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

// checkIfAllPkAreFk checks if all primary keys in the SQL query are foreign keys.
func checkIfAllPkAreFk(sqlQuery SQLQuery, d *workloadGenerator) bool {
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

// readSQL reads <dbName><read/write>.sql and returns a slice of Transactions.
// It will number placeholders $1…$N separately in each SQL statement.
func readSQL(path, typ string) ([]Transaction, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

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
				// sqlOut is the rewritten SQL where the placeholders have been replaced with $x.
				sqlOut := placeholderRe.ReplaceAllStringFunc(curr.SQL, getPlaceholderReplacer(&placeholders, &stmtPos))

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

// getPlaceholderReplacer returns a ReplaceAllStringFunc that
// appends to placeholders and increments stmtPos.
func getPlaceholderReplacer(placeholders *[]Placeholder, stmtPos *int) func(string) string {
	return func(match string) string {
		inner := placeholderRe.FindStringSubmatch(match)[1]
		parts := splitQuoted(inner)

		var p Placeholder
		//Set all the fields in the placeholder struct based on the information from the sql.
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
