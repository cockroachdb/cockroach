// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeSimpleSchema builds a *TableSchema with the given name and columns,
// each non-nullable TEXT column.
func makeSimpleSchema(name string, cols []string) *TableSchema {
	ts := &TableSchema{
		TableName:   name,
		Columns:     make(map[string]*Column, len(cols)),
		ColumnOrder: cols,
	}
	for _, c := range cols {
		ts.Columns[c] = &Column{
			Name:       c,
			ColType:    "TEXT",
			IsNullable: false,
		}
	}
	return ts
}

func TestReplacePlaceholders_AllCases(t *testing.T) {
	// 1) Set up two 5-column tables: orders and ref_data
	schemas := map[string]*TableSchema{
		"orders":   makeSimpleSchema("orders", []string{"acc_no", "status", "amount", "id", "ts"}),
		"ref_data": makeSimpleSchema("ref_data", []string{"acc_no", "c2", "c3", "c4", "c5"}),
	}

	// 2) Table-driven inputs covering INSERT, SELECT, UPDATE, DELETE
	tests := []struct {
		name   string
		input  string
		table  string // expected table name in tags
		clause string // expected clause name in tags
	}{
		{"Insert no-cols", "INSERT INTO orders VALUES (_,__more__);", "orders", "INSERT"},
		{"Insert with cols + RETURNING", "INSERT INTO orders(acc_no, status, amount) VALUES (_, __more__) RETURNING id;", "orders", "INSERT"},
		{"Insert compact", "INSERT INTO orders(acc_no,status,amount) VALUES(_,__more__);", "orders", "INSERT"},
		{"Insert two-tuples", "INSERT INTO orders(acc_no, status, amount) VALUES  ( _ ,__more__ ) , (__, __more__) ;", "orders", "INSERT"},
		{"Insert mixed-case", "insert into orders(acc_no, status, amount) Values(_,__more__),( _,__more__ );", "orders", "INSERT"},
		{"Insert three-row", "INSERT INTO orders(acc_no, status, amount) VALUES (_,__more__),( _,__more__ ),(__more__, _);", "orders", "INSERT"},
		{"Insert multi-3 no-cols", "INSERT INTO orders VALUES (_,__more__),( _,__more__),( __more__ , _);", "orders", "INSERT"},

		{"Select IFNULL+WHERE", "SELECT id, IFNULL(sum(amount), _) FROM orders WHERE acc_no = _;", "orders", "WHERE"},
		{"Select IFNULL only", "SELECT IFNULL(status, _) AS name FROM orders;", "orders", "WHERE"},
		{"Select AND", "SELECT * FROM orders WHERE (acc_no = _) AND (id = _);", "orders", "WHERE"},
		{"Select simple IN", "SELECT * FROM orders WHERE id IN (_, __more__, _);", "orders", "WHERE"},
		{"Select ref_data", "SELECT * FROM ref_data WHERE acc_no = _;", "ref_data", "WHERE"},
		{"Select BETWEEN", "SELECT * FROM orders WHERE amount BETWEEN _ AND __more__;", "orders", "WHERE"},
		{"Select between Tuple", "SELECT * FROM orders WHERE amount BETWEEN (_ ) AND (__more__);", "orders", "WHERE"},
		{"Select between with binary expr", "SELECT * FROM orders WHERE amount BETWEEN (_-_) AND (_-_);", "orders", "WHERE"},
		{"Select Limit", "SELECT * FROM orders WHERE amount > _ LIMIT _;", "orders", "LIMIT"},
		{"When Then SET", "UPDATE orders SET status = CASE amount WHEN _ THEN _ WHEN _ THEN _ ELSE _ END WHERE id = _;", "orders", "UPDATE"},
		{"Update tuple CASE", "UPDATE orders SET amount = CASE (amount, status) WHEN (_, __more__) THEN _ ELSE _ END;", "orders", "UPDATE"},

		{"Large IN", "SELECT * FROM orders WHERE id IN (_, __more__, _, __more__, _);", "orders", "WHERE"},
		{"Paren BETWEEN", "SELECT * FROM orders WHERE amount BETWEEN (_ ) AND (__more__);", "orders", "WHERE"},
		{"multi col in", "SELECT * FROM orders WHERE (acc_no, status) IN ((_,__more__),__more__);", "orders", "WHERE"},

		{"Update simple", "UPDATE orders SET status = _ WHERE id = _;", "orders", "UPDATE"},
		{"Update multi-col", "UPDATE orders SET status = _, amount = __more__ WHERE acc_no = _;", "orders", "UPDATE"},
		{"Update with func", "UPDATE orders SET updated_at = now(), status = _ WHERE id = _;", "orders", "UPDATE"},

		{"Update tuple LHS", "UPDATE orders SET (acc_no, id) = (_, __more__) WHERE status = _;", "orders", "UPDATE"},
		{"Update tuple RHS", "UPDATE orders SET (status, amount) = (__more__, _) WHERE id = _;", "orders", "UPDATE"},

		{"Delete simple", "DELETE FROM orders WHERE acc_no = _;", "orders", "WHERE"},
		{"Delete tuple", "DELETE FROM orders WHERE (acc_no, id) = (_, __more__);", "orders", "WHERE"},
	}

	// regex to strip out our metadata tags of form :-:|...|:-:
	tagRE := regexp.MustCompile(`:-:\|.*?\|:-:`)
	// regex to detect standalone placeholders "_" or "__more__"
	placeholderRE := regexp.MustCompile(`\b_\b|\b__more__\b`)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := replacePlaceholders(tt.input, schemas)
			if err != nil {
				t.Fatalf("replacePlaceholders(%q) error: %v", tt.input, err)
			}

			// Remove all metadata tags, leaving regular SQL
			cleaned := tagRE.ReplaceAllString(out, "")

			// Determine if this input actually contained a placeholder token
			hadPlaceholder := placeholderRE.MatchString(tt.input)

			if hadPlaceholder {
				// After stripping tags, no standalone "_" or "__more__" should remain
				if placeholderRE.MatchString(cleaned) {
					t.Errorf("raw placeholder remains after cleaning: %q", cleaned)
				}
				// We expect at least one tag delimiter in the output
				if !strings.Contains(out, ":-:|") {
					t.Errorf("expected placeholder tags in output: %q", out)
				}
				// And the clause and table names should both appear somewhere in those tags
				if !strings.Contains(out, tt.clause) {
					t.Errorf("expected clause %q in tags: %q", tt.clause, out)
				}
				if !strings.Contains(out, tt.table) {
					t.Errorf("expected table %q in tags: %q", tt.table, out)
				}
			} else {
				// No placeholders → output should be unchanged (ignoring whitespace)
				normalizedIn := strings.Join(strings.Fields(tt.input), " ")
				normalizedOut := strings.Join(strings.Fields(out), " ")
				if normalizedIn != normalizedOut {
					t.Errorf("expected no change for %q, got %q", tt.input, out)
				}
			}
		})
	}
}

func TestIsWriteTransaction(t *testing.T) {
	cases := []struct {
		stmts []string
		want  bool
	}{
		{[]string{"SELECT 1"}, false},
		{[]string{"INSERT INTO foo"}, true},
		{[]string{"update foo SET x=1"}, true},
		{[]string{"delete from bar"}, true},
		{[]string{"  select *"}, false},
	}

	for _, c := range cases {
		assert.Equal(t, c.want, isWriteTransaction(c.stmts), "stmts: %v", c.stmts)
	}
}

func TestGetColumnIndexes_Success(t *testing.T) {
	header := "database_name\tapplication_name\ttxn_fingerprint_id\tkey\tother\n"
	tmp := t.TempDir()
	path := filepath.Join(tmp, "stats.txt")
	require.NoError(t, os.WriteFile(path, []byte(header), 0644))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	// Advance scanner to header row
	scanner := bufio.NewScanner(f)
	require.True(t, scanner.Scan())

	idx, err := getColumnIndexes(scanner, f, path)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{
		"database_name":      0,
		"application_name":   1,
		"txn_fingerprint_id": 2,
		"key":                3,
		"other":              4,
	}, idx)
}
