package fktxn

import (
	"fmt"
	"testing"
)

func TestDumpSchema(t *testing.T) {
	tables, fkStmts, ok := generateSchema(2, 6, 0.4)
	if !ok {
		t.Skip("seed produced computed columns")
	}
	for _, tbl := range tables {
		fmt.Printf("CREATE TABLE %s%s;\n\n", tbl.Name, tbl.Schema)
	}
	for _, s := range fkStmts {
		fmt.Printf("%s;\n", s.String())
	}
}
