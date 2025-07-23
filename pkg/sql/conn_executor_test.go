package sql

import (
    "testing"
    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestContainsTxnControlStmt(t *testing.T) {
    tests := []struct {
        stmt string
        expected bool
    }{
        {"COMMIT;", true},
        {"ROLLBACK;", true},
        {"SELECT 1;", false},
    }
    for _, tc := range tests {
        stmt, err := parser.ParseOne(tc.stmt)
        if err != nil {
            t.Fatalf("could not parse stmt '%s': %v", tc.stmt, err)
        }
        result := containsTxnControlStmt(stmt.AST)
        if result != tc.expected {
            t.Errorf("expected %v, got %v for stmt '%s'", tc.expected, result, tc.stmt)
        }
    }
}