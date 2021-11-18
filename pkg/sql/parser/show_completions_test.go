package parser

import (
	"reflect"
	"testing"
)

func TestGetLastWordFromStmt(t *testing.T) {
	tests := []struct {
		stmt             string
		expectedLastWord string
	}{
		{
			stmt:             "SELECT * FRO",
			expectedLastWord: "FRO",
		},
		{
			stmt:             "SELECT * FROM movr.rid",
			expectedLastWord: "rid",
		},
		{
			stmt:             "SELECT * FROM t@prim",
			expectedLastWord: "prim",
		},
		{
			stmt:             "SELECT 1",
			expectedLastWord: "1",
		},
		{
			stmt:             "SELECT ",
			expectedLastWord: "",
		},
		{
			stmt:             "",
			expectedLastWord: "",
		},
	}
	for _, tc := range tests {
		lastWord := getLastWordFromStmt(tc.stmt)
		if lastWord != tc.expectedLastWord {
			t.Errorf("expected %s, got %s", tc.expectedLastWord, lastWord)
		}
	}
}

func TestCompletions(t *testing.T) {
	tests := []struct {
		stmt                string
		offset              int
		expectedCompletions []string
	}{
		{
			stmt:                "creat",
			expectedCompletions: []string{"CREATE", "CREATEDB", "CREATELOGIN", "CREATEROLE"},
		},
		{
			stmt:                "CREAT",
			expectedCompletions: []string{"CREATE", "CREATEDB", "CREATELOGIN", "CREATEROLE"},
		},
		{
			stmt:                "SELECT * FROM test_table_",
			expectedCompletions: []string{"test_table_1", "test_table_2"},
		},
	}
	for _, tc := range tests {
		offset := tc.offset
		if tc.offset == 0 {
			offset = len(tc.stmt)
		}
		testTables := []string{"test_table_1", "test_table_2"}
		completions := RunShowCompletions(tc.stmt, offset, testTables)
		if !reflect.DeepEqual(completions, tc.expectedCompletions) {
			t.Errorf("expected %v, got %v", tc.expectedCompletions, completions)
		}
	}
}
