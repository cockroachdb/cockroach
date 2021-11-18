package delegate

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
)

func TestCompletions(t *testing.T) {
	tests := []struct {
		stmt                string
		offset              int
		expectedCompletions []string
	}{
		{
			stmt:                "creat",
			expectedCompletions: []string{"CREATE"},
		},
		{
			stmt:                "CREAT",
			expectedCompletions: []string{"CREATE"},
		},
		{
			stmt:                "creat ",
			expectedCompletions: []string{},
		},
		{
			stmt:                "SHOW CREAT",
			expectedCompletions: []string{"CREATE", "CREATEDB", "CREATELOGIN", "CREATEROLE"},
		},
		{
			stmt:                "show creat",
			expectedCompletions: []string{"CREATE", "CREATEDB", "CREATELOGIN", "CREATEROLE"},
		},
		{
			stmt:                "SELECT * FROM test_table_",
			expectedCompletions: []string{"test_table_1", "test_table_2"},
		},
		{
			stmt:                "SELECT * FROM a",
			expectedCompletions: []string{"abc"},
		},
		{
			stmt:                "SELECT * FROM b",
			expectedCompletions: []string{},
		},
	}
	testCat := testcat.New()
	testCat.SetAllTableNames([]string{"abc", "test_table_1", "test_table_2"})
	for _, tc := range tests {
		offset := tc.offset
		if tc.offset == 0 {
			offset = len(tc.stmt)
		}
		completions, err := runShowCompletions(nil, testCat, tc.stmt, offset)
		if err != nil {
			t.Error(err)
		}
		if !(len(completions) == 0 && len(tc.expectedCompletions) == 0) &&
			!reflect.DeepEqual(completions, tc.expectedCompletions) {
			t.Errorf("expected %v, got %v", tc.expectedCompletions, completions)
		}
	}
}

func TestCommonSqlStartCommands(t *testing.T) {
	for _, word := range commonSqlStartCommands {
		i := sort.SearchStrings(lexbase.KeywordNames, strings.ToLower(word))
		if i > len(lexbase.KeywordNames)-1 || lexbase.KeywordNames[i] != strings.ToLower(word) {
			t.Errorf("%s not in lexbase.KeywordNames", word)
		}
	}
}
