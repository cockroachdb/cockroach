// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"reflect"
	"testing"
)

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
			stmt: "se",
			expectedCompletions: []string{
				"SEARCH", "SECOND", "SELECT", "SEQUENCE", "SEQUENCES",
				"SERIALIZABLE", "SERVER", "SESSION", "SESSIONS", "SESSION_USER",
				"SET", "SETS", "SETTING", "SETTINGS",
			},
		},
		{
			stmt:                "sel",
			expectedCompletions: []string{"SELECT"},
		},
		{
			stmt:                "create ta",
			expectedCompletions: []string{"TABLE", "TABLES", "TABLESPACE"},
		},
		{
			stmt:                "create ta",
			expectedCompletions: []string{"CREATE"},
			offset:              3,
		},
		{
			stmt:                "select",
			expectedCompletions: []string{"SELECT"},
			offset:              2,
		},
		{
			stmt:                "select ",
			expectedCompletions: []string{},
			offset:              7,
		},
		{
			stmt:                "你好，我的名字是鲍勃 SELECT",
			expectedCompletions: []string{"你好，我的名字是鲍勃"},
			offset:              2,
		},
		{
			stmt:                "你好，我的名字是鲍勃 SELECT",
			expectedCompletions: []string{},
			offset:              11,
		},
		{
			stmt:                "你好，我的名字是鲍勃 SELECT",
			expectedCompletions: []string{"SELECT"},
			offset:              12,
		},
	}
	for _, tc := range tests {
		offset := tc.offset
		if tc.offset == 0 {
			offset = len(tc.stmt)
		}
		completions, err := RunShowCompletions(tc.stmt, offset)
		if err != nil {
			t.Error(err)
		}
		if !(len(completions) == 0 && len(tc.expectedCompletions) == 0) &&
			!reflect.DeepEqual(completions, tc.expectedCompletions) {
			t.Errorf("expected %v, got %v", tc.expectedCompletions, completions)
		}
	}
}
