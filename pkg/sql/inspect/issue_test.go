// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestIssueRedaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc       string
		issue      *inspectIssue
		redactable string
		redacted   string
	}{
		{
			desc: "no details",
			issue: &inspectIssue{
				ErrorType:  "testing_error",
				DatabaseID: 10,
				ObjectID:   101,
				PrimaryKey: "/Table/101/c=10",
			},
			redactable: "{type=testing_error db=10 obj=101 pk=‹\"/Table/101/c=10\"›}",
			redacted:   "{type=testing_error db=10 obj=101 pk=‹×›}",
		},
		{
			desc: "with details",
			issue: &inspectIssue{
				ErrorType:  "testing_system_error",
				SchemaID:   100,
				PrimaryKey: "/System/100/",
				Details: map[redact.RedactableString]interface{}{
					"field1": 10,
					"field2": "strval",
				},
			},
			redactable: "{type=testing_system_error schema=100 pk=‹\"/System/100/\"› details=map[field1:10 field2:‹strval›]}",
			redacted:   "{type=testing_system_error schema=100 pk=‹×› details=map[field1:10 field2:‹×›]}",
		},
		{
			desc: "details that has a redactable string value",
			issue: &inspectIssue{
				ErrorType: "testing_system_error",
				Details: map[redact.RedactableString]interface{}{
					"field1": redact.RedactableString("redactable string"),
				},
			},
			redactable: "{type=testing_system_error pk=‹\"\"› details=map[field1:redactable string]}",
			redacted:   "{type=testing_system_error pk=‹×› details=map[field1:redactable string]}",
		},
	}
	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			require.EqualValues(t, c.redactable, redact.Sprint(c.issue))
			require.EqualValues(t, c.redacted, redact.Sprint(c.issue).Redact())
		})
	}
}
