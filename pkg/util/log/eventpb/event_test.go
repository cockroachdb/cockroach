// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventpb

import (
	"testing"

	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
)

func TestEventJSON(t *testing.T) {
	testCases := []struct {
		ev  EventPayload
		exp string
	}{
		// Check that sensitive fields get redaction markers.
		{&CreateDatabase{DatabaseName: "hello"}, `"DatabaseName":"‹hello›"`},
		{&CreateDatabase{DatabaseName: "he‹llo"}, `"DatabaseName":"‹he?llo›"`},

		// Check that strings in arrays are redactable too.
		{&DropDatabase{DatabaseName: "hello", DroppedSchemaObjects: []string{"world", "uni‹verse"}},
			`"DatabaseName":"‹hello›","DroppedSchemaObjects":["‹world›","‹uni?verse›"]`},

		// Check that non-sensitive fields are not redacted.
		{&ReverseSchemaChange{SQLSTATE: "XXUUU"}, `"SQLSTATE":"XXUUU"`},
		{&SetClusterSetting{SettingName: "my.setting"}, `"SettingName":"my.setting"`},
		{&AlterRole{Options: []string{"NOLOGIN", "PASSWORD"}}, `"Options":["NOLOGIN","PASSWORD"]`},
		{&ChangeDatabasePrivilege{CommonSQLPrivilegeEventDetails: CommonSQLPrivilegeEventDetails{
			GrantedPrivileges: []string{"INSERT", "CREATE"},
		}}, `"GrantedPrivileges":["INSERT","CREATE"]`},

		// Check that conditional-sensitive fields are redacted conditionally.
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{User: "root"}}, `"User":"root"`},
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{User: "someother"}}, `"User":"‹someother›"`},
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{ApplicationName: "$ inte‹rnal"}}, `"ApplicationName":"$ inte?rnal"`},
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{ApplicationName: "myapp"}}, `"ApplicationName":"‹myapp›"`},

		// Check that redactable strings get their redaction markers preserved.
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{Statement: "CREATE DATABASE ‹foo›"}}, `"Statement":"CREATE DATABASE ‹foo›"`},

		// Integer and boolean fields are not redactable in any case.
		{&UnsafeDeleteDescriptor{ParentID: 123, Force: true}, `"ParentID":123,"Force":true`},
	}

	for _, tc := range testCases {
		var b redact.RedactableBytes
		_, b = tc.ev.AppendJSONFields(false, b)
		assert.Equal(t, tc.exp, string(b))
	}
}
