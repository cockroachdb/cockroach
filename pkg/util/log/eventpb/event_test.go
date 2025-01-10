// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eventpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
)

func TestEventJSON(t *testing.T) {
	testCases := []struct {
		ev  logpb.EventPayload
		exp string
	}{
		// Check that names do not get redaction markers.
		{&CreateDatabase{DatabaseName: "hello"}, `"DatabaseName":"hello"`},
		{&CreateDatabase{DatabaseName: "he‹llo"}, `"DatabaseName":"he‹llo"`},

		// Check that names in arrays are not redacted either.
		{&DropDatabase{DatabaseName: "hello", DroppedSchemaObjects: []string{"world", "uni‹verse"}},
			`"DatabaseName":"hello","DroppedSchemaObjects":["world","uni‹verse"]`},

		// Check that non-sensitive fields are not redacted.
		{&ReverseSchemaChange{SQLSTATE: "XXUUU"}, `"SQLSTATE":"XXUUU"`},
		{&SetClusterSetting{SettingName: "my.setting"}, `"SettingName":"my.setting"`},
		{&AlterRole{Options: []string{"NOLOGIN", "PASSWORD"}}, `"Options":["NOLOGIN","PASSWORD"]`},
		{&AlterRole{SetInfo: []string{"DEFAULTSETTINGS"}}, `"SetInfo":["DEFAULTSETTINGS"]`},
		{&GrantRole{GranteeRoles: []string{"role1", "role2"}, Members: []string{"role3", " role4"}}, `"GranteeRoles":["‹role1›","‹role2›"],"Members":["‹role3›","‹ role4›"]`},
		{&ChangeDatabasePrivilege{CommonSQLPrivilegeEventDetails: CommonSQLPrivilegeEventDetails{
			GrantedPrivileges: []string{"INSERT", "CREATE"},
		}}, `"GrantedPrivileges":["INSERT","CREATE"]`},

		// Check that conditional-sensitive fields are redacted conditionally.
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{User: "root"}}, `"User":"root"`},
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{User: "someother"}}, `"User":"‹someother›"`},
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{ApplicationName: "$ inte‹rnal"}}, `"ApplicationName":"$ inte‹rnal"`},
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{ApplicationName: "myapp"}}, `"ApplicationName":"myapp"`},

		// Check that redactable strings get their redaction markers preserved.
		{&CreateDatabase{CommonSQLEventDetails: CommonSQLEventDetails{Statement: "CREATE DATABASE ‹foo›"}},
			`"Statement":"CREATE DATABASE ‹foo›"`},

		// Integer and boolean fields are not redactable in any case.
		{&UnsafeDeleteDescriptor{ParentID: 123, Force: true}, `"ParentID":123,"Force":true`},

		// Primitive fields without an `includeempty` annotation will NOT emit their
		// zero value. In this case, `SnapshotID` and `NumRecords` do not have the
		// `includeempty` annotation, so nothing is emitted, despite the presence of
		// zero values.
		{&SchemaSnapshotMetadata{SnapshotID: "", NumRecords: 0}, ""},

		// Primitive fields with an `includeempty` annotation will emit their zero
		// value.
		{
			&StoreStats{Levels: []LevelStats{{Level: 0, NumFiles: 1}, {Level: 6, NumFiles: 2}}},
			`"Levels":[{"Level":0,"NumFiles":1},{"Level":6,"NumFiles":2}]`,
		},
	}

	for _, tc := range testCases {
		var b redact.RedactableBytes
		_, b = tc.ev.AppendJSONFields(false, b)
		assert.Equal(t, tc.exp, string(b))
	}
}
