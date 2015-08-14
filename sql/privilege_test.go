// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)
	descriptor := sql.NewDefaultDatabasePrivilegeDescriptor()

	testCases := []struct {
		grantee       string // User to grant/revoke privileges on.
		grant, revoke privilege.List
		show          []sql.UserPrivilegeString
	}{
		{"", nil, nil,
			[]sql.UserPrivilegeString{{"root", "ALL"}},
		},
		{"root", privilege.List{privilege.ALL}, nil,
			[]sql.UserPrivilegeString{{"root", "ALL"}},
		},
		{"root", privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]sql.UserPrivilegeString{{"root", "ALL"}},
		},
		{"foo", privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]sql.UserPrivilegeString{{"foo", "DROP,INSERT"}, {"root", "ALL"}},
		},
		{"bar", nil, privilege.List{privilege.INSERT, privilege.ALL},
			[]sql.UserPrivilegeString{{"foo", "DROP,INSERT"}, {"root", "ALL"}},
		},
		{"foo", privilege.List{privilege.ALL}, nil,
			[]sql.UserPrivilegeString{{"foo", "ALL"}, {"root", "ALL"}},
		},
		{"foo", nil, privilege.List{privilege.SELECT, privilege.INSERT, privilege.READ, privilege.WRITE},
			[]sql.UserPrivilegeString{{"foo", "CREATE,DELETE,DROP,GRANT,UPDATE"}, {"root", "ALL"}},
		},
		{"foo", nil, privilege.List{privilege.ALL},
			[]sql.UserPrivilegeString{{"root", "ALL"}},
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{"root", nil, privilege.List{privilege.ALL},
			[]sql.UserPrivilegeString{},
		},
	}

	for tcNum, tc := range testCases {
		if tc.grantee != "" {
			if tc.grant != nil {
				descriptor.Grant(tc.grantee, tc.grant)
			}
			if tc.revoke != nil {
				descriptor.Revoke(tc.grantee, tc.revoke)
			}
		}
		show, err := descriptor.Show()
		if err != nil {
			t.Fatal(err)
		}
		if len(show) != len(tc.show) {
			t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
				tcNum, descriptor, show, tc.show)
		}
		for i := 0; i < len(show); i++ {
			if show[i].User != tc.show[i].User || show[i].Privileges != tc.show[i].Privileges {
				t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
					tcNum, descriptor, show, tc.show)
			}
		}
	}
}

func TestPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)
	descriptor := sql.NewDefaultDatabasePrivilegeDescriptor()
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant("foo", privilege.List{privilege.ALL})
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant("root", privilege.List{privilege.SELECT})
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke("root", privilege.List{privilege.SELECT})
	if err := descriptor.Validate(); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant("root", privilege.List{privilege.SELECT})
	if err := descriptor.Validate(); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke("root", privilege.List{privilege.ALL})
	if err := descriptor.Validate(); err == nil {
		t.Fatal("unexpected success")
	}
}
