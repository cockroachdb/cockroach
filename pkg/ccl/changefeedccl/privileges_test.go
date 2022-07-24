// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type authTest struct {
	name                 string
	tables               []catalog.Descriptor
	privileges           []mockedPrivilege
	hasControlChangefeed bool
	acceptableErrors     []error
}

type mockedPrivilege struct {
	id   descpb.ID
	kind privilege.Kind
}

type mockedDatabase struct {
	*dbdesc.Mutable
	t testing.TB
}

func (m mockedDatabase) table(i int) catalog.TableDescriptor {
	dID := int(m.GetID())
	return dummyTableDesc(m.t, i+100*dID, dID)
}

// CheckPrivilege implements the singlePrivilegeChecker interface.
func (t authTest) CheckPrivilege(
	_ context.Context, d catalog.PrivilegeObject, k privilege.Kind,
) error {
	for _, priv := range t.privileges {
		if priv.id == d.(catalog.Descriptor).GetID() && priv.kind == k {
			return nil
		}
	}
	return err(d, k)
}

// HasRoleOption implements the singlePrivilegeChecker interface.
func (t authTest) HasRoleOption(_ context.Context, _ roleoption.Option) (bool, error) {
	return t.hasControlChangefeed, nil
}

type missingPrivilege struct {
	id int
	k  privilege.Kind
}

func (m missingPrivilege) Error() string {
	return fmt.Sprintf("%d %s", m.id, m.k)
}

func err(d catalog.PrivilegeObject, k privilege.Kind) error {
	return missingPrivilege{int(d.(catalog.Descriptor).GetID()), k}
}

var _ singlePrivilegeChecker = authTest{}

func TestPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	db1 := mockedDatabase{dbdesc.NewInitial(1, "db1", username.SQLUsername{}), t}
	db2 := mockedDatabase{dbdesc.NewInitial(2, "db2", username.SQLUsername{}), t}

	dbDescs := []catalog.Descriptor{db1, db2}

	selectOn := func(d catalog.Descriptor) (r []mockedPrivilege) {
		return append(r, mockedPrivilege{id: d.GetID(), kind: privilege.SELECT})
	}
	changefeedOn := func(d catalog.Descriptor) (r []mockedPrivilege) {
		return append(r, mockedPrivilege{id: d.GetID(), kind: privilege.CHANGEFEED})
	}
	bothPrivilegesOn := func(d catalog.Descriptor) (r []mockedPrivilege) {
		return append(selectOn(d), changefeedOn(d)...)
	}

	singleTableAuthPassingTests := []authTest{
		{name: "direct privilege",
			tables: []catalog.Descriptor{db1.table(1)}, privileges: bothPrivilegesOn(db1.table(1))},
		{name: "inherited privilege",
			tables: []catalog.Descriptor{db1.table(2)}, privileges: bothPrivilegesOn(db1)},
		{name: "one direct one inherited",
			tables: []catalog.Descriptor{db1.table(3)}, privileges: append(selectOn(db1), changefeedOn(db1.table(3))...)},
	}

	singleTableAuthFailingTests := []authTest{
		{name: "missing select", tables: []catalog.Descriptor{db2.table(1)}, privileges: changefeedOn(db2.table(1)),
			acceptableErrors: []error{err(db2.table(1), privilege.SELECT)},
		},
		{name: "missing changefeed", tables: []catalog.Descriptor{db2.table(1)}, privileges: selectOn(db2),
			acceptableErrors: []error{err(db2.table(1), privilege.CHANGEFEED)},
		},
	}

	add := func(tests ...authTest) (combined authTest) {
		for i, at := range tests {
			if i > 0 {
				combined.name += "+"
			}
			combined.name += at.name
			combined.tables = append(combined.tables, at.tables...)
			combined.acceptableErrors = append(combined.acceptableErrors, at.acceptableErrors...)
			combined.privileges = append(combined.privileges, at.privileges...)
		}
		return combined
	}

	tests := append(singleTableAuthPassingTests, singleTableAuthFailingTests...)
	var multiTableTests []authTest

	// Adding in a target with the correct privileges to any other test
	// should not change the result of that test, as long as the targets
	// are different.
	for i, x := range singleTableAuthPassingTests {
		for j, y := range tests {
			if i != j {
				multiTableTests = append(multiTableTests, add(x, y))
			}
		}
	}

	tests = append(tests, multiTableTests...)

	var testsWithControlChangefeed []authTest

	changefeedPrivilegeNotNeeded := func(oldErrs []error) (filtered []error) {
		for _, e := range oldErrs {
			var mp missingPrivilege
			if !errors.As(e, &mp) || mp.k != privilege.CHANGEFEED {
				filtered = append(filtered, e)
			}
		}
		return filtered
	}

	// Replacing all grants of the CHANGEFEED privilege with the global
	// CONTROLCHANGEFEED role option should not change the result of any
	// test except to make it impossible for a missing CHANGEFEED privilege
	// error to be returned.
	for _, at := range tests {
		newTest := at
		newTest.hasControlChangefeed = true
		newTest.name = "ControlChangefeed/" + at.name
		newTest.acceptableErrors = changefeedPrivilegeNotNeeded(at.acceptableErrors)
		testsWithControlChangefeed = append(testsWithControlChangefeed, newTest)
	}

	tests = append(tests, testsWithControlChangefeed...)

	t.Logf("Running %d generated test cases", len(tests))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkPrivileges(context.Background(), test, append(dbDescs, test.tables...))
			if len(test.acceptableErrors) == 0 {
				require.NoError(t, err)
			} else {
				require.Contains(t, test.acceptableErrors, err)
			}
		})
	}

}
