// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilege

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Accumulator accumulates different rows of system.privileges
// and combines them into one PrivilegeDescriptor.
type Accumulator struct {
	desc       *catpb.PrivilegeDescriptor
	objectType privilege.ObjectType
	path       string
}

// NewAccumulator initializes a Accumulator.
func NewAccumulator(objectType privilege.ObjectType, path string) *Accumulator {
	return &Accumulator{
		desc:       &catpb.PrivilegeDescriptor{},
		objectType: objectType,
		path:       path,
	}
}

// AddRow adds a row from system.privileges into the accumulator.
func (s *Accumulator) AddRow(path, user tree.DString, privArr, grantOptionArr *tree.DArray) error {
	if string(path) != s.path {
		return errors.AssertionFailedf("mismatched path in Accumulator; expected %s, got %s", s.path, path)
	}

	var privilegeStrings []string
	for _, elem := range privArr.Array {
		privilegeStrings = append(privilegeStrings, string(tree.MustBeDString(elem)))
	}

	var grantOptionStrings []string
	for _, elem := range grantOptionArr.Array {
		grantOptionStrings = append(grantOptionStrings, string(tree.MustBeDString(elem)))
	}
	privs, err := privilege.ListFromStrings(privilegeStrings)
	if err != nil {
		return err
	}
	grantOptions, err := privilege.ListFromStrings(grantOptionStrings)
	if err != nil {
		return err
	}
	privsWithGrantOption := privilege.ListFromBitField(
		privs.ToBitField()&grantOptions.ToBitField(),
		s.objectType,
	)
	privsWithoutGrantOption := privilege.ListFromBitField(
		privs.ToBitField()&^privsWithGrantOption.ToBitField(),
		s.objectType,
	)
	s.desc.Grant(
		username.MakeSQLUsernameFromPreNormalizedString(string(user)),
		privsWithGrantOption,
		true, /* withGrantOption */
	)
	s.desc.Grant(
		username.MakeSQLUsernameFromPreNormalizedString(string(user)),
		privsWithoutGrantOption,
		false, /* withGrantOption */
	)
	return nil
}

// GetDescriptor returns the privilege descriptor.
func (s *Accumulator) GetDescriptor() *catpb.PrivilegeDescriptor {
	return s.desc
}
