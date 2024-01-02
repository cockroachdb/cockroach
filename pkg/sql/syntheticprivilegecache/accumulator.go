// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilegecache

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// accumulator accumulates different rows of system.privileges
// and combines them into one PrivilegeDescriptor.
type accumulator struct {
	desc       *catpb.PrivilegeDescriptor
	objectType privilege.ObjectType
	path       string
}

// newAccumulator initializes a Accumulator.
func newAccumulator(objectType privilege.ObjectType, path string) *accumulator {
	return &accumulator{
		desc: &catpb.PrivilegeDescriptor{
			OwnerProto: username.NodeUserName().EncodeProto(),
		},
		objectType: objectType,
		path:       path,
	}
}

// addRow adds a row from system.privileges into the accumulator.
func (s *accumulator) addRow(path, user tree.DString, privArr, grantOptionArr *tree.DArray) error {
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
	privs, err := privilege.ListFromStrings(privilegeStrings, privilege.OriginFromSystemTable)
	if err != nil {
		return err
	}
	grantOptions, err := privilege.ListFromStrings(grantOptionStrings, privilege.OriginFromSystemTable)
	if err != nil {
		return err
	}
	privsWithGrantOption, err := privilege.ListFromBitField(
		privs.ToBitField()&grantOptions.ToBitField(),
		s.objectType,
	)
	if err != nil {
		return err
	}
	privsWithoutGrantOption, err := privilege.ListFromBitField(
		privs.ToBitField()&^privsWithGrantOption.ToBitField(),
		s.objectType,
	)
	if err != nil {
		return err
	}
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

// finish returns the privilege descriptor.
func (s *accumulator) finish() *catpb.PrivilegeDescriptor {
	return s.desc
}
