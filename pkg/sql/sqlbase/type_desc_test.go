// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTypeDescIsCompatibleWith(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		a TypeDescriptor
		b TypeDescriptor
		// If err == "", then no error is expected. Otherwise, an error that
		// matches is expected.
		err string
	}{
		// Different type kinds shouldn't be equal.
		{
			a: TypeDescriptor{
				Name: "a",
				Kind: TypeDescriptor_ENUM,
			},
			b: TypeDescriptor{
				Name: "b",
				Kind: TypeDescriptor_ALIAS,
			},
			err: `"b" is not an enum`,
		},
		// We aren't considering compatibility between different alias kinds.
		{
			a: TypeDescriptor{
				Kind: TypeDescriptor_ALIAS,
			},
			b: TypeDescriptor{
				Kind: TypeDescriptor_ALIAS,
			},
			err: `compatibility comparison unsupported`,
		},
		// The empty enum should be compatible with any other enums.
		{
			a: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
			},
			b: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
				},
			},
			err: ``,
		},
		// The same enum should be compatible with itself.
		{
			a: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: ``,
		},
		// An enum with only some members of another enum should be compatible.
		{
			a: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: ``,
		},
		// An enum with missing members shouldn't be compatible.
		{
			a: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "howdy",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: `could not find enum value "howdy"`,
		},
		// An enum with a different physical representation shouldn't be compatible.
		{
			a: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{201},
					},
				},
			},
			b: TypeDescriptor{
				Kind: TypeDescriptor_ENUM,
				EnumMembers: []TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: `has differing physical representation for value "hi"`,
		},
	}

	for _, test := range tests {
		err := test.a.IsCompatibleWith(&test.b)
		if test.err == "" {
			require.NoError(t, err)
		} else {
			if !testutils.IsError(err, test.err) {
				t.Errorf("expected error %s, but found %s", test.err, err)
			}
		}
	}
}
