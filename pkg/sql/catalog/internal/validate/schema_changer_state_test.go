// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validate_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

func TestValidateSchemaChangerState(t *testing.T) {
	// TODO(ajwerner): bitmask the cases for which descriptor types they
	// should apply to when verifying that the elements make sense given
	// the descriptor targets.
	type ds = scpb.DescriptorState
	type mkDesc = func(name string, id descpb.ID, s scpb.DescriptorState) catalog.Descriptor
	type testCase struct {
		name           string
		id             descpb.ID
		ds             ds
		expectedErrors []string
	}
	runTestCase := func(t *testing.T, c testCase, mk mkDesc) {
		d := mk(c.name, c.id, c.ds)
		errs := validate.TestingSchemaChangerState(context.Background(), d)
		require.Equalf(t, len(errs), len(c.expectedErrors),
			"expected %d errors, got %d: %q", len(c.expectedErrors), len(errs), errs)
		for i, exp := range c.expectedErrors {
			require.Regexp(t, exp, errs[i])
		}
	}
	const prefix = validate.InvalidSchemaChangerStatePrefix
	testCases := []testCase{
		{
			name: "no job ID",
			ds:   ds{},
			expectedErrors: []string{
				prefix + " empty job ID",
			},
		},
		{
			name:           "no targets",
			ds:             ds{JobID: 1},
			expectedErrors: []string{},
		},
		{
			name: "target desc ID mismatch, and missing ranks and status",
			ds: ds{
				JobID: 1,
				Targets: []scpb.Target{
					scpb.MakeTarget(scpb.ToPublic, &scpb.Namespace{
						DatabaseID:   2,
						SchemaID:     2,
						DescriptorID: 3,
						Name:         "foo",
					}, &scpb.TargetMetadata{
						SubWorkID:       1,
						SourceElementID: 1,
						StatementID:     2,
					}),
				},
			},
			expectedErrors: []string{
				prefix + " target 0 corresponds to descriptor 3 != 0",
				prefix + " number mismatch between Targets and TargetRanks: 1 != 0",
				prefix + " number mismatch between Targets and CurentStatuses: 1 != 0",
			},
		},
		{
			name: "statement has wrong target",
			id:   3,
			ds: ds{
				JobID: 1,
				Targets: []scpb.Target{
					scpb.MakeTarget(scpb.ToPublic, &scpb.Namespace{
						DatabaseID:   2,
						SchemaID:     2,
						DescriptorID: 3,
						Name:         "foo",
					}, &scpb.TargetMetadata{
						SubWorkID:       1,
						SourceElementID: 1,
						StatementID:     2,
					}),
				},
				CurrentStatuses: []scpb.Status{scpb.Status_ABSENT},
				TargetRanks:     []uint32{0},
				RelevantStatements: []scpb.DescriptorState_Statement{
					{
						Statement: scpb.Statement{
							Statement: "ALTER TABLE a RENAME TO b",
						},
						StatementRank: 0,
					},
				},
			},
			expectedErrors: []string{
				prefix + ` unexpected statement 0 \(ALTER TABLE a RENAME TO b\)`,
				prefix + ` missing statement for targets \(0\) / \(Namespace:\{DescID: 3, Name: foo, ReferencedDescID: 2\}\)`,
			},
		},
		{
			name: "statements are out of order",
			id:   3,
			ds: ds{
				JobID: 1,
				Targets: []scpb.Target{
					scpb.MakeTarget(scpb.ToPublic, &scpb.Namespace{
						DatabaseID:   2,
						SchemaID:     2,
						DescriptorID: 3,
						Name:         "foo",
					}, &scpb.TargetMetadata{
						SubWorkID:       1,
						SourceElementID: 1,
						StatementID:     0,
					}),
					scpb.MakeTarget(scpb.ToPublic, &scpb.Namespace{
						DatabaseID:   2,
						SchemaID:     2,
						DescriptorID: 3,
						Name:         "foo",
					}, &scpb.TargetMetadata{
						SubWorkID:       1,
						SourceElementID: 1,
						StatementID:     1,
					}),
				},
				CurrentStatuses: []scpb.Status{scpb.Status_ABSENT, scpb.Status_ABSENT},
				TargetRanks:     []uint32{0, 1},
				RelevantStatements: []scpb.DescriptorState_Statement{
					{
						Statement: scpb.Statement{
							Statement: "ALTER TABLE a RENAME TO b",
						},
						StatementRank: 1,
					},
					{
						Statement: scpb.Statement{
							Statement: "ALTER TABLE b RENAME TO a",
						},
						StatementRank: 0,
					},
				},
			},
			expectedErrors: []string{
				prefix + ` RelevantStatements are not sorted`,
			},
		},
		{
			name: "duplicate target ranks",
			id:   3,
			ds: ds{
				JobID: 1,
				Targets: []scpb.Target{
					scpb.MakeTarget(scpb.ToPublic, &scpb.Namespace{
						DatabaseID:   2,
						SchemaID:     2,
						DescriptorID: 3,
						Name:         "foo",
					}, &scpb.TargetMetadata{
						SubWorkID:       1,
						SourceElementID: 1,
						StatementID:     1,
					}),
					scpb.MakeTarget(scpb.ToPublic, &scpb.Namespace{
						DatabaseID:   2,
						SchemaID:     2,
						DescriptorID: 3,
						Name:         "foo",
					}, &scpb.TargetMetadata{
						SubWorkID:       1,
						SourceElementID: 1,
						StatementID:     1,
					}),
				},
				CurrentStatuses: []scpb.Status{scpb.Status_ABSENT, scpb.Status_ABSENT},
				TargetRanks:     []uint32{1, 1},
				RelevantStatements: []scpb.DescriptorState_Statement{
					{
						Statement: scpb.Statement{
							Statement: "ALTER TABLE a RENAME TO b",
						},
						StatementRank: 1,
					},
				},
			},
			expectedErrors: []string{
				prefix + ` TargetRanks contains duplicate entries \[1\]`,
			},
		},
	}

	mkFuncs := []struct {
		name string
		mk   mkDesc
	}{
		{
			name: "type",
			mk: func(name string, id descpb.ID, s scpb.DescriptorState) catalog.Descriptor {
				return typedesc.NewBuilder(&descpb.TypeDescriptor{
					Name:                          name,
					ID:                            id,
					Version:                       1,
					ParentID:                      1,
					ParentSchemaID:                29,
					State:                         descpb.DescriptorState_PUBLIC,
					DeclarativeSchemaChangerState: &s,
				}).BuildCreatedMutable()
			},
		},
		{
			name: "database",
			mk: func(name string, id descpb.ID, s scpb.DescriptorState) catalog.Descriptor {
				return dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
					Name:                          name,
					ID:                            id,
					Version:                       1,
					State:                         descpb.DescriptorState_PUBLIC,
					DeclarativeSchemaChangerState: &s,
				}).BuildCreatedMutable()
			},
		},
		{
			name: "schema",
			mk: func(name string, id descpb.ID, s scpb.DescriptorState) catalog.Descriptor {
				return schemadesc.NewBuilder(&descpb.SchemaDescriptor{
					Name:                          name,
					ID:                            id,
					ParentID:                      1,
					Version:                       1,
					State:                         descpb.DescriptorState_PUBLIC,
					DeclarativeSchemaChangerState: &s,
				}).BuildCreatedMutable()
			},
		},
		{
			name: "table",
			mk: func(name string, id descpb.ID, s scpb.DescriptorState) catalog.Descriptor {
				return tabledesc.NewBuilder(&descpb.TableDescriptor{
					Name:                          name,
					ID:                            id,
					ParentID:                      1,
					Version:                       1,
					State:                         descpb.DescriptorState_PUBLIC,
					UnexposedParentSchemaID:       29,
					DeclarativeSchemaChangerState: &s,
				}).BuildCreatedMutable()
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			for _, mk := range mkFuncs {
				t.Run(mk.name, func(t *testing.T) {
					runTestCase(t, c, mk.mk)
				})
			}
		})
	}
}
