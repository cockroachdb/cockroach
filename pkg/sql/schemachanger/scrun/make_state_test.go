// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scrun

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestMakeState tests some validation checking in the makeState function.
func TestMakeState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	for _, tc := range []struct {
		name        string
		ids         []descpb.ID
		jobID       jobspb.JobID
		descriptors []catalog.Descriptor
		expErr      string
	}{
		{
			name:   "missing job ID",
			jobID:  1,
			ids:    []descpb.ID{2},
			expErr: `descriptor "foo" \(2\): missing job ID in schema changer state`,
			descriptors: []catalog.Descriptor{
				tabledesc.NewBuilder(&descpb.TableDescriptor{
					Name: "foo",
					ID:   2,
					DeclarativeSchemaChangerState: &scpb.DescriptorState{
						Authorization: scpb.Authorization{
							UserName: "user1",
							AppName:  "app1",
						},
					},
				}).BuildImmutable(),
			},
		},
		{
			name:   "mismatched job ID",
			jobID:  1,
			ids:    []descpb.ID{2},
			expErr: `descriptor "foo" \(2\): job ID mismatch: expected 1, got 2`,
			descriptors: []catalog.Descriptor{
				tabledesc.NewBuilder(&descpb.TableDescriptor{
					Name: "foo",
					ID:   2,
					DeclarativeSchemaChangerState: &scpb.DescriptorState{
						JobID: 2,
						Authorization: scpb.Authorization{
							UserName: "user1",
							AppName:  "app1",
						},
					},
				}).BuildImmutable(),
			},
		},
		{
			name:   "missing authorization",
			jobID:  1,
			ids:    []descpb.ID{2},
			expErr: `descriptor "foo" \(2\): missing authorization in schema changer state`,
			descriptors: []catalog.Descriptor{
				tabledesc.NewBuilder(&descpb.TableDescriptor{
					Name: "foo",
					ID:   2,
					DeclarativeSchemaChangerState: &scpb.DescriptorState{
						JobID: 1,
					},
				}).BuildImmutable(),
			},
		},
		{
			name:   "mismatched authorization",
			jobID:  1,
			ids:    []descpb.ID{2, 3},
			expErr: `descriptor "bar" \(3\): authorization mismatch: expected {user1 app1}, got {user2 app1}`,
			descriptors: []catalog.Descriptor{
				tabledesc.NewBuilder(&descpb.TableDescriptor{
					Name: "foo",
					ID:   2,
					DeclarativeSchemaChangerState: &scpb.DescriptorState{
						JobID: 1,
						Authorization: scpb.Authorization{
							UserName: "user1",
							AppName:  "app1",
						},
					},
				}).BuildImmutable(),
				tabledesc.NewBuilder(&descpb.TableDescriptor{
					Name: "bar",
					ID:   3,
					DeclarativeSchemaChangerState: &scpb.DescriptorState{
						JobID: 1,
						Authorization: scpb.Authorization{
							UserName: "user2",
							AppName:  "app1",
						},
					},
				}).BuildImmutable(),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const jobID = 1
			_, err := makeState(ctx, jobID, tc.ids, tc.descriptors, clusterversion.TestingClusterVersion)
			if tc.expErr == "" {
				require.NoError(t, err)
			} else {
				require.Regexp(t, tc.expErr, err)
			}
		})
	}
}
