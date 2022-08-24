// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTopicForEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test verifies that topic naming works for various combinations of table/families.
	// Versions prior to 22.1 did not initialize TargetSpecifications, and used Tables instead.
	// Both flavors are tested -- with "old proto" tests testing topic naming with "Tables" field.
	for _, tc := range []struct {
		name      string
		details   jobspb.ChangefeedDetails
		event     cdcevent.Metadata
		expectErr string
		topicName string
	}{
		{
			name: "old proto",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{1: jobspb.ChangefeedTargetTable{StatementTimeName: "t1"}},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "t1"},
			topicName: "t1",
		},
		{
			name: "old proto no such topic",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{
					1: jobspb.ChangefeedTargetTable{StatementTimeName: "t1"},
					2: jobspb.ChangefeedTargetTable{StatementTimeName: "t2"},
				},
			},
			event:     cdcevent.Metadata{TableID: 3},
			expectErr: "no TargetSpecification for row",
		},
		{
			name: "old proto table renamed",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{1: jobspb.ChangefeedTargetTable{StatementTimeName: "old_name"}},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name"},
			topicName: "old_name",
		},
		{
			name: "old proto family ignored",
			details: jobspb.ChangefeedDetails{
				Tables: jobspb.ChangefeedTargets{1: jobspb.ChangefeedTargetTable{StatementTimeName: "t1"}},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "t1", FamilyID: 2, FamilyName: "fam"},
			topicName: "t1",
		},
		{
			name: "full table",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						TableID:           1,
						StatementTimeName: "t1",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "t1"},
			topicName: "t1",
		},
		{
			name: "full table renamed",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						TableID:           1,
						StatementTimeName: "old_name",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name"},
			topicName: "old_name",
		},
		{
			name: "single family",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						Type:              jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
						TableID:           1,
						FamilyName:        "fam",
						StatementTimeName: "t1",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name", FamilyName: "fam", FamilyID: 1},
			topicName: "t1.fam",
		},
		{
			name: "each family",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
						TableID:           1,
						FamilyName:        "fam",
						StatementTimeName: "old_name",
					},
					{
						Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
						TableID:           1,
						FamilyName:        "fam2",
						StatementTimeName: "old_name",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name", FamilyName: "fam2", FamilyID: 2},
			topicName: "old_name.fam2",
		},
		{
			name: "wrong family",
			details: jobspb.ChangefeedDetails{
				TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
					{
						Type:              jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
						TableID:           1,
						FamilyName:        "fam",
						StatementTimeName: "t1",
					},
				},
			},
			event:     cdcevent.Metadata{TableID: 1, TableName: "new_name", FamilyName: "wrong", FamilyID: 0},
			expectErr: "no TargetSpecification for row",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := kvEventToRowConsumer{
				details:              makeChangefeedConfigFromJobDetails(tc.details),
				topicDescriptorCache: make(map[TopicIdentifier]TopicDescriptor),
			}
			tn, err := MakeTopicNamer(AllTargets(tc.details))
			require.NoError(t, err)

			td, err := c.topicForEvent(tc.event)
			if tc.expectErr == "" {
				require.NoError(t, err)
				topicName, err := tn.Name(td)
				require.NoError(t, err)
				require.Equal(t, tc.topicName, topicName)
			} else {
				require.Regexp(t, tc.expectErr, err)
				require.Equal(t, noTopic{}, td)
			}
		})
	}
}
