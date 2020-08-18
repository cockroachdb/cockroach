// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package doctor_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestExamine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	toBytes := func(tableDesc *descpb.TableDescriptor) []byte {
		desc := descpb.Descriptor{
			Union: &descpb.Descriptor_Table{
				Table: tableDesc,
			},
		}
		res, err := protoutil.Marshal(&desc)
		require.NoError(t, err)
		return res
	}

	tests := []struct {
		descTable []doctor.DescriptorTableRow
		valid     bool
		errStr    string
		expected  string
	}{
		{
			valid:    true,
			expected: "Examining 0 descriptors...\n",
		},
		{
			descTable: []doctor.DescriptorTableRow{{ID: 1, DescBytes: []byte("#$@#@#$#@#")}},
			errStr:    "failed to unmarshal descriptor",
			expected:  "Examining 1 descriptors...\n",
		},
		{
			descTable: []doctor.DescriptorTableRow{{ID: 1, DescBytes: toBytes(&descpb.TableDescriptor{ID: 2})}},
			errStr:    "",
			expected:  "Examining 1 descriptors...\nTable   1: different id in the descriptor: 2",
		},
		{
			descTable: []doctor.DescriptorTableRow{
				{ID: 1, DescBytes: toBytes(&descpb.TableDescriptor{Name: "foo", ID: 1})},
			},
			expected: "Examining 1 descriptors...\nTable   1: invalid parent ID 0\n",
		},
	}

	for i, test := range tests {
		var buf bytes.Buffer
		valid, err := doctor.Examine(test.descTable, false, &buf)
		msg := fmt.Sprintf("Test %d failed!", i+1)
		if test.errStr != "" {
			require.Containsf(t, err.Error(), test.errStr, msg)
		} else {
			require.NoErrorf(t, err, msg)
		}
		require.Equalf(t, test.valid, valid, msg)
		require.Equalf(t, test.expected, buf.String(), msg)
	}
}
