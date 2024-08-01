// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestDescriptorsAreNotCopied validate that descriptors that have
// survived post de-serialziation don't have to go through it again.
func TestDescriptorsAreNotCopied(t *testing.T) {
	// Sanity: Loop over the system tables and confirm that
	// PostDeserialization does zero work on updated descriptors.
	for _, targetDesc := range systemschema.MakeSystemTables() {
		ts := hlc.Timestamp{WallTime: timeutil.Now().Unix()}
		b := tabledesc.NewBuilderWithMVCCTimestamp(targetDesc.TableDesc(), ts)
		require.NoError(t, b.RunPostDeserializationChanges())
		require.Falsef(t, b.DescriptorWasModified(),
			"%s descriptor was copied again", targetDesc.GetName())
	}
}
