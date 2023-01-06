// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// TestStartTemplateIsNotStale checks that the hard-coded start template
// is up to date and the idempotent RunPostDeserializationChanges does
// not change it.
func TestStartTemplateIsNotStale(t *testing.T) {
	initialProto := startDescriptor()
	{
		// Populate the template with dummy values to pass validation.
		initialProto.ID = 123
		initialProto.ParentID = 456
		initialProto.UnexposedParentSchemaID = 789
		initialProto.Name = "test"
		initialProto.PrimaryIndex.Name = tabledesc.PrimaryKeyIndexName(initialProto.Name)
	}
	b := tabledesc.NewBuilder(&initialProto)
	require.NoError(t, b.RunPostDeserializationChanges())
	tbl := b.BuildCreatedMutableTable()
	require.NoError(t, desctestutils.TestingValidateSelf(tbl))
	if finalProto := tbl.TableDescriptor; !reflect.DeepEqual(initialProto, finalProto) {
		diff := strings.Join(pretty.Diff(initialProto, finalProto), "\n")
		t.Fatalf("Descriptor protobufs should be equal but aren't:\n%s", diff)
	}
}
