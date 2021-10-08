// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

func ValidatePartitioning(immI catalog.TableDescriptor) error {
	imm, ok := immI.(*immutable)
	if !ok {
		return errors.Errorf("expected immutable descriptor")
	}
	return imm.validatePartitioning()
}

func GetPostDeserializationChanges(
	immI catalog.TableDescriptor,
) (PostDeserializationTableDescriptorChanges, error) {
	imm, ok := immI.(*immutable)
	if !ok {
		return PostDeserializationTableDescriptorChanges{}, errors.Errorf("expected immutable descriptor")
	}
	return imm.GetPostDeserializationChanges(), nil
}

var FitColumnToFamily = fitColumnToFamily

func TestingMakeColumn(
	direction descpb.DescriptorMutation_Direction, desc *descpb.ColumnDescriptor,
) catalog.Column {
	return &column{
		maybeMutation: maybeMutation{mutationDirection: direction},
		desc:          desc,
		ordinal:       0,
	}
}
