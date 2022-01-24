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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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

// noopValidationErrorAccumulator implements catalog.ValidationErrorAccumulator
type noopValidationErrorAccumulator struct {
}

// Report implements catalog.ValidationErrorAccumulator
func (*noopValidationErrorAccumulator) Report(err error) {
}

// IsActive implements catalog.ValidationErrorAccumulator
func (*noopValidationErrorAccumulator) IsActive(version clusterversion.Key) bool {
	return true
}

func ValidateConstraints(immI catalog.TableDescriptor) error {
	imm, ok := immI.(*immutable)
	if !ok {
		return errors.Errorf("expected immutable descriptor")
	}
	return imm.validateConstraintIDs(&noopValidationErrorAccumulator{})
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
