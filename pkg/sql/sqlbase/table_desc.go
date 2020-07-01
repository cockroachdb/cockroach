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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ TableDescriptorInterface = (*ImmutableTableDescriptor)(nil)
var _ TableDescriptorInterface = (*MutableTableDescriptor)(nil)

// TableDescriptorInterface is an interface around the table descriptor types.
//
// TODO(ajwerner): This interface likely belongs in a catalog/tabledesc package
// or perhaps in the catalog package directly. It's not clear how expansive this
// interface should be. Perhaps very.
type TableDescriptorInterface interface {
	BaseDescriptorInterface

	TableDesc() *TableDescriptor
	FindColumnByName(name tree.Name) (*ColumnDescriptor, bool, error)
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableTableDescriptor) Immutable() DescriptorInterface {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableTableDescriptor(*protoutil.Clone(desc.TableDesc()).(*TableDescriptor))
}
