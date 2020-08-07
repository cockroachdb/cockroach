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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ catalog.TableDescriptor = (*ImmutableTableDescriptor)(nil)
var _ catalog.TableDescriptor = (*MutableTableDescriptor)(nil)

// Immutable implements the MutableDescriptor interface.
func (desc *MutableTableDescriptor) Immutable() catalog.Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableTableDescriptor(*protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor))
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *MutableTableDescriptor) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}
