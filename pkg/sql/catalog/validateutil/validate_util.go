// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package validateutil provides utilities for implementations of descriptor
// validation.
package validateutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// ValidateNamespace can be used to validate that the namespaces entries
// associated with a descriptor are sane.
func ValidateNamespace(
	ctx context.Context, desc catalog.Descriptor, ns catalog.NamespaceGetter,
) error {
	id, ok, err := ns.GetNamespaceEntry(ctx, desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName())
	if err != nil {
		return err
	}
	if !ok {
		if !desc.Dropped() {
			err = errors.CombineErrors(err,
				errors.Errorf("no namespace entry found for (%d, %d, %s)->%d",
					desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName(), desc.GetID()))
		}
	}
	if !desc.Dropped() && id != desc.GetID() {
		err = errors.CombineErrors(err,
			errors.Errorf("namespace entry mismatch for (%d, %d, %s)->%d != %d",
				desc.GetParentID(), desc.GetParentSchemaID(), desc.GetName(), id, desc.GetID()))
	}

	// Now we want to go through all of the draining names and ensure that the
	// entries exist.
	for _, dn := range desc.GetDrainingNames() {
		err = errors.CombineErrors(err, validateDrainingName(ctx, ns, desc, dn))
	}
	return err
}

func validateDrainingName(
	ctx context.Context, ns catalog.NamespaceGetter, desc catalog.Descriptor, ni descpb.NameInfo,
) error {
	id, ok, err := ns.GetNamespaceEntry(ctx, ni.ParentID, ni.ParentSchemaID, ni.Name)
	if err != nil {
		return err
	}
	if !ok {
		err = errors.CombineErrors(err,
			errors.Errorf("no namespace entry found for draining name (%d, %d, %s)->%d",
				ni.ParentID, ni.ParentSchemaID, ni.Name, desc.GetID()))
	}
	if id != desc.GetID() {
		err = errors.CombineErrors(err,
			errors.Errorf("namespace entry mismatch for draining name (%d, %d, %s)->%d != %d",
				ni.ParentID, ni.ParentSchemaID, ni.Name, id, desc.GetID()))
	}
	return err
}
