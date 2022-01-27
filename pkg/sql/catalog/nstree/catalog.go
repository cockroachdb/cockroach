// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nstree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/errors"
)

// Catalog is used to store an in-memory copy of the whole catalog, or a portion
// thereof.
type Catalog struct {
	underlying Map
}

// ForEachDescriptorEntry iterates over all descriptor table entries in an
// ordered fashion.
func (c Catalog) ForEachDescriptorEntry(fn func(desc catalog.Descriptor) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.underlying.byID.ascend(func(e catalog.NameEntry) error {
		return fn(e.(catalog.Descriptor))
	})
}

// ForEachNamespaceEntry iterates over all namespace table entries in an ordered
// fashion.
func (c Catalog) ForEachNamespaceEntry(fn func(e catalog.NameEntry) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.underlying.byName.ascend(fn)
}

// LookupDescriptorEntry looks up a descriptor by ID.
func (c Catalog) LookupDescriptorEntry(id descpb.ID) catalog.Descriptor {
	if !c.IsInitialized() || id == descpb.InvalidID {
		return nil
	}
	e := c.underlying.byID.get(id)
	if e == nil {
		return nil
	}
	return e.(catalog.Descriptor)
}

// LookupNamespaceEntry looks up a descriptor ID by name.
func (c Catalog) LookupNamespaceEntry(key catalog.NameKey) descpb.ID {
	if !c.IsInitialized() || key == nil {
		return descpb.InvalidID
	}
	e := c.underlying.byName.getByName(key.GetParentID(), key.GetParentSchemaID(), key.GetName())
	if e == nil {
		return descpb.InvalidID
	}
	return e.GetID()
}

// OrderedDescriptors returns the descriptors in an ordered fashion.
func (c Catalog) OrderedDescriptors() []catalog.Descriptor {
	if !c.IsInitialized() {
		return nil
	}
	ret := make([]catalog.Descriptor, 0, c.underlying.byID.t.Len())
	_ = c.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
		ret = append(ret, desc)
		return nil
	})
	return ret
}

// OrderedDescriptorIDs returns the descriptor IDs in an ordered fashion.
func (c Catalog) OrderedDescriptorIDs() []descpb.ID {
	if !c.IsInitialized() {
		return nil
	}
	ret := make([]descpb.ID, 0, c.underlying.byName.t.Len())
	_ = c.ForEachNamespaceEntry(func(e catalog.NameEntry) error {
		ret = append(ret, e.GetID())
		return nil
	})
	return ret
}

// IsInitialized returns false if the underlying map has not yet been
// initialized. Initialization is done lazily when
func (c Catalog) IsInitialized() bool {
	return c.underlying.initialized()
}

var _ validate.ValidationDereferencer = Catalog{}
var _ validate.ValidationDereferencer = MutableCatalog{}

// DereferenceDescriptors implements the validate.ValidationDereferencer
// interface.
func (c Catalog) DereferenceDescriptors(
	ctx context.Context, version clusterversion.ClusterVersion, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	ret := make([]catalog.Descriptor, len(reqs))
	for i, id := range reqs {
		ret[i] = c.LookupDescriptorEntry(id)
	}
	return ret, nil
}

// DereferenceDescriptorIDs implements the validate.ValidationDereferencer
// interface.
func (c Catalog) DereferenceDescriptorIDs(
	_ context.Context, reqs []descpb.NameInfo,
) ([]descpb.ID, error) {
	ret := make([]descpb.ID, len(reqs))
	for i, req := range reqs {
		ret[i] = c.LookupNamespaceEntry(req)
	}
	return ret, nil
}

// Validate delegates to validate.Validate.
func (c Catalog) Validate(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	telemetry catalog.ValidationTelemetry,
	targetLevel catalog.ValidationLevel,
	descriptors ...catalog.Descriptor,
) (ve catalog.ValidationErrors) {
	return validate.Validate(ctx, version, c, telemetry, targetLevel, descriptors...)
}

// ValidateWithRecover is like Validate but which recovers from panics.
// This is useful when we're validating many descriptors separately and we don't
// want a corrupt descriptor to prevent validating the others.
func (c Catalog) ValidateWithRecover(
	ctx context.Context, version clusterversion.ClusterVersion, desc catalog.Descriptor,
) (ve catalog.ValidationErrors) {
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = errors.Newf("%v", r)
			}
			err = errors.WithAssertionFailure(errors.Wrap(err, "validation"))
			ve = append(ve, err)
		}
	}()
	return c.Validate(ctx, version, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, desc)
}

// MutableCatalog is like Catalog but mutable.
type MutableCatalog struct {
	Catalog
}

// UpsertDescriptorEntry adds a descriptor to the MutableCatalog.
func (mc *MutableCatalog) UpsertDescriptorEntry(desc catalog.Descriptor) {
	if desc == nil || desc.GetID() == descpb.InvalidID {
		return
	}
	mc.underlying.maybeInitialize()
	mc.underlying.byID.upsert(desc)
}

// DeleteDescriptorEntry removes a descriptor from the MutableCatalog.
func (mc *MutableCatalog) DeleteDescriptorEntry(id descpb.ID) {
	if id == descpb.InvalidID || !mc.IsInitialized() {
		return
	}
	mc.underlying.maybeInitialize()
	mc.underlying.byID.delete(id)
}

// UpsertNamespaceEntry adds a name -> id mapping to the MutableCatalog.
func (mc *MutableCatalog) UpsertNamespaceEntry(key catalog.NameKey, id descpb.ID) {
	if key == nil || id == descpb.InvalidID {
		return
	}
	mc.underlying.maybeInitialize()
	mc.underlying.byName.upsert(&namespaceEntry{
		NameInfo: descpb.NameInfo{
			ParentID:       key.GetParentID(),
			ParentSchemaID: key.GetParentSchemaID(),
			Name:           key.GetName(),
		},
		ID: id,
	})
}

// DeleteNamespaceEntry removes a name -> id mapping from the MutableCatalog.
func (mc *MutableCatalog) DeleteNamespaceEntry(key catalog.NameKey) {
	if key == nil || !mc.IsInitialized() {
		return
	}
	mc.underlying.maybeInitialize()
	mc.underlying.byName.delete(key)
}

// Clear empties the MutableCatalog.
func (mc *MutableCatalog) Clear() {
	mc.underlying.Clear()
}

type namespaceEntry struct {
	descpb.NameInfo
	descpb.ID
}

var _ catalog.NameEntry = namespaceEntry{}

// GetID implements the catalog.NameEntry interface.
func (e namespaceEntry) GetID() descpb.ID {
	return e.ID
}
