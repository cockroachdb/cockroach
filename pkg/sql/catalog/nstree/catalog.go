// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/errors"
)

// Catalog is used to store an in-memory copy of the whole catalog, or a portion
// thereof, as well as metadata like comment and zone configs.
type Catalog struct {
	byID     byIDMap
	byName   byNameMap
	byteSize int64
}

// CommentCatalog is a limited interface wrapper, which is used for partial
// immutable catalogs that are incomeplete and only contain comment information.
type CommentCatalog interface {
	ForEachComment(fn func(key catalogkeys.CommentKey, cmt string) error) error
	ForEachCommentOnDescriptor(
		id descpb.ID, fn func(key catalogkeys.CommentKey, cmt string) error) error
	LookupComment(key catalogkeys.CommentKey) (_ string, found bool)
}

// Sanity: Catalog implements a comment catalog.
var _ CommentCatalog = Catalog{}

// ForEachDescriptor iterates over all descriptor table entries in an
// ordered fashion.
func (c Catalog) ForEachDescriptor(fn func(desc catalog.Descriptor) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.byID.ascend(func(entry catalog.NameEntry) error {
		if d := entry.(*byIDEntry).desc; d != nil {
			return fn(d)
		}
		return nil
	})
}

// ForEachComment iterates through all descriptor comments in the same
// order as in system.comments.
func (c Catalog) ForEachComment(fn func(key catalogkeys.CommentKey, cmt string) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.byID.ascend(func(entry catalog.NameEntry) error {
		return entry.(*byIDEntry).forEachComment(fn)
	})
}

// ForEachCommentOnDescriptor iterates through all comments on a specific
// descriptor in the same order as in system.comments.
func (c Catalog) ForEachCommentOnDescriptor(
	id descpb.ID, fn func(key catalogkeys.CommentKey, cmt string) error,
) error {
	if !c.IsInitialized() {
		return nil
	}
	e := c.byID.get(id)
	if e == nil {
		return nil
	}
	return e.(*byIDEntry).forEachComment(fn)
}

// ForEachZoneConfig iterates over all zone config table entries in an
// ordered fashion.
func (c Catalog) ForEachZoneConfig(fn func(id descpb.ID, zc catalog.ZoneConfig) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.byID.ascend(func(entry catalog.NameEntry) error {
		if zc := entry.(*byIDEntry).zc; zc != nil {
			return fn(entry.GetID(), zc)
		}
		return nil
	})
}

// ForEachNamespaceEntry iterates over all name -> ID mappings in the same
// order as in system.namespace.
func (c Catalog) ForEachNamespaceEntry(fn func(e NamespaceEntry) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.byName.ascend(func(entry catalog.NameEntry) error {
		return fn(entry.(NamespaceEntry))
	})
}

// ForEachDatabaseNamespaceEntry iterates over all database name -> ID mappings
// in the same order as in system.namespace.
func (c Catalog) ForEachDatabaseNamespaceEntry(fn func(e NamespaceEntry) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.byName.ascendDatabases(func(entry catalog.NameEntry) error {
		return fn(entry.(NamespaceEntry))
	})
}

// ForEachSchemaNamespaceEntryInDatabase iterates over all schema name -> ID
// mappings in the same order as in system.namespace for the mappings
// corresponding to schemas in the requested database.
func (c Catalog) ForEachSchemaNamespaceEntryInDatabase(
	dbID descpb.ID, fn func(e NamespaceEntry) error,
) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.byName.ascendSchemasForDatabase(dbID, func(entry catalog.NameEntry) error {
		return fn(entry.(NamespaceEntry))
	})
}

// LookupDescriptor looks up a descriptor by ID.
func (c Catalog) LookupDescriptor(id descpb.ID) catalog.Descriptor {
	if !c.IsInitialized() || id == descpb.InvalidID {
		return nil
	}
	e := c.byID.get(id)
	if e == nil {
		return nil
	}
	return e.(*byIDEntry).desc
}

// LookupComment looks up a comment by (CommentType, ID, SubID).
func (c Catalog) LookupComment(key catalogkeys.CommentKey) (_ string, found bool) {
	if !c.IsInitialized() {
		return "", false
	}
	e := c.byID.get(descpb.ID(key.ObjectID))
	if e == nil {
		return "", false
	}
	cbt := &e.(*byIDEntry).comments[key.CommentType]
	ordinal, ok := cbt.subObjectOrdinals.Get(int(key.SubID))
	if !ok {
		return "", false
	}
	return cbt.comments[ordinal], true
}

// LookupZoneConfig looks up a zone config by ID.
func (c Catalog) LookupZoneConfig(id descpb.ID) catalog.ZoneConfig {
	if !c.IsInitialized() {
		return nil
	}
	e := c.byID.get(id)
	if e == nil {
		return nil
	}
	return e.(*byIDEntry).zc
}

// LookupNamespaceEntry looks up a descriptor ID by name.
func (c Catalog) LookupNamespaceEntry(key descpb.NameInfo) NamespaceEntry {
	if !c.IsInitialized() {
		return nil
	}
	e := c.byName.getByName(key.GetParentID(), key.GetParentSchemaID(), key.GetName())
	if e == nil {
		return nil
	}
	return e.(NamespaceEntry)
}

// OrderedDescriptors returns the descriptors in an ordered fashion.
func (c Catalog) OrderedDescriptors() []catalog.Descriptor {
	if !c.IsInitialized() {
		return nil
	}
	ret := make([]catalog.Descriptor, 0, c.byID.t.Len())
	_ = c.ForEachDescriptor(func(desc catalog.Descriptor) error {
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
	ret := make([]descpb.ID, 0, c.byName.t.Len())
	_ = c.ForEachNamespaceEntry(func(e NamespaceEntry) error {
		ret = append(ret, e.GetID())
		return nil
	})
	return ret
}

// IsInitialized returns false if the underlying map has not yet been
// initialized. Initialization is done lazily when
func (c Catalog) IsInitialized() bool {
	return c.byID.initialized() && c.byName.initialized()
}

var _ validate.ValidationDereferencer = Catalog{}

// DereferenceDescriptors implements the validate.ValidationDereferencer
// interface.
func (c Catalog) DereferenceDescriptors(
	ctx context.Context, version clusterversion.ClusterVersion, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	ret := make([]catalog.Descriptor, len(reqs))
	for i, id := range reqs {
		ret[i] = c.LookupDescriptor(id)
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
		ne := c.LookupNamespaceEntry(req)
		if ne == nil {
			continue
		}
		ret[i] = ne.GetID()
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

// ValidateNamespaceEntry returns an error if the specified namespace entry
// is invalid.
func (c Catalog) ValidateNamespaceEntry(key catalog.NameKey) error {
	ne := c.LookupNamespaceEntry(catalog.MakeNameInfo(key))
	if ne == nil {
		return errors.AssertionFailedf("invalid namespace entry")
	}
	// Handle special cases.
	switch ne.GetID() {
	case descpb.InvalidID:
		return errors.New("invalid descriptor ID")
	case keys.SystemPublicSchemaID:
		// The public schema for the system database doesn't have a descriptor.
		return nil
	default:
		isSchema := ne.GetParentID() != keys.RootNamespaceID && ne.GetParentSchemaID() == keys.RootNamespaceID
		if isSchema && strings.HasPrefix(ne.GetName(), "pg_temp_") {
			// Temporary schemas have namespace entries but not descriptors.
			return nil
		}
	}
	// Compare the namespace entry with the referenced descriptor.
	desc := c.LookupDescriptor(ne.GetID())
	if desc == nil {
		return catalog.NewReferencedDescriptorNotFoundError("schema", ne.GetID())
	}
	if desc.Dropped() {
		return catalog.ErrDescriptorDropped
	}
	if ne.GetParentID() == desc.GetParentID() &&
		ne.GetParentSchemaID() == desc.GetParentSchemaID() &&
		ne.GetName() == desc.GetName() {
		return nil
	}
	return errors.Newf("mismatched name %q in %s descriptor", desc.GetName(), desc.DescriptorType())
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
	return c.Validate(ctx, version, catalog.NoValidationTelemetry, validate.Write, desc)
}

// ByteSize returns memory usage of the underlying map in bytes.
func (c Catalog) ByteSize() int64 {
	return c.byteSize
}

// FilterByIDs returns a subset of the catalog only for the desired IDs.
func (c Catalog) FilterByIDs(ids []descpb.ID) Catalog {
	var ret MutableCatalog
	ret.addByIDEntries(c, ids)
	if !ret.IsInitialized() {
		return Catalog{}
	}
	_ = c.byName.ascend(func(found catalog.NameEntry) error {
		if ret.byID.get(found.GetID()) == nil {
			return nil
		}
		e := ret.ensureForName(found)
		*e = *found.(*byNameEntry)
		return nil
	})
	return ret.Catalog
}

// FilterByIDsExclusive is like FilterByIDs but without any by-name entries.
func (c Catalog) FilterByIDsExclusive(ids []descpb.ID) Catalog {
	var ret MutableCatalog
	ret.addByIDEntries(c, ids)
	return ret.Catalog
}

func (mc *MutableCatalog) addByIDEntries(c Catalog, ids []descpb.ID) {
	if !c.IsInitialized() {
		return
	}
	for _, id := range ids {
		found := c.byID.get(id)
		if found == nil {
			continue
		}
		e := mc.ensureForID(id)
		*e = *found.(*byIDEntry)
	}
}

// FilterByNames returns a subset of the catalog only for the desired names.
func (c Catalog) FilterByNames(nameInfos []descpb.NameInfo) Catalog {
	if !c.IsInitialized() {
		return Catalog{}
	}
	var ret MutableCatalog
	for i := range nameInfos {
		ni := &nameInfos[i]
		found := c.byName.getByName(ni.ParentID, ni.ParentSchemaID, ni.Name)
		if found == nil {
			continue
		}
		e := ret.ensureForName(ni)
		*e = *found.(*byNameEntry)
		if foundByID := c.byID.get(e.id); foundByID != nil {
			e := ret.ensureForID(e.id)
			*e = *foundByID.(*byIDEntry)
		}
	}
	return ret.Catalog
}
