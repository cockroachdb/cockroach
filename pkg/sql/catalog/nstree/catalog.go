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
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// Catalog is used to store an in-memory copy of the whole catalog, or a portion
// thereof, as well as metadata like comment and zone configs.
type Catalog struct {
	underlying  NameMap
	byteSize    int64
	comments    map[catalogkeys.CommentKey]string
	zoneConfigs map[descpb.ID]catalog.ZoneConfig
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

// ForEachCommentUnordered iterates through all descriptor comments in an unordered manner.
func (c Catalog) ForEachCommentUnordered(
	fn func(key catalogkeys.CommentKey, cmt string) error,
) error {
	if !c.IsInitialized() {
		return nil
	}

	for k, cmt := range c.comments {
		if err := fn(k, cmt); err != nil {
			return iterutil.Map(err)
		}
	}

	return nil
}

// ForEachZoneConfigUnordered iterates through all descriptor zone configs in an unordered manner.
func (c Catalog) ForEachZoneConfigUnordered(
	fn func(id descpb.ID, zoneConfig catalog.ZoneConfig) error,
) error {
	if !c.IsInitialized() {
		return nil
	}
	for id, z := range c.zoneConfigs {
		if err := fn(id, z); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// NamespaceEntry is a catalog.NameEntry augmented with an MVCC timestamp.
type NamespaceEntry interface {
	catalog.NameEntry
	GetMVCCTimestamp() hlc.Timestamp
}

// ForEachNamespaceEntry iterates over all namespace table entries in an ordered
// fashion.
func (c Catalog) ForEachNamespaceEntry(fn func(e NamespaceEntry) error) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.underlying.byName.ascend(func(entry catalog.NameEntry) error {
		return fn(entry.(NamespaceEntry))
	})
}

// ForEachSchemaNamespaceEntryInDatabase iterates over all namespace
// entries in an ordered fashion for the entries corresponding to
// schemas in the requested database.
func (c Catalog) ForEachSchemaNamespaceEntryInDatabase(
	dbID descpb.ID, fn func(e NamespaceEntry) error,
) error {
	if !c.IsInitialized() {
		return nil
	}
	return c.underlying.byName.ascendSchemasForDatabase(dbID, func(entry catalog.NameEntry) error {
		return fn(entry.(NamespaceEntry))
	})
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
func (c Catalog) LookupNamespaceEntry(key catalog.NameKey) NamespaceEntry {
	if !c.IsInitialized() || key == nil {
		return nil
	}
	e := c.underlying.byName.getByName(key.GetParentID(), key.GetParentSchemaID(), key.GetName())
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
	_ = c.ForEachNamespaceEntry(func(e NamespaceEntry) error {
		ret = append(ret, e.GetID())
		return nil
	})
	return ret
}

// IsInitialized returns false if the underlying map has not yet been
// initialized. Initialization is done lazily when
func (c Catalog) IsInitialized() bool {
	return c.underlying.initialized() && c.comments != nil && c.zoneConfigs != nil
}

func (c *Catalog) maybeInitialize() {
	if c.IsInitialized() {
		return
	}
	c.underlying.maybeInitialize()
	c.comments = make(map[catalogkeys.CommentKey]string)
	c.zoneConfigs = make(map[descpb.ID]catalog.ZoneConfig)
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
	ne := c.LookupNamespaceEntry(key)
	if ne == nil {
		return errors.New("invalid descriptor ID")
	}
	// Handle special cases.
	switch ne.GetID() {
	case descpb.InvalidID:
		return errors.New("invalid descriptor ID")
	case keys.PublicSchemaID:
		// The public schema doesn't have a descriptor.
		return nil
	default:
		isSchema := ne.GetParentID() != keys.RootNamespaceID && ne.GetParentSchemaID() == keys.RootNamespaceID
		if isSchema && strings.HasPrefix(ne.GetName(), "pg_temp_") {
			// Temporary schemas have namespace entries but not descriptors.
			return nil
		}
	}
	// Compare the namespace entry with the referenced descriptor.
	desc := c.LookupDescriptorEntry(ne.GetID())
	if desc == nil {
		return catalog.ErrDescriptorNotFound
	}
	if desc.Dropped() {
		return errors.Newf("no matching name info in draining names of dropped %s",
			desc.DescriptorType())
	}
	if ne.GetParentID() == desc.GetParentID() &&
		ne.GetParentSchemaID() == desc.GetParentSchemaID() &&
		ne.GetName() == desc.GetName() {
		return nil
	}
	return errors.Newf("no matching name info found in non-dropped %s %q",
		desc.DescriptorType(), desc.GetName())
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

// MutableCatalog is like Catalog but mutable.
type MutableCatalog struct {
	Catalog
}

// UpsertDescriptorEntry adds a descriptor to the MutableCatalog.
func (mc *MutableCatalog) UpsertDescriptorEntry(desc catalog.Descriptor) {
	if desc == nil || desc.GetID() == descpb.InvalidID {
		return
	}
	mc.maybeInitialize()
	if replaced := mc.underlying.byID.upsert(desc); replaced != nil {
		mc.byteSize -= replaced.(catalog.Descriptor).ByteSize()
	}
	mc.byteSize += desc.ByteSize()
}

// DeleteDescriptorEntry removes a descriptor from the MutableCatalog.
func (mc *MutableCatalog) DeleteDescriptorEntry(id descpb.ID) {
	if id == descpb.InvalidID || !mc.IsInitialized() {
		return
	}
	if removed := mc.underlying.byID.delete(id); removed != nil {
		mc.byteSize -= removed.(catalog.Descriptor).ByteSize()
	}
}

// UpsertNamespaceEntry adds a name -> id mapping to the MutableCatalog.
func (mc *MutableCatalog) UpsertNamespaceEntry(
	key catalog.NameKey, id descpb.ID, mvccTimestamp hlc.Timestamp,
) {
	if key == nil || id == descpb.InvalidID {
		return
	}
	mc.maybeInitialize()
	nsEntry := &namespaceEntry{
		NameInfo: descpb.NameInfo{
			ParentID:       key.GetParentID(),
			ParentSchemaID: key.GetParentSchemaID(),
			Name:           key.GetName(),
		},
		ID:        id,
		Timestamp: mvccTimestamp,
	}
	if replaced := mc.underlying.byName.upsert(nsEntry); replaced != nil {
		mc.byteSize -= replaced.(*namespaceEntry).ByteSize()
	}
	mc.byteSize += nsEntry.ByteSize()
}

// DeleteNamespaceEntry removes a name -> id mapping from the MutableCatalog.
func (mc *MutableCatalog) DeleteNamespaceEntry(key catalog.NameKey) {
	if key == nil || !mc.IsInitialized() {
		return
	}
	if removed := mc.underlying.byName.delete(key); removed != nil {
		mc.byteSize -= removed.(*namespaceEntry).ByteSize()
	}
}

// UpsertComment upserts a ((ObjectID, SubID, CommentType) -> Comment) mapping
// into the catalog.
func (mc *MutableCatalog) UpsertComment(key catalogkeys.CommentKey, cmt string) {
	mc.maybeInitialize()
	mc.byteSize -= int64(len(mc.comments[key]))
	mc.comments[key] = cmt
	mc.byteSize += int64(len(cmt))
}

// UpsertZoneConfig upserts a (descriptor id -> zone config) mapping into the
// catalog.
func (mc *MutableCatalog) UpsertZoneConfig(
	id descpb.ID, zoneConfig *zonepb.ZoneConfig, rawBytes []byte,
) {
	mc.maybeInitialize()
	if mc.zoneConfigs == nil {
		mc.zoneConfigs = make(map[descpb.ID]catalog.ZoneConfig)
	}
	if existing, ok := mc.zoneConfigs[id]; ok {
		mc.byteSize -= int64(existing.Size())
	}
	zc := zone.NewZoneConfigWithRawBytes(zoneConfig, rawBytes)
	mc.zoneConfigs[id] = zc
	mc.byteSize += int64(zc.Size())
}

// Clear empties the MutableCatalog.
func (mc *MutableCatalog) Clear() {
	mc.underlying.Clear()
}

type namespaceEntry struct {
	descpb.NameInfo
	descpb.ID
	hlc.Timestamp
}

var _ NamespaceEntry = namespaceEntry{}

// GetID implements the catalog.NameEntry interface.
func (e namespaceEntry) GetID() descpb.ID {
	return e.ID
}

// ByteSize returns the number of bytes a namespaceEntry object takes.
func (e namespaceEntry) ByteSize() int64 {
	return int64(e.NameInfo.Size()) + int64(unsafe.Sizeof(e.ID))
}

func (e namespaceEntry) GetMVCCTimestamp() hlc.Timestamp {
	return e.Timestamp
}
