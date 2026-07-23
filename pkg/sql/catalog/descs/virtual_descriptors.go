// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// VirtualCatalogHolder holds a catalog of all virtual descriptors.
type VirtualCatalogHolder interface {
	catalog.VirtualSchemas
	// GetCatalog returns a catalog of vritual descriptors.
	GetCatalog() nstree.Catalog
}

type virtualDescriptors struct {
	vs VirtualCatalogHolder
}

func makeVirtualDescriptors(schemas VirtualCatalogHolder) virtualDescriptors {
	return virtualDescriptors{vs: schemas}
}

func (tc virtualDescriptors) getSchemaByName(schemaName string) catalog.SchemaDescriptor {
	if tc.vs == nil {
		return nil
	}
	if sc, ok := tc.vs.GetVirtualSchema(schemaName); ok {
		return sc.Desc()
	}
	return nil
}

func (tc virtualDescriptors) getObjectByName(
	schema string, object string, requestedKind tree.DesiredObjectKind,
) (virtualSchema catalog.VirtualSchema, virtualObject catalog.VirtualObject, _ error) {
	if tc.vs == nil {
		return nil, nil, nil
	}
	var found bool
	virtualSchema, found = tc.vs.GetVirtualSchema(schema)
	if !found {
		return nil, nil, nil
	}
	obj, err := virtualSchema.GetObjectByName(object, requestedKind)
	return virtualSchema, obj, err
}

func (tc virtualDescriptors) getObjectByID(
	id descpb.ID,
) (catalog.VirtualSchema, catalog.VirtualObject) {
	if tc.vs == nil {
		return nil, nil
	}
	vd, found := tc.vs.GetVirtualObjectByID(id)
	if !found {
		return nil, nil
	}
	vs, found := tc.vs.GetVirtualSchemaByID(vd.Desc().GetParentSchemaID())
	if !found {
		return nil, vd
	}
	return vs, vd
}

func (tc virtualDescriptors) getSchemaByID(id descpb.ID) catalog.VirtualSchema {
	if tc.vs == nil {
		return nil
	}
	vs, found := tc.vs.GetVirtualSchemaByID(id)
	if !found {
		return nil
	}
	return vs
}

func (tc virtualDescriptors) addAllToCatalog(mc nstree.MutableCatalog) {
	mc.AddAll(tc.vs.GetCatalog())
}
