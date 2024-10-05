// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type virtualDescriptors struct {
	vs catalog.VirtualSchemas
}

func makeVirtualDescriptors(schemas catalog.VirtualSchemas) virtualDescriptors {
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
	_ = tc.vs.Visit(func(vd catalog.Descriptor, comment string) error {
		mc.UpsertDescriptor(vd)
		if vd.GetID() != keys.PublicSchemaID && !vd.Dropped() && !vd.SkipNamespace() {
			mc.UpsertNamespaceEntry(vd, vd.GetID(), hlc.Timestamp{})
		}
		if comment == "" {
			return nil
		}
		ck := catalogkeys.CommentKey{ObjectID: uint32(vd.GetID())}
		switch vd.DescriptorType() {
		case catalog.Database:
			ck.CommentType = catalogkeys.DatabaseCommentType
		case catalog.Schema:
			ck.CommentType = catalogkeys.SchemaCommentType
		case catalog.Table:
			ck.CommentType = catalogkeys.TableCommentType
		default:
			return nil
		}
		return mc.UpsertComment(ck, comment)
	})
}
