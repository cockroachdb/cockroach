// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
)

var _ scbuildstmt.DescriptorReader = buildCtx{}

// MustReadDatabase implements the scbuildstmt.DescriptorReader interface.
func (b buildCtx) MustReadDatabase(id descpb.ID) catalog.DatabaseDescriptor {
	desc := b.CatalogReader().MustReadDescriptor(b, id)
	db, err := catalog.AsDatabaseDescriptor(desc)
	if err != nil {
		panic(err)
	}
	return db
}

// MustReadTable implements the scbuildstmt.DescriptorReader interface.
func (b buildCtx) MustReadTable(id descpb.ID) catalog.TableDescriptor {
	desc := b.CatalogReader().MustReadDescriptor(b, id)
	table, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		panic(err)
	}
	return table
}

// MustReadType implements the scbuildstmt.DescriptorReader interface.
func (b buildCtx) MustReadType(id descpb.ID) catalog.TypeDescriptor {
	desc := b.CatalogReader().MustReadDescriptor(b, id)
	typ, err := catalog.AsTypeDescriptor(desc)
	if err != nil {
		panic(err)
	}
	return typ
}
