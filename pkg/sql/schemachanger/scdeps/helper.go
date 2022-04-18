// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// FindTableContainsIndex is a helper to find a table which contains a target
// index. dsNames and dsIDs are parallel slices containing table names and ids,
// they must have same length.
func FindTableContainsIndex(
	ctx context.Context,
	catalogReader scbuild.CatalogReader,
	indexName tree.Name,
	dsNames tree.TableNames,
	dsIDs descpb.IDs,
	requireTable bool,
) (catalog.TableDescriptor, catalog.Index) {
	tableFound := -1
	var tableDesc catalog.TableDescriptor
	var idxDesc catalog.Index
	for i, dsID := range dsIDs {
		desc := catalogReader.MustReadDescriptor(ctx, dsID)
		if tblDesc, ok := desc.(catalog.TableDescriptor); ok {
			if !tblDesc.IsTable() {
				continue
			}
			for _, index := range tblDesc.NonDropIndexes() {
				if index.GetName() == string(indexName) {
					if tableFound != -1 {
						panic(pgerror.Newf(pgcode.AmbiguousParameter, "index name %q is ambiguous (found in %s and %s)",
							indexName, dsNames[tableFound].String(), dsNames[i].String()))
					}
					tableFound = i
					idxDesc = index
					tableDesc = tblDesc
				}
			}
		}
	}
	if tableFound == -1 && requireTable {
		panic(pgerror.Newf(pgcode.UndefinedObject, "index %q does not exist", indexName))
	}
	return tableDesc, idxDesc
}
