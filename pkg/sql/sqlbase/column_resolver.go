// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// sourceNameMatches checks whether a request for table name toFind
// can be satisfied by the FROM source name srcName.
//
// For example:
// - a request for "kv" is matched by a source named "db1.public.kv"
// - a request for "public.kv" is not matched by a source named just "kv"
func sourceNameMatches(srcName *tree.TableName, toFind tree.TableName) bool {
	if srcName.TableName != toFind.TableName {
		return false
	}
	if toFind.ExplicitSchema {
		if !srcName.ExplicitSchema || srcName.SchemaName != toFind.SchemaName {
			return false
		}
		if toFind.ExplicitCatalog {
			if !srcName.ExplicitCatalog || srcName.CatalogName != toFind.CatalogName {
				return false
			}
		}
	}
	return true
}

// ColumnResolver is a utility struct to be used when resolving column
// names to point to one of the data sources and one of the column IDs
// in that data source.
type ColumnResolver struct {
	Sources MultiSourceInfo

	// resolverState is modified in-place by the implementation of the
	// tree.ColumnItemResolver interface in resolver.go.
	ResolverState struct {
		ForUpdateOrDelete bool
		SrcIdx            int
		ColIdx            int
		ColSetIdx         int
	}
}

// FindSourceMatchingName is part of the tree.ColumnItemResolver interface.
func (r *ColumnResolver) FindSourceMatchingName(
	ctx context.Context, tn tree.TableName,
) (
	res tree.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	err error,
) {
	// log.VEventf(ctx, 2, "FindSourceMatchingName(%s) w/\n%s", tn, r.Sources.String())
	// defer func() {
	// 	log.VEventf(ctx, 2, "FindSourceMachingName(%s) -> %v %v %v %v",
	// 		&tn, res, prefix, srcMeta, err)
	// }()
	found := false
	for srcIdx, src := range r.Sources {
		for colSetIdx, alias := range src.SourceAliases {
			if !sourceNameMatches(&alias.Name, tn) {
				continue
			}
			if found {
				return tree.MoreThanOne, nil, nil, newAmbiguousSourceError(&tn)
			}
			found = true
			prefix = &alias.Name
			r.ResolverState.SrcIdx = srcIdx
			r.ResolverState.ColSetIdx = colSetIdx
		}
	}
	if !found {
		return tree.NoResults, nil, nil, nil
	}
	return tree.ExactlyOne, prefix, nil, nil
}

const invalidColIdx = -1
const invalidSrcIdx = -1

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (r *ColumnResolver) FindSourceProvidingColumn(
	ctx context.Context, col tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	// log.VEventf(ctx, 2, "FindSourceProvidingColumn(%s) w/\n%s", col, r.Sources.String())
	// defer func() {
	// 	log.VEventf(ctx, 2, "FindSourceProvidingColumn(%s) -> %q %v %v %v",
	// 		col, prefix, srcMeta, colHint, err)
	// }()
	colIdx := invalidColIdx
	srcIdx := 0
	colName := string(col)

	// First search all the anonymous sources.
	for iSrc, src := range r.Sources {
		for colSetIdx := range src.SourceAliases {
			if src.SourceAliases[colSetIdx].Name.TableName != "" {
				continue
			}
			colSet := src.SourceAliases[colSetIdx].ColumnSet
			for idx, ok := colSet.Next(0); ok; idx, ok = colSet.Next(idx + 1) {
				srcIdx, colIdx, err = r.findColHelper(src, colName, iSrc, srcIdx, colIdx, idx)
				if err != nil {
					return nil, nil, -1, err
				}
				if colIdx != invalidColIdx {
					prefix = &src.SourceAliases[colSetIdx].Name
				}
			}
		}
	}
	if colIdx == invalidColIdx {
		// Try harder: unqualified column names can look at all
		// columns, not just columns of the anonymous table.
		for iSrc, src := range r.Sources {
			for colSetIdx := range src.SourceAliases {
				colSet := src.SourceAliases[colSetIdx].ColumnSet
				for idx, ok := colSet.Next(0); ok; idx, ok = colSet.Next(idx + 1) {
					srcIdx, colIdx, err = r.findColHelper(src, colName, iSrc, srcIdx, colIdx, idx)
					if err != nil {
						return nil, nil, -1, err
					}
					if colIdx != invalidColIdx {
						prefix = &src.SourceAliases[colSetIdx].Name
					}
				}
			}
		}
	}
	if colIdx == invalidColIdx {
		return nil, nil, -1,
			pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"column name %q not found", tree.ErrString(&col))
	}
	r.ResolverState.SrcIdx = srcIdx
	r.ResolverState.ColIdx = colIdx
	return prefix, nil, colIdx, nil
}

// Resolve is part of the tree.ColumnItemResolver interface.
func (r *ColumnResolver) Resolve(
	ctx context.Context,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	colHint int,
	col tree.Name,
) (tree.ColumnResolutionResult, error) {
	if colHint != -1 {
		// (*ColumnItem).Resolve() is telling us that we found the source
		// via FindSourceProvidingColumn(). So we can count on
		// r.ResolverState.SrcIdx and r.ResolverState.ColIdx being set
		// already. There's nothing remaining to do!
		return nil, nil
	}

	// If we're here, we just know that some source alias was found that
	// matches the column prefix, but we haven't found the column
	// yet. Do this now.
	// FindSourceMatchingName() was careful to set r.ResolverState.SrcIdx
	// and r.ResolverState.ColSetIdx for us.
	iSrc := r.ResolverState.SrcIdx
	src := r.Sources[iSrc]
	colSetIdx := r.ResolverState.ColSetIdx
	colSet := src.SourceAliases[colSetIdx].ColumnSet
	srcIdx := 0
	colIdx := invalidColIdx
	colName := string(col)
	for idx, ok := colSet.Next(0); ok; idx, ok = colSet.Next(idx + 1) {
		var err error
		srcIdx, colIdx, err = r.findColHelper(src, colName, iSrc, srcIdx, colIdx, idx)
		if err != nil {
			return nil, err
		}
	}

	if colIdx == invalidColIdx {
		r.ResolverState.SrcIdx = invalidSrcIdx
		r.ResolverState.ColIdx = invalidColIdx
		return nil, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
			"column name %q not found",
			tree.ErrString(tree.NewColumnItem(&src.SourceAliases[colSetIdx].Name, tree.Name(colName))))
	}
	r.ResolverState.SrcIdx = srcIdx
	r.ResolverState.ColIdx = colIdx
	return nil, nil
}

// findColHelper is used by FindSourceProvidingColumn and Resolve above.
// It checks whether a column name is available in a given data source.
func (r *ColumnResolver) findColHelper(
	src *DataSourceInfo, colName string, iSrc, srcIdx, colIdx, idx int,
) (int, int, error) {
	col := src.SourceColumns[idx]
	if col.Name == colName {
		// Do not return a match if:
		// 1. The column is being backfilled and therefore should not be
		// used to resolve a column expression, and,
		// 2. The column expression being resolved is not from a selector
		// column expression from an UPDATE/DELETE.
		if backfillThreshold := len(src.SourceColumns) - src.NumBackfillColumns; idx >= backfillThreshold && !r.ResolverState.ForUpdateOrDelete {
			return invalidSrcIdx, invalidColIdx,
				pgerror.NewErrorf(pgerror.CodeInvalidColumnReferenceError,
					"column %q is being backfilled", tree.ErrString(src.NodeFormatter(idx)))
		}
		if colIdx != invalidColIdx {
			colString := tree.ErrString(src.NodeFormatter(idx))
			var msgBuf bytes.Buffer
			sep := ""
			fmtCandidate := func(alias *SourceAlias) {
				name := tree.ErrString(&alias.Name.TableName)
				if len(name) == 0 {
					name = "<anonymous>"
				}
				fmt.Fprintf(&msgBuf, "%s%s.%s", sep, name, colString)
			}
			for i := range src.SourceAliases {
				fmtCandidate(&src.SourceAliases[i])
				sep = ", "
			}
			if iSrc != srcIdx {
				for i := range r.Sources[srcIdx].SourceAliases {
					fmtCandidate(&r.Sources[srcIdx].SourceAliases[i])
					sep = ", "
				}
			}
			return invalidSrcIdx, invalidColIdx, pgerror.NewErrorf(pgerror.CodeAmbiguousColumnError,
				"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String())
		}
		srcIdx = iSrc
		colIdx = idx
	}
	return srcIdx, colIdx, nil
}

func newAmbiguousSourceError(tn *tree.TableName) error {
	if tn.Catalog() == "" {
		return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
			"ambiguous source name: %q", tree.ErrString(tn))

	}
	return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
		"ambiguous source name: %q (within database %q)",
		tree.ErrString(&tn.TableName), tree.ErrString(&tn.CatalogName))
}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (*TableDescriptor) NameResolutionResult() {}

// SchemaMeta implements the tree.SchemaMeta interface.
func (*DatabaseDescriptor) SchemaMeta() {}

// SchemaMeta implements the tree.SchemaMeta interface.
func (Descriptor) SchemaMeta() {}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (Descriptor) NameResolutionResult() {}
