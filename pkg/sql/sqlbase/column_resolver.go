// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ProcessTargetColumns returns the column descriptors identified by the
// given name list. It also checks that a given column name is only
// listed once. If no column names are given (special case for INSERT)
// and ensureColumns is set, the descriptors for all visible columns
// are returned. If allowMutations is set, even columns undergoing
// mutations are added.
func ProcessTargetColumns(
	tableDesc *ImmutableTableDescriptor, nameList tree.NameList, ensureColumns, allowMutations bool,
) ([]ColumnDescriptor, error) {
	if len(nameList) == 0 {
		if ensureColumns {
			// VisibleColumns is used here to prevent INSERT INTO <table> VALUES (...)
			// (as opposed to INSERT INTO <table> (...) VALUES (...)) from writing
			// hidden columns. At present, the only hidden column is the implicit rowid
			// primary key column.
			return tableDesc.VisibleColumns(), nil
		}
		return nil, nil
	}

	cols := make([]ColumnDescriptor, len(nameList))
	colIDSet := make(map[ColumnID]struct{}, len(nameList))
	for i, colName := range nameList {
		var col *ColumnDescriptor
		var err error
		if allowMutations {
			col, _, err = tableDesc.FindColumnByName(colName)
		} else {
			col, err = tableDesc.FindActiveColumnByName(string(colName))
		}
		if err != nil {
			return nil, err
		}

		if _, ok := colIDSet[col.ID]; ok {
			return nil, pgerror.Newf(pgcode.Syntax,
				"multiple assignments to the same column %q", &nameList[i])
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = *col
	}

	return cols, nil
}

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
	found := false
	for srcIdx, src := range r.Sources {
		for colSetIdx := range src.SourceAliases {
			alias := &src.SourceAliases[colSetIdx]
			if !sourceNameMatches(&alias.Name, tn) {
				continue
			}
			if found {
				tnAlloc := tn
				return tree.MoreThanOne, nil, nil, newAmbiguousSourceError(&tnAlloc)
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
		colAlloc := col
		return nil, nil, -1, NewUndefinedColumnError(tree.ErrString(&colAlloc))
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
		return nil, NewUndefinedColumnError(
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
				pgerror.Newf(pgcode.InvalidColumnReference,
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
			return invalidSrcIdx, invalidColIdx, pgerror.Newf(pgcode.AmbiguousColumn,
				"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String())
		}
		srcIdx = iSrc
		colIdx = idx
	}
	return srcIdx, colIdx, nil
}

func newAmbiguousSourceError(tn *tree.TableName) error {
	if tn.Catalog() == "" {
		return pgerror.Newf(pgcode.AmbiguousAlias,
			"ambiguous source name: %q", tree.ErrString(tn))

	}
	return pgerror.Newf(pgcode.AmbiguousAlias,
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
