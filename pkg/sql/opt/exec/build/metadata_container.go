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

package build

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// metadataContainer implements the tree.IndexedVarContainer interface over
// opt.Metadata, which contains per-query metadata.
type metadataContainer opt.Metadata

var _ tree.IndexedVarContainer = &metadataContainer{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (m *metadataContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("no eval allowed in metadataContainer")
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (m *metadataContainer) IndexedVarResolvedType(idx int) types.T {
	// Adjust index to make it 1-based.
	return (*opt.Metadata)(m).ColumnType(opt.ColumnIndex(idx + 1))
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (m *metadataContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}
