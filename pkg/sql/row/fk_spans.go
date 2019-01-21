// Copyright 2019 The Cockroach Authors.
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

package row

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// The functions in this file facilitate the collection of R/W spans
// for the dependency analysis of parallel statement execution
// (RETURNING NOTHING).

// FkSpanCollector can collect the spans that foreign key validation will touch.
type FkSpanCollector interface {
	CollectSpans() roachpb.Spans
	CollectSpansForValues(values tree.Datums) (roachpb.Spans, error)
}

var _ FkSpanCollector = fkExistenceCheckForInsert{}
var _ FkSpanCollector = fkExistenceCheckForDelete{}
var _ FkSpanCollector = fkExistenceCheckForUpdate{}

// collectSpansForValuesWithFKMap produce r/w access spans for all the
// given FK constraints and a given set of known datums.
func collectSpansForValuesWithFKMap(
	fks map[sqlbase.IndexID][]fkExistenceCheckBaseHelper, values tree.Datums,
) (roachpb.Spans, error) {
	var reads roachpb.Spans
	for idx := range fks {
		for _, fk := range fks[idx] {
			read, err := fk.spanForValues(values)
			if err != nil {
				return nil, err
			}
			reads = append(reads, read)
		}
	}
	return reads, nil
}

// spanForValues produce access spans for a single FK constraint and a
// tuple of columns.
func (f fkExistenceCheckBaseHelper) spanForValues(values tree.Datums) (roachpb.Span, error) {
	var key roachpb.Key
	if values != nil {
		span, _, err := sqlbase.EncodePartialIndexSpan(
			f.searchTable.TableDesc(), f.searchIdx, f.prefixLen, f.ids, values, f.searchPrefix)
		return span, err
	}
	key = roachpb.Key(f.searchPrefix)
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

// collectSpansForValuesWithFKMap produce r/w access spans for all the
// given FK constraints when the accessed values are not known.
func collectSpansWithFKMap(fks map[sqlbase.IndexID][]fkExistenceCheckBaseHelper) roachpb.Spans {
	var reads roachpb.Spans
	for idx := range fks {
		for _, fk := range fks[idx] {
			reads = append(reads, fk.span())
		}
	}
	return reads
}

// span produces a span for the entire prefix of the FK constraint.
func (f fkExistenceCheckBaseHelper) span() roachpb.Span {
	key := roachpb.Key(f.searchPrefix)
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}
}
