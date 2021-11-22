// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svpreemptor

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
)

type rowSet map[svstorage.Row]struct{}

func (s rowSet) toIDSet() (ids catalog.DescriptorIDSet) {
	for r := range s {
		ids.Add(r.Descriptor)
	}
	return ids
}

func (s rowSet) reset() {
	for k := range s {
		delete(s, k)
	}
}

func (s rowSet) add(r svstorage.Row) {
	s[r] = struct{}{}
}
