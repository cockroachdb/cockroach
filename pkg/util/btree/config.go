// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package btree

import (
	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
	"golang.org/x/exp/constraints"
)

// Config configures the btree.
type Config[K any] interface {
	ordered.Comparator[K]
}

// WithBytes is a config for bytes slice types.
// It can be used without initialization.
type WithBytes[K ordered.Bytes] struct{ ordered.BytesComparator[K] }

// WithKey is a config for types which implement the Key interface.
// It can be used without initialization.
type WithKey[K ordered.Key[K]] struct{ ordered.KeyComparator[K] }

// WithOrdered is a config for types which are ordered.
// It can be used without initialization.
type WithOrdered[K constraints.Ordered] struct{ ordered.ValueComparator[K] }

// WithFunc is a config for providing a custom function comparator.
// It cannot be used without initialization
type WithFunc[K any] struct{ ordered.Func[K] }

// Compare implements Comparator[K].
func (c WithFunc[K]) Compare(a, b K) int { return c.Func(a, b) }
