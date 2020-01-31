// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package generic

import "github.com/cockroachdb/cockroach/pkg/roachpb"

//go:generate ./gen.sh *example generic

type example struct {
	id   uint64
	span roachpb.Span
}

// Methods required by generic contract.
func (ex *example) ID() uint64         { return ex.id }
func (ex *example) Key() []byte        { return ex.span.Key }
func (ex *example) EndKey() []byte     { return ex.span.EndKey }
func (ex *example) String() string     { return ex.span.String() }
func (ex *example) New() *example      { return new(example) }
func (ex *example) SetID(v uint64)     { ex.id = v }
func (ex *example) SetKey(v []byte)    { ex.span.Key = v }
func (ex *example) SetEndKey(v []byte) { ex.span.EndKey = v }
