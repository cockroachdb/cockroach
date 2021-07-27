// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavtest

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"

type entity struct {
	onGet getInterceptor

	eav.Values
}

type getInterceptor interface {
	onGet(a eav.Attribute)
}

func (e *entity) Get(a eav.Attribute) eav.Value {
	e.onGet.onGet(a)
	return e.Values.Get(a)
}
