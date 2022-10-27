// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package descriptorlist contains the interface that corresponds to
// catalog.Descriptors. It's only used for sqlutil.InternalExecutor.
package descriptorlist

// CatalogDescriptorList corresponds to catalog.Descriptors.
type CatalogDescriptorList interface {
	ImplementCatalogDescriptorList()
}
