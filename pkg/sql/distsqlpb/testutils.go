// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlpb

import "context"

// CallbackMetadataSource is a utility struct that implements the MetadataSource
// interface by calling a provided callback.
type CallbackMetadataSource struct {
	DrainMetaCb func(context.Context) []ProducerMetadata
}

// DrainMeta is part of the MetadataSource interface.
func (s CallbackMetadataSource) DrainMeta(ctx context.Context) []ProducerMetadata {
	return s.DrainMetaCb(ctx)
}
