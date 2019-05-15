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
