// Copyright 2015 The Cockroach Authors.
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
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package keys

import "github.com/cockroachdb/cockroach/pkg/roachpb"

var (
	// Meta1Span holds all first level addressing.
	Meta1Span = roachpb.Span{Key: roachpb.KeyMin, EndKey: Meta2Prefix}

	// NodeLivenessSpan holds the liveness records for nodes in the cluster.
	NodeLivenessSpan = roachpb.Span{Key: NodeLivenessPrefix, EndKey: NodeLivenessKeyMax}

	// SystemConfigSpan is the range of system objects which will be gossiped.
	SystemConfigSpan = roachpb.Span{Key: TableDataMin, EndKey: SystemConfigTableDataMax}

	// UserDataSpan is the non-meta and non-structured portion of the key space.
	UserDataSpan = roachpb.Span{Key: SystemMax, EndKey: TableDataMin}

	// NoSplitSpans describes the ranges that should never be split.
	// Meta1Span: needed to find other ranges.
	// NodeLivenessSpan: liveness information on nodes in the cluster.
	// SystemConfigSpan: system objects which will be gossiped.
	NoSplitSpans = []roachpb.Span{Meta1Span, NodeLivenessSpan, SystemConfigSpan}
)
