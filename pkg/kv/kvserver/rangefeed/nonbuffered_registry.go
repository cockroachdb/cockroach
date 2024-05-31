// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import "github.com/cockroachdb/cockroach/pkg/util/interval"

type nonBufferedRegistry struct {
	metrics *Metrics
	tree    interval.Tree // *nonBufferedRegistration items
	idAlloc int64
}

// TODO(wenyihu6): check on alloc memory accounting again
