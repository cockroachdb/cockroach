// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import "github.com/cockroachdb/cockroach/pkg/util/log/logpb"

type logFormatter interface {
	// formatEntry formats a logpb.Entry into a newly allocated *buffer.
	// The caller is responsible for calling putBuffer() afterwards.
	formatEntry(entry logpb.Entry, stacks []byte) *buffer
}

var _ logFormatter = formatCrdbV1{}
var _ logFormatter = formatCrdbV1WithCounter{}
var _ logFormatter = formatCrdbV1TTY{}
var _ logFormatter = formatCrdbV1TTYWithCounter{}
