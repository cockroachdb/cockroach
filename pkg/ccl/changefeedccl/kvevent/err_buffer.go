// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
)

type errorWrapperEventBuffer struct {
	Buffer
}

// NewErrorWrapperEventBuffer returns kvevent Buffer which treats any errors
// as retryable.
func NewErrorWrapperEventBuffer(b Buffer) Buffer {
	return &errorWrapperEventBuffer{b}
}

// Add implements Writer interface.
func (e errorWrapperEventBuffer) Add(ctx context.Context, event Event) error {
	if err := e.Buffer.Add(ctx, event); err != nil {
		return changefeedbase.MarkRetryableError(err)
	}
	return nil
}

var _ Buffer = (*errorWrapperEventBuffer)(nil)
