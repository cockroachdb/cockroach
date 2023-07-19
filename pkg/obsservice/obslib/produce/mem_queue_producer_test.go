// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package produce

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/queue"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/stretchr/testify/require"
)

func TestMemQueueProducer_Produce(t *testing.T) {
	t.Run("panics when fed event of different type", func(t *testing.T) {
		q := queue.NewMemoryQueue[*obspb.Event](1000, "test")
		p := NewMemQueueProducer[*obspb.Event](q)
		require.Panicsf(t, func() {
			_ = p.Produce("invalid type")
		}, "expected panic when producing invalid type")
	})
}
