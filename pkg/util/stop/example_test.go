// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stop_test

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// ExampleStopper_GetHandle demonstrates the basic usage of the Handle API.
func ExampleStopper_GetHandle() {
	ctx := context.Background()

	s := stop.NewStopper()
	defer s.Stop(ctx)

	ctx, hdl, err := s.GetHandle(ctx, stop.TaskOpts{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	done := make(chan struct{})
	go func(ctx context.Context, hdl *stop.Handle) {
		defer hdl.Activate(ctx).Release(ctx)
		defer close(done)
		fmt.Println("Working...")
	}(ctx, hdl)

	<-done
	fmt.Println("Done")

	// Output:
	// Working...
	// Done
}
