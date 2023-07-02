// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package obsutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
)

// StdOutConsumer implements the EventConsumer interface and logs
// each event it receives to STDOUT, for testing purposes.
//
// StdOutConsumer is not intended for real-world use.
type StdOutConsumer struct{}

func (s StdOutConsumer) Consume(ctx context.Context, event *obspb.Event) error {
	fmt.Printf("StdOutConsumer - consumed event: %v\n", event)
	return nil
}

var _ obslib.EventConsumer = (*StdOutConsumer)(nil)
