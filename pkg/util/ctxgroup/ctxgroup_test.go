// Copyright 2018 The Cockroach Authors.
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

package ctxgroup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestErrorAfterCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())

	g := WithContext(ctx)
	g.Go(func() error {
		cancel()
		return nil
	})
	if err := g.Wait(); err != context.Canceled {
		t.Errorf("expected context cancellation, got %v", err)
	}
}
