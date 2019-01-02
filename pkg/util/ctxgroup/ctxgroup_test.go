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

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestErrorAfterCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "canceled", func(t *testing.T, canceled bool) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g := WithContext(ctx)
		g.Go(func() error {
			return nil
		})
		expErr := context.Canceled
		if !canceled {
			expErr = nil
		} else {
			cancel()
		}

		if err := g.Wait(); err != expErr {
			t.Errorf("expected %v, got %v", expErr, err)
		}
	})
}
