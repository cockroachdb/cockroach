// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber

import (
	"context"
)

// TestingSubscribeInner exports the inner subscription route for testing
// purposes.
func (s *KVSubscriber) TestingSubscribeInner(ctx context.Context) error {
	return s.subscribeInner(ctx)
}
