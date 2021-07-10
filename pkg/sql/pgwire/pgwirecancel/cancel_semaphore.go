// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwirecancel

import "github.com/cockroachdb/cockroach/pkg/util/quotapool"

// CancelSemaphore is a semaphore that limits the number of concurrent
// calls to the pgwire query cancellation endpoint. This is needed to avoid the
// risk of a DoS attack by malicious users that attempts to cancel random
// queries by spamming the request.
//
// We hard-code a limit of 256 concurrent pgwire cancel requests (per node).
// We also add a 1-second penalty for failed cancellation requests, meaning
// that an attacker needs 1 second per guess. With an attacker randomly
// guessing a 32-bit secret, it would take 2^24 seconds to hit one query. If
// we suppose there are 256 concurrent queries actively running on a node,
// then it would take 2^16 seconds (18 hours) to hit any one of them.
var CancelSemaphore = quotapool.NewIntPool("pgwire-cancel", 256)
