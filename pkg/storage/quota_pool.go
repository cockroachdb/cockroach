// Copyright 2017 The Cockroach Authors.
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

/*
 *
 * Copyright 2014, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/net/context"
)

const (
	// TODO(peter): This setting needs additional thought. Should it be adjusted
	// dynamically?
	defaultProposalQuota = 1000
)

type quotaPool struct {
	c chan int64

	mu    syncutil.Mutex
	quota int64
}

func newQuotaPool(q int64) *quotaPool {
	qp := &quotaPool{
		c: make(chan int64, 1),
	}
	qp.c <- q
	return qp
}

// add adds the specified quota back to the pool. Safe for concurrent use.
func (qp *quotaPool) add(v int64) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	select {
	case n := <-qp.c:
		qp.quota += n
	default:
	}
	qp.quota += v
	if qp.quota <= 0 {
		return
	}
	select {
	case qp.c <- qp.quota:
		qp.quota = 0
	default:
	}
}

// acquire acquires a single unit of quota from the pool. On success, nil is
// returned and the caller must call add(1) or otherwise arrange for the quota
// to be returned to the pool. Safe for concurrent use.
func (qp *quotaPool) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case n := <-qp.c:
		if n > 1 {
			qp.add(n - 1)
		}
		return nil
	}
}
