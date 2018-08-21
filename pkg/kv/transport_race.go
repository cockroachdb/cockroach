// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

// +build race

package kv

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var running int32 // atomically updated
var incoming chan *roachpb.BatchRequest

func init() {
	incoming = make(chan *roachpb.BatchRequest, 100)
}

const defaultRaceInterval = 150 * time.Microsecond

func jitter(avgInterval time.Duration) time.Duration {
	// Use defaultRaceInterval as a minimum to limit how much time
	// we spend here.
	if avgInterval < defaultRaceInterval {
		avgInterval = defaultRaceInterval
	}
	return time.Duration(rand.Int63n(int64(2 * avgInterval)))
}

// raceTransport wrap a Transport implementation and intercepts all
// BatchRequests, sending them to the transport racer task to read
// them asynchronously in a tight loop.
type raceTransport struct {
	Transport
}

func (tr raceTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	select {
	// We have a shallow copy here and so the top level scalar fields can't
	// really race, but making more copies doesn't make anything more
	// transparent, so from now on we operate on a pointer.
	case incoming <- &ba:
	default:
		// Avoid slowing down the tests if we're backed up.
	}
	return tr.Transport.SendNext(ctx, ba)
}

// GRPCTransportFactory during race builds wraps the implementation and
// intercepts all BatchRequests, reading them asynchronously in a tight loop.
// This allows the race detector to catch any mutations of a batch passed to the
// transport. The dealio is that batches passed to the transport are immutable -
// neither the client nor the server are allowed to mutate anything and this
// transport makes sure they don't. See client.Sender() for more.
func GRPCTransportFactory(
	opts SendOptions, nodeDialer *nodedialer.Dialer, replicas ReplicaSlice,
) (Transport, error) {
	if atomic.AddInt32(&running, 1) <= 1 {
		// NB: We can't use Stopper.RunWorker because doing so would race with
		// calling Stopper.Stop.
		if err := nodeDialer.Stopper().RunAsyncTask(
			context.TODO(), "transport racer", func(ctx context.Context) {
				var iters int
				var curIdx int
				defer func() {
					atomic.StoreInt32(&running, 0)
					log.Infof(
						ctx,
						"transport race promotion: ran %d iterations on up to %d requests",
						iters, curIdx+1,
					)
				}()
				// Make a fixed-size slice of *BatchRequest. When full, entries
				// are evicted in FIFO order.
				const size = 1000
				bas := make([]*roachpb.BatchRequest, size)
				encoder := json.NewEncoder(ioutil.Discard)
				for {
					iters++
					start := timeutil.Now()
					for _, ba := range bas {
						if ba != nil {
							if err := encoder.Encode(ba); err != nil {
								panic(err)
							}
						}
					}
					// Prevent the goroutine from spinning too hot as this lets CI
					// times skyrocket. Sleep on average for as long as we worked
					// on the last iteration so we spend no more than half our CPU
					// time on this task.
					jittered := time.After(jitter(timeutil.Since(start)))
					// Collect incoming requests until the jittered timer fires,
					// then access everything we have.
					for {
						select {
						case <-nodeDialer.Stopper().ShouldQuiesce():
							return
						case ba := <-incoming:
							bas[curIdx%size] = ba
							curIdx++
							continue
						case <-jittered:
						}
						break
					}
				}
			}); err != nil {
			// Failed to start async task, reset our state.
			atomic.StoreInt32(&running, 0)
		}
	}

	t, err := grpcTransportFactoryImpl(opts, nodeDialer, replicas)
	if err != nil {
		return nil, err
	}
	return &raceTransport{Transport: t}, nil
}
