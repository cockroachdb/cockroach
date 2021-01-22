// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build race

package kvcoord

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"reflect"
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
	// Make a copy of the requests slice, and shallow copies of the requests.
	// The caller is allowed to mutate the request after the call returns. Since
	// this transport has no way of checking who's doing mutations (the client -
	// which is allowed, or the server - which is not). So, for now, we exclude
	// the slice and the requests from any checks, since those are the parts that
	// the client currently mutates.
	requestsCopy := make([]roachpb.RequestUnion, len(ba.Requests))
	for i, ru := range ba.Requests {
		// ru is a RequestUnion interface, so we need some hoops to dereference it.
		requestsCopy[i] = reflect.Indirect(reflect.ValueOf(ru)).Interface().(roachpb.RequestUnion)
	}
	ba.Requests = requestsCopy
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
// the server is not allowed to mutate anything and this transport makes sure
// they don't. See client.Sender() for more.
//
// NOTE(andrei): We don't like this transport very much. It's slow, preventing
// us from running clusters with race binaries and, the way it's written, it
// prevents both the client and the server from mutating the BatchRequest. But
// only the server is prohibited (according to the client.Sender interface). In
// fact, we'd like to have the client reuse these requests and mutate them.
// Instead of this transport, we should find other mechanisms ensuring that:
// a) the server doesn't hold on to any memory, and
// b) the server doesn't mutate the request
func GRPCTransportFactory(
	opts SendOptions, nodeDialer *nodedialer.Dialer, replicas ReplicaSlice,
) (Transport, error) {
	if atomic.AddInt32(&running, 1) <= 1 {
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
