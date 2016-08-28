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
//
// Author: Tobias Schottdorf

// +build race

package kv

import (
	"encoding/gob"
	"io/ioutil"
	"math/rand"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/log"
)

var running atomic.Value // bool
var incoming chan *roachpb.BatchRequest

func init() {
	running.Store(false)
	incoming = make(chan *roachpb.BatchRequest, 100)
}

func jitter() time.Duration {
	// Prevent the goroutine from spinning too hot as this lets CI times
	// skyrocket.
	return time.Duration(rand.Int63n(int64(300 * time.Microsecond)))
}

// grpcTransportFactory during race builds wraps the implementation and
// intercepts all BatchRequests, reading them in a tight loop. This allows the
// race detector to catch any mutations of a batch passed to the transport.
func grpcTransportFactory(
	opts SendOptions,
	rpcContext *rpc.Context,
	replicas ReplicaSlice,
	args roachpb.BatchRequest,
) (Transport, error) {
	if !running.Load().(bool) {
		running.Store(true)
		rpcContext.Stopper.RunWorker(func() {
			var iters int
			defer running.Store(false)
			defer func() {
				log.Infof(context.Background(), "transport race promotion: ran %d iterations", iters)
			}()
			const size = 1000
			bas := make([]*roachpb.BatchRequest, size)
			encoder := gob.NewEncoder(ioutil.Discard)
			var curIdx int
			for {
				select {
				case <-rpcContext.Stopper.ShouldStop():
					return
				case <-time.After(jitter()):
				}

				iters++
				for i := 0; i < size/10; i++ {
					select {
					case ba := <-incoming:
						bas[curIdx] = ba
						curIdx = (curIdx + 1) % size
						continue
					default:
					}
					break
				}
				for _, ba := range bas {
					if ba != nil {
						if err := encoder.Encode(&ba); err != nil {
							panic(err)
						}
					}
				}
			}
		})
	}
	select {
	case incoming <- &args:
	default:
	}
	return grpcTransportFactoryImpl(opts, rpcContext, replicas, args)
}
