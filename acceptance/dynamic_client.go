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
// permissions and limitations under the License.

package acceptance

import (
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// dynamicClient should be used in acceptance tests when connecting to a
// cluster that may lose and gain nodes as the test proceeds.
type dynamicClient struct {
	cluster cluster.Cluster
	stopper *stop.Stopper

	mu struct {
		sync.Mutex
		// clients is a map from node indexes used by methods passed to the
		// cluster `c` to db clients.
		clients map[int]*sql.DB
	}
}

// newDyanmicClient creates a dynamic client. `close()` must be called after
// the dynamic client is no longer needed.
func newDynamicClient(cluster cluster.Cluster, stopper *stop.Stopper) *dynamicClient {
	dc := &dynamicClient{
		cluster: cluster,
		stopper: stopper,
	}
	dc.mu.clients = make(map[int]*sql.DB)
	return dc
}

// Close closes all connected database clients. Close implements the Closer
// interface.
func (dc *dynamicClient) Close() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for i, client := range dc.mu.clients {
		log.Infof("closing connection to %s", dc.cluster.Addr(i))
		client.Close()
		delete(dc.mu.clients, i)
	}
}

// isRetryableError returns true for any errors that show a connection issue
// or an issue with the node itself. This can occur when a node is restarting
// or is unstable in some other way.
func isRetryableError(err error) bool {
	return testutils.IsError(err, "(connection reset by peer|connection refused|failed to send RPC|EOF)")
}

var errTestFinished = errors.New("test is shutting down")

// exec calls exec on a client using a preexisting or new connection.
func (dc *dynamicClient) exec(query string, args ...interface{}) (sql.Result, error) {
	for dc.isRunning() {
		client, err := dc.getClient()
		if err != nil {
			return nil, err
		}
		if result, err := client.Exec(query, args...); err == nil || !isRetryableError(err) {
			return result, err
		}
	}
	return nil, errTestFinished
}

// queryRowScan performs first a QueryRow and follows that up with a Scan using
// a preexisting or new connection.
func (dc *dynamicClient) queryRowScan(query string, queryArgs, destArgs []interface{}) error {
	for dc.isRunning() {
		client, err := dc.getClient()
		if err != nil {
			return err
		}
		if err := client.QueryRow(query, queryArgs...).Scan(destArgs...); err == nil || !isRetryableError(err) {
			return err
		}
	}
	return errTestFinished
}

// getClient returns open client to a random node from the cluster.
func (dc *dynamicClient) getClient() (*sql.DB, error) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	indexes := rand.Perm(dc.cluster.NumNodes())
	for _, index := range indexes {
		if client, ok := dc.mu.clients[index]; ok {
			return client, nil
		}
		client, err := sql.Open("postgres", dc.cluster.PGUrl(index))
		if err != nil {
			log.Infof("could not establish connection to %s: %s", dc.cluster.Addr(index), err)
			continue
		}
		log.Infof("connection established to %s", dc.cluster.Addr(index))
		dc.mu.clients[index] = client
		return client, nil
	}

	// If we find that we end up having no connections often, consider putting
	// in a retry loop for this whole function.
	return nil, fmt.Errorf("there are no available connections to the cluster")
}

// isRunning returns true as long as the stopper is still running.
func (dc *dynamicClient) isRunning() bool {
	select {
	case <-dc.stopper.ShouldStop():
		return false
	default:
	}
	return true
}
