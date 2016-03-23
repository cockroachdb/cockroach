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
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
)

// dynamicClient should be used in acceptance tests when connecting to a
// cluster that may lose and gain nodes as the test proceeds.
type dynamicClient struct {
	t        *testing.T
	c        cluster.Cluster
	finished <-chan struct{}

	mu struct {
		sync.Mutex                 // Protects all fields in the mu struct.
		clients    map[int]*sql.DB // clients is a map from node indexes to db clients.
	}
}

// newDyanmicClient creates a dynamic client. `close()` must be called after
// the dynamic client is no longer needed.
func newDynamicClient(t *testing.T, c cluster.Cluster, finished <-chan struct{}) *dynamicClient {
	dc := &dynamicClient{
		t:        t,
		c:        c,
		finished: finished,
	}
	dc.mu.clients = make(map[int]*sql.DB)
	return dc
}

// close closes all connected database clients.
func (dc *dynamicClient) close() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for i, client := range dc.mu.clients {
		if client != nil {
			log.Infof("closing connection to %s", dc.c.Addr(i))
			client.Close()
			delete(dc.mu.clients, i)
		}
	}
}

// isRetryableError returns true for any errors that show a connection issue
// or possibly an issue with the node itself. This can occur when a node is
// resetting or is unstable in some other way.
func isRetryableError(err error) bool {
	return testutils.IsError(err, "(connection reset by peer|connection refused|failed to send RPC|EOF)")
}

// exec calls exec on a client using a preexisting or new connection.
func (dc *dynamicClient) exec(query string, args ...interface{}) (sql.Result, error) {
	for dc.isRunning() {
		client := dc.getClient()
		if client == nil {
			return nil, fmt.Errorf("there are no available connections to the cluster")
		}
		result, err := client.Exec(query, args...)
		if err == nil {
			return result, nil
		}
		if !isRetryableError(err) {
			return result, err
		}
	}
	return nil, nil
}

// queryRowScan performs first a QueryRow and follows that up with a Scan using
// a preexisting or new connection.
func (dc *dynamicClient) queryRowScan(query string, queryArgs, destArgs []interface{}) error {
	for dc.isRunning() {
		client := dc.getClient()
		if client == nil {
			return fmt.Errorf("there are no available connections to the cluster")
		}
		err := client.QueryRow(query, queryArgs...).Scan(destArgs...)
		if err == nil {
			return nil
		}
		if !isRetryableError(err) {
			return err
		}
	}
	return nil
}

// getClient returns open client to a random node from the cluster. Returns nil
// if no connection could be made.
func (dc *dynamicClient) getClient() *sql.DB {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	indexes := rand.Perm(dc.c.NumNodes())
	for _, index := range indexes {
		client := dc.mu.clients[index]
		// If we don't have a cached connection, create a new one.
		if client == nil {
			var err error
			client, err = sql.Open("postgres", dc.c.PGUrl(index))
			if err != nil {
				log.Infof("could not establish connection to %s: %s", dc.c.Addr(index), err)
				continue
			}
			if client == nil {
				log.Infof("could not establish connection to %s", dc.c.Addr(index))
				continue
			}
			log.Infof("connection established to %s", dc.c.Addr(index))
			dc.mu.clients[index] = client
		}
		// Ensure that connection is active.
		if err := client.Ping(); err != nil {
			// Could not ping the client, close the connection.
			log.Infof("closing established connection due to error %s: %s", dc.c.Addr(index), err)
			client.Close()
			delete(dc.mu.clients, index)
			continue
		}
		return client
	}

	// If we find that we end up having no connections often, consider putting
	// in a retry loop for this whole function.
	return nil
}

// isRunning returns true as long as the finished channel is still open.
func (dc *dynamicClient) isRunning() bool {
	select {
	case <-dc.finished:
		return false
	default:
	}
	return true
}
