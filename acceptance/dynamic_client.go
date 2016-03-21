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
	"net/url"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/log"
)

// dynamicClient should be used in acceptance tests when connecting to a
// cluster that may lose and gain nodes as the test proceeds.
type dynamicClient struct {
	t       *testing.T
	running func() bool

	sync.Mutex    // This mutex protects all fields below.
	clientNumbers int
	urls          map[string]struct{}
	clients       map[int]*sql.DB
}

// newDyanmicClient creates a dynamic client. Running should be a threadsafe
// function that returns true if the test is still running.
func newDynamicClient(t *testing.T, c cluster.Cluster, running func() bool) *dynamicClient {
	dc := &dynamicClient{
		t:       t,
		clients: make(map[int]*sql.DB),
		running: running,
	}

	dc.updateURLs(c)
	return dc
}

// initClient initializes a new dynamic client and returns a unique client
// number that should be passed to the dc for all subsequent calls. All dc
// clients should call close when finished with them.
func (dc *dynamicClient) initClient() int {
	dc.Lock()
	defer dc.Unlock()
	dc.clientNumbers++
	return dc.clientNumbers
}

// close closes the client attached to the current clientNumber. This should be
// a deferred call following each call to initClient.
func (dc *dynamicClient) closeClient(clientNumber int) {
	dc.Lock()
	defer dc.Unlock()
	client, ok := dc.clients[clientNumber]
	if ok {
		client.Close()
		delete(dc.clients, clientNumber)
	}
}

// updateURLs adds all the URLs from the current cluster. This should be called
// after a new node is added or removed.
func (dc *dynamicClient) updateURLs(c cluster.Cluster) {
	dc.Lock()
	defer dc.Unlock()

	oldURLs := dc.urls
	dc.urls = make(map[string]struct{})
	for i := 0; i < c.NumNodes(); i++ {
		rawURL := c.PGUrl(i)
		dc.urls[rawURL] = struct{}{}
		if _, exists := oldURLs[rawURL]; !exists {
			log.Infof("%s added to dynamic client list", dc.getHost(rawURL))
		} else {
			delete(oldURLs, rawURL)
		}
	}

	for oldURL := range oldURLs {
		log.Infof("%s removed from dynamic client list", dc.getHost(oldURL))
	}
}

// isReconnectableError returns any errors that show a connection issue and not
// necessarily a client one. This can occur when a client is resetting or is
// unstable in some other way.
func isReconnectableError(err error) bool {
	return testutils.IsError(err, "connection reset by peer") ||
		testutils.IsError(err, "connection refused") ||
		testutils.IsError(err, "failed to send RPC") ||
		testutils.IsError(err, "EOF")
}

// exec calls exec on a client using a preexisting or creates a new connection
// if that one fails.
func (dc *dynamicClient) exec(clientNumber int, query string, args ...interface{}) (sql.Result, error) {
	for {
		client := dc.getClient(clientNumber)
		if client == nil {
			return nil, fmt.Errorf("there are no available connections to the cluster")
		}
		result, err := client.Exec(query, args...)
		if err == nil {
			return result, nil
		}
		if !isReconnectableError(err) {
			return result, err
		}
		dc.closeClient(clientNumber)
	}
}

// queryRowScan performs first a QueryRow and follows that up with a Scan.
func (dc *dynamicClient) queryRowScan(clientNumber int, query string, queryArgs, destArgs []interface{}) error {
	for {
		client := dc.getClient(clientNumber)
		if client == nil {
			return fmt.Errorf("there are no available connections to the cluster")
		}
		err := client.QueryRow(query, queryArgs...).Scan(destArgs...)
		if err == nil {
			return nil
		}
		if !isReconnectableError(err) {
			return err
		}
		dc.closeClient(clientNumber)
	}
}

// getClient first checks for an existing client and returns that if one
// already exists. If it doesn't exist, it tries to create a new one. Returns
// nil if unable to connect to any node.
func (dc *dynamicClient) getClient(clientNumber int) *sql.DB {
	dc.Lock()
	defer dc.Unlock()
	if client, ok := dc.clients[clientNumber]; ok {
		return client
	}

	for len(dc.urls) > 0 {
		// This relies on the fact that range will randomize the keys of dc.url so
		// new clients should pick a random node to connect to.
		for rawURL := range dc.urls {
			client, err := sql.Open("postgres", rawURL)
			if err == nil {
				dc.clients[clientNumber] = client
				log.Infof("%d: now connected to %s", clientNumber, dc.getHost(rawURL))
				return client
			}
			if !isReconnectableError(err) {
				log.Infof("could not create client for %s, removing it from the dynamic client", dc.getHost(rawURL))
				delete(dc.urls, rawURL)
				continue
			}
			// If we have a reconnectable error, leave the URL around for the next
			// pass.
		}
	}
	return nil
}

// getHost returns the host:port of the passed in raw URL.
func (dc *dynamicClient) getHost(rawURL string) string {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		dc.t.Fatal(err)
	}
	return parsedURL.Host
}
