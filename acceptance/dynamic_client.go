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
	t *testing.T

	sync.Mutex // This mutex protects all fields below.
	// lastConsumerId keeps track of the last consumer to have initialized
	// against the dynamic client in order to provide a new unique id.
	lastConsumerID int
	// urls is an internal list of currently available URLs. When a new
	// connection cannot be established to a url, it is expunged from this
	// list.
	urls map[string]struct{}
	// clients is a map from consumerIDs to db clients.
	clients map[int]*sql.DB
}

// newDyanmicClient creates a dynamic client.
func newDynamicClient(t *testing.T, c cluster.Cluster) *dynamicClient {
	dc := &dynamicClient{
		t:       t,
		clients: make(map[int]*sql.DB),
	}

	dc.updateURLs(c)
	return dc
}

// init initializes returns a unique consumer ID that should be passed  to the
// dynamic client for all subsequent calls. All dc consumers should call close
// when finished.
func (dc *dynamicClient) init() int {
	dc.Lock()
	defer dc.Unlock()
	dc.lastConsumerID++
	return dc.lastConsumerID
}

// close closes any clients attached to the current consumerID. This should be
// a deferred call following each call to init to ensure that any outstanding
// open clients are closed.
func (dc *dynamicClient) close(consumerID int) {
	dc.Lock()
	defer dc.Unlock()
	client, ok := dc.clients[consumerID]
	if ok {
		client.Close()
		delete(dc.clients, consumerID)
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
func (dc *dynamicClient) exec(consumerID int, query string, args ...interface{}) (sql.Result, error) {
	for {
		client := dc.getClient(consumerID)
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
		dc.close(consumerID)
	}
}

// queryRowScan performs first a QueryRow and follows that up with a Scan.
func (dc *dynamicClient) queryRowScan(consumerID int, query string, queryArgs, destArgs []interface{}) error {
	for {
		client := dc.getClient(consumerID)
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
		dc.close(consumerID)
	}
}

// getClient first checks for an existing client and returns that if one
// already exists. If it doesn't exist, it tries to create a new one. Returns
// nil if unable to connect to any node. Note that this should not typically
// be called directly from a consumer, they should call the DB methods
// directly (i.e. exec() or queryRowScan()).
func (dc *dynamicClient) getClient(consumerID int) *sql.DB {
	dc.Lock()
	defer dc.Unlock()
	if client, ok := dc.clients[consumerID]; ok {
		return client
	}

	for len(dc.urls) > 0 {
		// This relies on the fact that range will randomize the keys of dc.url so
		// new clients should pick a random node to connect to.
		for rawURL := range dc.urls {
			client, err := sql.Open("postgres", rawURL)
			if err == nil {
				dc.clients[consumerID] = client
				log.Infof("%d: now connected to %s", consumerID, dc.getHost(rawURL))
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
