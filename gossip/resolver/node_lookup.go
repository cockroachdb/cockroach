// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package resolver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const lookupTimeout = time.Second * 3

// nodeLookupResolver implements Resolver.
// It queries http(s)://<address>/_status/local and extracts the node's address.
// This is useful for http load balancers which will not forward RPC.
// It is never exhausted.
type nodeLookupResolver struct {
	context   *base.Context
	typ       string
	addr      string
	exhausted bool
	// We need our own client so that we may specify timeouts.
	httpClient *http.Client
}

// Type returns the resolver type.
func (nl *nodeLookupResolver) Type() string { return nl.typ }

// Addr returns the resolver address.
func (nl *nodeLookupResolver) Addr() string { return nl.addr }

// GetAddress returns a net.Addr or error.
// Upon errors, we set exhausted=true, then flip it back when called again.
func (nl *nodeLookupResolver) GetAddress() (net.Addr, error) {
	if nl.exhausted {
		nl.exhausted = false
		return nil, util.Errorf("skipping temporarily-exhausted resolved")
	}

	if nl.httpClient == nil {
		tlsConfig, err := nl.context.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		nl.httpClient = &http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
			Timeout:   lookupTimeout,
		}
	}

	nl.exhausted = true
	// TODO(marc): put common URIs in base and reuse everywhere.
	url := fmt.Sprintf("%s://%s/_status/local", nl.context.RequestScheme(), nl.addr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	log.Infof("querying %s for gossip nodes", url)
	resp, err := nl.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	local := struct {
		Address util.UnresolvedAddr `json:"address"`
		// We ignore all other fields.
	}{}

	err = json.Unmarshal(contents, &local)
	if err != nil {
		return nil, err
	}

	addr, err := resolveAddress(local.Address.Network(), local.Address.String())
	if err != nil {
		return nil, err
	}
	nl.exhausted = false
	log.Infof("found gossip node: %+v", addr)
	return addr, nil
}

func resolveAddress(network, address string) (net.Addr, error) {
	if network == "tcp" {
		_, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		return util.MakeUnresolvedAddr("tcp", address), nil
	}
	return nil, util.Errorf("unknown address type: %q", network)
}

// IsExhausted returns whether the resolver can yield further
// addresses.
func (nl *nodeLookupResolver) IsExhausted() bool { return nl.exhausted }
