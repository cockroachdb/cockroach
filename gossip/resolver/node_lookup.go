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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package resolver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
)

// nodeLookupResolver implements Resolver.
// It queries http(s)://<address>/_status/details/local and extracts the node's
// address. This is useful for http load balancers which will not forward RPC.
// It is never exhausted.
type nodeLookupResolver struct {
	context *base.Context
	typ     string
	addr    string
	// We need our own client so that we may specify timeouts.
	httpClient *http.Client
}

// Type returns the resolver type.
func (nl *nodeLookupResolver) Type() string { return nl.typ }

// Addr returns the resolver address.
func (nl *nodeLookupResolver) Addr() string { return nl.addr }

// GetAddress returns a net.Addr or error.
func (nl *nodeLookupResolver) GetAddress() (net.Addr, error) {
	if nl.httpClient == nil {
		tlsConfig, err := nl.context.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		nl.httpClient = &http.Client{
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
			Timeout:   base.NetworkTimeout,
		}
	}

	// TODO(marc): put common URIs in base and reuse everywhere.
	url := fmt.Sprintf("%s://%s/_status/details/local", nl.context.HTTPRequestScheme(), nl.addr)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
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

// IsExhausted always returns true, as there's no way to know how many
// nodes are behind a load balancer.
func (nl *nodeLookupResolver) IsExhausted() bool { return true }
