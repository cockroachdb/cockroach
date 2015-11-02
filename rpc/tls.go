// Copyright 2014 The Cockroach Authors.
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
// Author: jqmp (jaqueramaphan@gmail.com)

package rpc

// TODO(jqmp): The use of TLS here is just a proof of concept; its security
// properties haven't been analyzed or audited.

import (
	"crypto/tls"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/util/log"
)

// tlsListen wraps either net.Listen or crypto/tls.Listen, depending on the contents of
// the passed TLS Config.
func tlsListen(network, address string, config *tls.Config) (net.Listener, error) {
	if config == nil {
		// Warn if starting a network-accessible server without TLS.
		// Unix sockets can't use TLS but have other security mechanisms.
		// Port 0 is mainly used for transient test servers so we don't warn in this case.
		if network != "unix" && !strings.HasSuffix(address, ":0") {
			log.Warningf("listening via %s to %s without TLS", network, address)
		}
		return net.Listen(network, address)
	}
	return tls.Listen(network, address, config)
}
