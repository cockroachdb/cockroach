// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

var rpcsAllowedWhileBootstrapping = map[string]struct{}{
	"/cockroach.rpc.Heartbeat/Ping":             {},
	"/cockroach.gossip.Gossip/Gossip":           {},
	"/cockroach.server.serverpb.Init/Bootstrap": {},
	"/cockroach.server.serverpb.Admin/Health":   {},
}

// intercept provides logic for RPC interception based on the server's operational state.
// It checks if the server is fully operational. If it is, all RPCs are allowed.
// If the server is not operational (e.g., during bootstrapping), it checks if the
// incoming RPC, identified by fullName, is in a predefined list of RPCs that are
// permitted to run even in a non-operational state.
func intercept(
	smh *serveModeHandler, fullName string, errorSupplier func(methodName string) error,
) error {
	if smh.operational() {
		return nil
	}
	if _, allowed := rpcsAllowedWhileBootstrapping[fullName]; !allowed {
		return errorSupplier(fullName)
	}
	return nil
}
