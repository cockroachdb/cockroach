// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cliflags

// Flags specific to multi-tenancy commands.
var (
	TenantID = FlagInfo{
		Name:        "tenant-id",
		EnvVar:      "COCKROACH_TENANT_ID",
		Description: `The tenant ID under which to start the SQL server.`,
	}

	KVAddrs = FlagInfo{
		Name:        "kv-addrs",
		EnvVar:      "COCKROACH_KV_ADDRS",
		Description: `A comma-separated list of KV endpoints (load balancers allowed).`,
	}

	DenyList = FlagInfo{
		Name:        "denylist-file",
		Description: "Denylist file to limit access to IP addresses and tenant ids.",
	}

	ProxyListenAddr = FlagInfo{
		Name:        "listen-addr",
		Description: "Listen address for incoming connections.",
	}

	ListenCert = FlagInfo{
		Name:        "listen-cert",
		Description: "File containing PEM-encoded x509 certificate for listen address.",
	}

	ListenKey = FlagInfo{
		Name:        "listen-key",
		Description: "File containing PEM-encoded x509 key for listen address.",
	}

	ListenMetrics = FlagInfo{
		Name:        "listen-metrics",
		Description: "Listen address for incoming connections for metrics retrieval.",
	}

	RoutingRule = FlagInfo{
		Name: "routing-rule",
		Description: `
Routing rule for incoming connections. Use '{{clusterName}}' for substitution.
This rule must include the port of the SQL pod.`,
	}

	DirectoryAddr = FlagInfo{
		Name:        "directory",
		Description: "Directory address of the service doing resolution from backend id to IP.",
	}

	SkipVerify = FlagInfo{
		Name:        "skip-verify",
		Description: "If true, skip identity verification of backend. For testing only.",
	}

	InsecureBackend = FlagInfo{
		Name:        "insecure",
		Description: "If true, use insecure connection to the backend.",
	}

	RatelimitBaseDelay = FlagInfo{
		Name:        "ratelimit-base-delay",
		Description: "Initial backoff after a failed login attempt. Set to 0 to disable rate limiting.",
	}

	ValidateAccessInterval = FlagInfo{
		Name:        "validate-access-interval",
		Description: "Time interval between validation that current connections are still valid.",
	}

	PollConfigInterval = FlagInfo{
		Name:        "poll-config-interval",
		Description: "Polling interval changes in config file.",
	}

	DrainTimeout = FlagInfo{
		Name:        "drain-timeout",
		Description: "Close DRAINING connections idle for this duration.",
	}

	TestDirectoryListenPort = FlagInfo{
		Name:        "port",
		Description: "Test directory server binds and listens on this port.",
	}
)
