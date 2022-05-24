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

	ThrottleBaseDelay = FlagInfo{
		Name:        "throttle-base-delay",
		Description: "Initial value for the exponential backoff used to throttle connection attempts.",
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
		Name:        "routing-rule",
		Description: "Routing rule for incoming connections. This rule must include the port of the SQL pod.",
	}

	DirectoryAddr = FlagInfo{
		Name:        "directory",
		Description: "Directory address of the service doing resolution of tenants to their IP addresses.",
	}

	// TODO(chrisseto): Remove skip-verify as a CLI option. It should only be
	// set internally for testing, rather than being exposed to consumers.
	SkipVerify = FlagInfo{
		Name:        "skip-verify",
		Description: "If true, skip identity verification of backend. For testing only.",
	}

	InsecureBackend = FlagInfo{
		Name:        "insecure",
		Description: "If true, use insecure connection to the backend.",
	}

	DisableConnectionRebalancing = FlagInfo{
		Name:        "disable-connection-rebalancing",
		Description: "If true, proxy will not attempt to rebalance connections.",
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

	TestDirectoryListenPort = FlagInfo{
		Name:        "port",
		Description: "Test directory server binds and listens on this port.",
	}

	TestDirectoryTenantCertsDir = FlagInfo{
		Name:        "certs-dir",
		Description: CertsDir.Description,
	}

	TestDirectoryTenantBaseDir = FlagInfo{
		Name:        "base-dir",
		Description: "If set, the tenant processes will use it as a store location.",
	}
)
