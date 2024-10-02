// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliflags

// Flags specific to multi-tenancy commands.
var (
	TenantID = FlagInfo{
		Name:        "tenant-id",
		EnvVar:      "COCKROACH_TENANT_ID",
		Description: `The tenant ID under which to start the SQL server.`,
	}

	TenantIDFile = FlagInfo{
		Name: "tenant-id-file",
		Description: `Allows sourcing the tenant id from a file. The tenant id
will be expected to be by itself on the first line of the file. If the file does
not exist, or if the tenant id is incomplete, the tenant server will block, and
wait for the tenant id to be fully written to the file (with a newline character).`,
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

	AllowList = FlagInfo{
		Name:        "allowlist-file",
		Description: "Allow file to limit access to tenants based on IP addresses.",
	}

	ProxyListenAddr = FlagInfo{
		Name:        "listen-addr",
		Description: "Listen address for incoming connections.",
	}

	ProxyProtocolListenAddr = FlagInfo{
		Name:        "proxy-protocol-listen-addr",
		Description: "Listen address for incoming connections which require proxy protocol headers.",
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

	// TODO(joel): Remove this flag, and use --listen-addr for a non-proxy
	// protocol listener, and use --proxy-protocol-listen-addr for a proxy
	// protocol listener.
	RequireProxyProtocol = FlagInfo{
		Name: "require-proxy-protocol",
		Description: `Requires PROXY protocol on the SQL listener. The HTTP
listener will support the protocol on a best-effort basis. If this is set to
true, the PROXY info from upstream will be trusted on both SQL and HTTP
listeners, if the headers are allowed.`,
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

	Virtualized = FlagInfo{
		Name:        "virtualized",
		Description: "If set, the cluster will be initialized as a virtualized cluster.",
	}
	VirtualizedEmpty = FlagInfo{
		Name:        "virtualized-empty",
		Description: "If set, the cluster will be initialized as a virtualized cluster without main virtual cluster.",
	}
)
