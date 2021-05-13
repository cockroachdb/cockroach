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
		EnvVar:      "COCKROACH_DENYLIST_FILE",
		Description: "Denylist file to limit access to IP addresses and tenant ids.",
	}

	ProxyListenAddr = FlagInfo{
		Name:        "listen-addr",
		EnvVar:      "COCKROACH_LISTEN_ADDR",
		Description: "Listen address for incoming connections.",
	}

	ListenCert = FlagInfo{
		Name:        "listen-cert",
		EnvVar:      "COCKROACH_LISTEN_CERT",
		Description: "File containing PEM-encoded x509 certificate for listen address.",
	}

	ListenKey = FlagInfo{
		Name:        "listen-key",
		EnvVar:      "COCKROACH_LISTEN_KEY",
		Description: "File containing PEM-encoded x509 key for listen address.",
	}

	ListenMetrics = FlagInfo{
		Name:        "listen-metrics",
		EnvVar:      "COCKROACH_LISTEN_METRICS",
		Description: "Listen address for incoming connections for metrics retrieval.",
	}

	RoutingRule = FlagInfo{
		Name:        "routing-rule",
		EnvVar:      "COCKROACH_ROUTING_RULE",
		Description: "Routing rule for incoming connections. Use '{{clusterName}}' for substitution.",
	}

	DirectoryAddr = FlagInfo{
		Name:        "directory",
		EnvVar:      "COCKROACH_DIRECTORY",
		Description: "Directory address of the service doing resolution from backend id to IP.",
	}

	SkipVerify = FlagInfo{
		Name:        "skip-verify",
		EnvVar:      "COCKROACH_SKIP_VERIFY",
		Description: "If true, skip identity verification of backend. For testing only.",
	}

	InsecureBackend = FlagInfo{
		Name:        "insecure",
		EnvVar:      "COCKROACH_INSECURE",
		Description: "If true, use insecure connection to the backend.",
	}

	RatelimitBaseDelay = FlagInfo{
		Name:        "ratelimit-base-delay",
		EnvVar:      "COCKROACH_RATELIMIT_BASE_DELAY",
		Description: "Initial backoff after a failed login attempt. Set to 0 to disable rate limiting.",
	}

	ValidateAccessInterval = FlagInfo{
		Name:        "validate-access-interval",
		EnvVar:      "COCKROACH_VALIDATE_ACCESS_INTERVAL",
		Description: "Time interval between validation that current connections are still valid.",
	}

	PollConfigInterval = FlagInfo{
		Name:        "poll-config-interval",
		EnvVar:      "COCKROACH_POLL_CONFIG_INTERVAL",
		Description: "Polling interval changes in config file.",
	}

	IdleTimeout = FlagInfo{
		Name:        "idle-timeout",
		EnvVar:      "COCKROACH_IDLE_TIMEOUT",
		Description: "Close connections idle for this duration.",
	}

	TestDirectoryListenPort = FlagInfo{
		Name:        "port",
		Description: "Test directory server binds and listens on this port.",
	}
)
