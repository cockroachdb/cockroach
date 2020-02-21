// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Base config defaults.
const (
	defaultInsecure = false
	defaultUser     = security.RootUser
	httpScheme      = "http"
	httpsScheme     = "https"

	// From IANA Service Name and Transport Protocol Port Number Registry. See
	// https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=cockroachdb
	//
	// This is used for both RPC and SQL connections unless --sql-addr
	// is used on the command line and/or SQLAddr is set in the Config object.
	DefaultPort = "26257"

	// The default port for HTTP-for-humans.
	DefaultHTTPPort = "8080"

	// NB: net.JoinHostPort is not a constant.
	defaultAddr     = ":" + DefaultPort
	defaultHTTPAddr = ":" + DefaultHTTPPort
	defaultSQLAddr  = ":" + DefaultPort

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// DefaultCertsDirectory is the default value for the cert directory flag.
	DefaultCertsDirectory = "${HOME}/.cockroach-certs"

	// defaultRaftTickInterval is the default resolution of the Raft timer.
	defaultRaftTickInterval = 200 * time.Millisecond

	// defaultRangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the
	// leader lease active duration should be of the raft election timeout.
	defaultRangeLeaseRaftElectionTimeoutMultiplier = 3

	// defaultRPCHeartbeatInterval is the default value of RPCHeartbeatInterval
	// used by the rpc context.
	defaultRPCHeartbeatInterval = 3 * time.Second

	// rangeLeaseRenewalFraction specifies what fraction the range lease
	// renewal duration should be of the range lease active time. For example,
	// with a value of 0.2 and a lease duration of 10 seconds, leases would be
	// eagerly renewed 2 seconds into each lease.
	rangeLeaseRenewalFraction = 0.5

	// livenessRenewalFraction specifies what fraction the node liveness
	// renewal duration should be of the node liveness duration. For example,
	// with a value of 0.2 and a liveness duration of 10 seconds, each node's
	// liveness record would be eagerly renewed after 2 seconds.
	livenessRenewalFraction = 0.5

	// DefaultTableDescriptorLeaseDuration is the default mean duration a
	// lease will be acquired for. The actual duration is jittered using
	// the jitter fraction. Jittering is done to prevent multiple leases
	// from being renewed simultaneously if they were all acquired
	// simultaneously.
	DefaultTableDescriptorLeaseDuration = 5 * time.Minute

	// DefaultTableDescriptorLeaseJitterFraction is the default factor
	// that we use to randomly jitter the lease duration when acquiring a
	// new lease and the lease renewal timeout.
	DefaultTableDescriptorLeaseJitterFraction = 0.05

	// DefaultTableDescriptorLeaseRenewalTimeout is the default time
	// before a lease expires when acquisition to renew the lease begins.
	DefaultTableDescriptorLeaseRenewalTimeout = time.Minute
)

var (
	// defaultRaftElectionTimeoutTicks specifies the number of Raft Tick
	// invocations that must pass between elections.
	defaultRaftElectionTimeoutTicks = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS", 15)

	// defaultRaftLogTruncationThreshold specifies the upper bound that a single
	// Range's Raft log can grow to before log truncations are triggered while at
	// least one follower is missing. If all followers are active, the quota pool
	// is responsible for ensuring the raft log doesn't grow without bound by
	// making sure the leader doesn't get too far ahead.
	defaultRaftLogTruncationThreshold = envutil.EnvOrDefaultInt64(
		"COCKROACH_RAFT_LOG_TRUNCATION_THRESHOLD", 4<<20 /* 4 MB */)

	// defaultRaftMaxSizePerMsg specifies the maximum aggregate byte size of Raft
	// log entries that a leader will send to followers in a single MsgApp.
	defaultRaftMaxSizePerMsg = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_MAX_SIZE_PER_MSG", 16<<10 /* 16 KB */)

	// defaultRaftMaxSizeCommittedSizePerReady specifies the maximum aggregate
	// byte size of the committed log entries which a node will receive in a
	// single Ready.
	defaultRaftMaxCommittedSizePerReady = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_MAX_COMMITTED_SIZE_PER_READY", 64<<20 /* 64 MB */)

	// defaultRaftMaxSizePerMsg specifies how many "inflight" messages a leader
	// will send to a follower without hearing a response.
	defaultRaftMaxInflightMsgs = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_MAX_INFLIGHT_MSGS", 64)
)

type lazyHTTPClient struct {
	once       sync.Once
	httpClient http.Client
	err        error
}

type lazyCertificateManager struct {
	once sync.Once
	cm   *security.CertificateManager
	err  error
}

// Config is embedded by server.Config. A base config is not meant to be used
// directly, but embedding configs should call cfg.InitDefaults().
type Config struct {
	// Insecure specifies whether to use SSL or not.
	// This is really not recommended.
	Insecure bool

	// SSLCAKey is used to sign new certs.
	SSLCAKey string
	// SSLCertsDir is the path to the certificate/key directory.
	SSLCertsDir string

	// User running this process. It could be the user under which
	// the server is running or the user passed in client calls.
	User string

	// Addr is the address the server is listening on.
	Addr string

	// AdvertiseAddr is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Addr is listening on.
	AdvertiseAddr string

	// ClusterName is the name used as a sanity check when a node joins
	// an uninitialized cluster, or when an uninitialized node joins an
	// initialized cluster. The initial RPC handshake verifies that the
	// name matches on both sides. Once the cluster ID has been
	// negotiated on both sides, the cluster name is not used any more.
	ClusterName string

	// DisableClusterNameVerification, when set, alters the cluster name
	// verification to only verify that a non-empty cluster name on
	// both sides match. This is meant for use while rolling an
	// existing cluster into using a new cluster name.
	DisableClusterNameVerification bool

	// SplitListenSQL indicates whether to listen for SQL
	// clients on a separate address from RPC requests.
	SplitListenSQL bool

	// SQLAddr is the configured SQL listen address.
	// This is used if SplitListenSQL is set to true.
	SQLAddr string

	// SQLAdvertiseAddr is the advertised SQL address.
	// This is computed from SQLAddr if specified otherwise Addr.
	SQLAdvertiseAddr string

	// HTTPAddr is the configured HTTP listen address.
	HTTPAddr string

	// HTTPAdvertiseAddr is the advertised HTTP address.
	// This is computed from HTTPAddr if specified otherwise Addr.
	HTTPAdvertiseAddr string

	// The certificate manager. Must be accessed through GetCertificateManager.
	certificateManager lazyCertificateManager

	// httpClient uses the client TLS config. It is initialized lazily.
	httpClient lazyHTTPClient

	// HistogramWindowInterval is used to determine the approximate length of time
	// that individual samples are retained in in-memory histograms. Currently,
	// it is set to the arbitrary length of six times the Metrics sample interval.
	// See the comment in server.Config for more details.
	HistogramWindowInterval time.Duration

	// RPCHeartbeatInterval controls how often a Ping request is sent on peer
	// connections to determine connection health and update the local view
	// of remote clocks.
	RPCHeartbeatInterval time.Duration
}

func wrapError(err error) error {
	if _, ok := err.(*security.Error); !ok {
		return &security.Error{
			Message: "problem using security settings",
			Err:     err,
		}
	}
	return err
}

// InitDefaults sets up the default values for a config.
// This is also used in tests to reset global objects.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.User = defaultUser
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
	cfg.HTTPAdvertiseAddr = ""
	cfg.SplitListenSQL = false
	cfg.SQLAddr = defaultSQLAddr
	cfg.SQLAdvertiseAddr = cfg.SQLAddr
	cfg.SSLCertsDir = DefaultCertsDirectory
	cfg.certificateManager = lazyCertificateManager{}
	cfg.RPCHeartbeatInterval = defaultRPCHeartbeatInterval
	cfg.ClusterName = ""
	cfg.DisableClusterNameVerification = false
}

// HTTPRequestScheme returns "http" or "https" based on the value of Insecure.
func (cfg *Config) HTTPRequestScheme() string {
	if cfg.Insecure {
		return httpScheme
	}
	return httpsScheme
}

// AdminURL returns the URL for the admin UI.
func (cfg *Config) AdminURL() *url.URL {
	return &url.URL{
		Scheme: cfg.HTTPRequestScheme(),
		Host:   cfg.HTTPAdvertiseAddr,
	}
}

// GetClientCertPaths returns the paths to the client cert and key.
func (cfg *Config) GetClientCertPaths(user string) (string, string, error) {
	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return "", "", err
	}
	return cm.GetClientCertPaths(user)
}

// GetCACertPath returns the path to the CA certificate.
func (cfg *Config) GetCACertPath() (string, error) {
	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return "", err
	}
	return cm.GetCACertPath()
}

// LoadSecurityOptions extends a url.Values with SSL settings suitable for
// the given server config. It returns true if and only if the URL
// already contained SSL config options.
func (cfg *Config) LoadSecurityOptions(options url.Values, username string) error {
	if cfg.Insecure {
		options.Set("sslmode", "disable")
		options.Del("sslrootcert")
		options.Del("sslcert")
		options.Del("sslkey")
	} else {
		sslMode := options.Get("sslmode")
		if sslMode == "" || sslMode == "disable" {
			options.Set("sslmode", "verify-full")
		}

		if sslMode != "require" {
			// verify-ca and verify-full need a CA certificate.
			if options.Get("sslrootcert") == "" {
				// Fetch CA cert. This is required.
				caCertPath, err := cfg.GetCACertPath()
				if err != nil {
					return wrapError(err)
				}
				options.Set("sslrootcert", caCertPath)
			}
		} else {
			// require does not check the CA.
			options.Del("sslrootcert")
		}

		// Fetch certs, but don't fail, we may be using a password.
		certPath, keyPath, err := cfg.GetClientCertPaths(username)
		if err == nil {
			if options.Get("sslcert") == "" {
				options.Set("sslcert", certPath)
			}
			if options.Get("sslkey") == "" {
				options.Set("sslkey", keyPath)
			}
		}
	}
	return nil
}

// PGURL constructs a URL for the postgres endpoint, given a server
// config. There is no default database set.
func (cfg *Config) PGURL(user *url.Userinfo) (*url.URL, error) {
	options := url.Values{}
	if err := cfg.LoadSecurityOptions(options, user.Username()); err != nil {
		return nil, err
	}
	return &url.URL{
		Scheme:   "postgresql",
		User:     user,
		Host:     cfg.SQLAdvertiseAddr,
		RawQuery: options.Encode(),
	}, nil
}

// GetCertificateManager returns the certificate manager, initializing it
// on the first call.
func (cfg *Config) GetCertificateManager() (*security.CertificateManager, error) {
	cfg.certificateManager.once.Do(func() {
		cfg.certificateManager.cm, cfg.certificateManager.err =
			security.NewCertificateManager(cfg.SSLCertsDir)
	})
	return cfg.certificateManager.cm, cfg.certificateManager.err
}

// InitializeNodeTLSConfigs tries to load client and server-side TLS configs.
// It also enables the reload-on-SIGHUP functionality on the certificate manager.
// This should be called early in the life of the server to make sure there are no
// issues with TLS configs.
// Returns the certificate manager if successfully created and in secure mode.
func (cfg *Config) InitializeNodeTLSConfigs(
	stopper *stop.Stopper,
) (*security.CertificateManager, error) {
	if cfg.Insecure {
		return nil, nil
	}

	if _, err := cfg.GetServerTLSConfig(); err != nil {
		return nil, err
	}
	if _, err := cfg.GetUIServerTLSConfig(); err != nil {
		return nil, err
	}
	if _, err := cfg.GetClientTLSConfig(); err != nil {
		return nil, err
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, err
	}
	cm.RegisterSignalHandler(stopper)
	return cm, nil
}

// GetClientTLSConfig returns the client TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a TLS config using certs for the config.User.
// This TLSConfig might **NOT** be suitable to talk to the Admin UI, use GetUIClientTLSConfig instead.
func (cfg *Config) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetClientTLSConfig(cfg.User)
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetUIClientTLSConfig returns the client TLS config for Admin UI clients, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a TLS config configured to talk to the Admin UI.
// This TLSConfig is **NOT** suitable to talk to the GRPC or SQL servers, use GetClientTLSConfig instead.
func (cfg *Config) GetUIClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetUIClientTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetServerTLSConfig returns the server TLS config, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a server TLS config.
func (cfg *Config) GetServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetServerTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetUIServerTLSConfig returns the server TLS config for the Admin UI, initializing it if needed.
// If Insecure is true, return a nil config, otherwise ask the certificate
// manager for a server UI TLS config.
func (cfg *Config) GetUIServerTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	tlsCfg, err := cm.GetUIServerTLSConfig()
	if err != nil {
		return nil, wrapError(err)
	}
	return tlsCfg, nil
}

// GetHTTPClient returns the http client, initializing it
// if needed. It uses the client TLS config.
func (cfg *Config) GetHTTPClient() (http.Client, error) {
	cfg.httpClient.once.Do(func() {
		cfg.httpClient.httpClient.Timeout = 10 * time.Second
		var transport http.Transport
		cfg.httpClient.httpClient.Transport = &transport
		transport.TLSClientConfig, cfg.httpClient.err = cfg.GetUIClientTLSConfig()
	})

	return cfg.httpClient.httpClient, cfg.httpClient.err
}

// RaftConfig holds raft tuning parameters.
type RaftConfig struct {
	// RaftTickInterval is the resolution of the Raft timer.
	RaftTickInterval time.Duration

	// RaftElectionTimeoutTicks is the number of raft ticks before the
	// previous election expires. This value is inherited by individual stores
	// unless overridden.
	RaftElectionTimeoutTicks int

	// RangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the leader
	// lease active duration should be of the raft election timeout.
	RangeLeaseRaftElectionTimeoutMultiplier float64

	// RaftLogTruncationThreshold controls how large a single Range's Raft log
	// can grow. When a Range's Raft log grows above this size, the Range will
	// begin performing log truncations.
	RaftLogTruncationThreshold int64

	// RaftProposalQuota controls the maximum aggregate size of Raft commands
	// that a leader is allowed to propose concurrently.
	//
	// By default, the quota is set to a fraction of the Raft log truncation
	// threshold. In doing so, we ensure all replicas have sufficiently up to
	// date logs so that when the log gets truncated, the followers do not need
	// non-preemptive snapshots. Changing this deserves care. Too low and
	// everything comes to a grinding halt, too high and we're not really
	// throttling anything (we'll still generate snapshots).
	RaftProposalQuota int64

	// RaftMaxUncommittedEntriesSize controls how large the uncommitted tail of
	// the Raft log can grow. The limit is meant to provide protection against
	// unbounded Raft log growth when quorum is lost and entries stop being
	// committed but continue to be proposed.
	RaftMaxUncommittedEntriesSize uint64

	// RaftMaxSizePerMsg controls the maximum aggregate byte size of Raft log
	// entries the leader will send to followers in a single MsgApp.
	RaftMaxSizePerMsg uint64

	// RaftMaxCommittedSizePerReady controls the maximum aggregate byte size of
	// committed Raft log entries a replica will receive in a single Ready.
	RaftMaxCommittedSizePerReady uint64

	// RaftMaxInflightMsgs controls how many "inflight" messages Raft will send
	// to a follower without hearing a response. The total number of Raft log
	// entries is a combination of this setting and RaftMaxSizePerMsg. The
	// current default settings provide for up to 1 MB of raft log to be sent
	// without acknowledgement. With an average entry size of 1 KB that
	// translates to ~1024 commands that might be executed in the handling of a
	// single raft.Ready operation.
	RaftMaxInflightMsgs int

	// Splitting a range which has a replica needing a snapshot results in two
	// ranges in that state. The delay configured here slows down splits when in
	// that situation (limiting to those splits not run through the split
	// queue). The most important target here are the splits performed by
	// backup/restore.
	//
	// -1 to disable.
	RaftDelaySplitToSuppressSnapshotTicks int
}

// SetDefaults initializes unset fields.
func (cfg *RaftConfig) SetDefaults() {
	if cfg.RaftTickInterval == 0 {
		cfg.RaftTickInterval = defaultRaftTickInterval
	}
	if cfg.RaftElectionTimeoutTicks == 0 {
		cfg.RaftElectionTimeoutTicks = defaultRaftElectionTimeoutTicks
	}
	if cfg.RangeLeaseRaftElectionTimeoutMultiplier == 0 {
		cfg.RangeLeaseRaftElectionTimeoutMultiplier = defaultRangeLeaseRaftElectionTimeoutMultiplier
	}
	if cfg.RaftLogTruncationThreshold == 0 {
		cfg.RaftLogTruncationThreshold = defaultRaftLogTruncationThreshold
	}
	if cfg.RaftProposalQuota == 0 {
		// By default, set this to a fraction of RaftLogMaxSize. See the comment
		// on the field for the tradeoffs of setting this higher or lower.
		cfg.RaftProposalQuota = cfg.RaftLogTruncationThreshold / 4
	}
	if cfg.RaftMaxUncommittedEntriesSize == 0 {
		// By default, set this to twice the RaftProposalQuota. The logic here
		// is that the quotaPool should be responsible for throttling proposals
		// in all cases except for unbounded Raft re-proposals because it queues
		// efficiently instead of dropping proposals on the floor indiscriminately.
		cfg.RaftMaxUncommittedEntriesSize = uint64(2 * cfg.RaftProposalQuota)
	}
	if cfg.RaftMaxSizePerMsg == 0 {
		cfg.RaftMaxSizePerMsg = uint64(defaultRaftMaxSizePerMsg)
	}
	if cfg.RaftMaxCommittedSizePerReady == 0 {
		cfg.RaftMaxCommittedSizePerReady = uint64(defaultRaftMaxCommittedSizePerReady)
	}
	if cfg.RaftMaxInflightMsgs == 0 {
		cfg.RaftMaxInflightMsgs = defaultRaftMaxInflightMsgs
	}

	if cfg.RaftDelaySplitToSuppressSnapshotTicks == 0 {
		// The Raft Ticks interval defaults to 200ms, and an election is 15
		// ticks. Add a generous amount of ticks to make sure even a backed up
		// Raft snapshot queue is going to make progress when a (not overly
		// concurrent) amount of splits happens.
		// The generous amount should result in a delay sufficient to
		// transmit at least one snapshot with the slow delay, which
		// with default settings is max 64MB at 2MB/s, ie 32 seconds.
		//
		// The resulting delay configured here is about 50s.
		cfg.RaftDelaySplitToSuppressSnapshotTicks = 3*cfg.RaftElectionTimeoutTicks + 200
	}
}

// RaftElectionTimeout returns the raft election timeout, as computed from the
// tick interval and number of election timeout ticks.
func (cfg RaftConfig) RaftElectionTimeout() time.Duration {
	return time.Duration(cfg.RaftElectionTimeoutTicks) * cfg.RaftTickInterval
}

// RangeLeaseDurations computes durations for range lease expiration and
// renewal based on a default multiple of Raft election timeout.
func (cfg RaftConfig) RangeLeaseDurations() (rangeLeaseActive, rangeLeaseRenewal time.Duration) {
	rangeLeaseActive = time.Duration(cfg.RangeLeaseRaftElectionTimeoutMultiplier *
		float64(cfg.RaftElectionTimeout()))
	rangeLeaseRenewal = time.Duration(float64(rangeLeaseActive) * rangeLeaseRenewalFraction)
	return
}

// RangeLeaseActiveDuration is the duration of the active period of leader
// leases requested.
func (cfg RaftConfig) RangeLeaseActiveDuration() time.Duration {
	rangeLeaseActive, _ := cfg.RangeLeaseDurations()
	return rangeLeaseActive
}

// RangeLeaseRenewalDuration specifies a time interval at the end of the
// active lease interval (i.e. bounded to the right by the start of the stasis
// period) during which operations will trigger an asynchronous renewal of the
// lease.
func (cfg RaftConfig) RangeLeaseRenewalDuration() time.Duration {
	_, rangeLeaseRenewal := cfg.RangeLeaseDurations()
	return rangeLeaseRenewal
}

// NodeLivenessDurations computes durations for node liveness expiration and
// renewal based on a default multiple of Raft election timeout.
func (cfg RaftConfig) NodeLivenessDurations() (livenessActive, livenessRenewal time.Duration) {
	livenessActive = cfg.RangeLeaseActiveDuration()
	livenessRenewal = time.Duration(float64(livenessActive) * livenessRenewalFraction)
	return
}

// SentinelGossipTTL is time-to-live for the gossip sentinel. The sentinel
// informs a node whether or not it's connected to the primary gossip network
// and not just a partition. As such it must expire fairly quickly and be
// continually re-gossiped as a connected gossip network is necessary to
// propagate liveness. The replica which is the lease holder of the first range
// gossips it.
func (cfg RaftConfig) SentinelGossipTTL() time.Duration {
	return cfg.RangeLeaseActiveDuration() / 2
}

// DefaultRetryOptions should be used for retrying most
// network-dependent operations.
func DefaultRetryOptions() retry.Options {
	// TODO(bdarnell): This should vary with network latency.
	// Derive the retry options from a configured or measured
	// estimate of latency.
	return retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
	}
}

// StorageConfig contains storage configs for all storage engine.
type StorageConfig struct {
	Attrs roachpb.Attributes
	// Dir is the data directory for the Pebble instance.
	Dir string
	// If true, creating the instance fails if the target directory does not hold
	// an initialized instance.
	//
	// Makes no sense for in-memory instances.
	MustExist bool
	// MaxSize is used for calculating free space and making rebalancing
	// decisions. Zero indicates that there is no maximum size.
	MaxSize int64
	// Settings instance for cluster-wide knobs.
	Settings *cluster.Settings
	// UseFileRegistry is true if the file registry is needed (eg: encryption-at-rest).
	// This may force the store version to versionFileRegistry if currently lower.
	UseFileRegistry bool
	// ExtraOptions is a serialized protobuf set by Go CCL code and passed through
	// to C CCL code.
	ExtraOptions []byte
}

const (
	// DefaultTempStorageMaxSizeBytes is the default maximum budget
	// for temp storage.
	DefaultTempStorageMaxSizeBytes = 32 * 1024 * 1024 * 1024 /* 32GB */
	// DefaultInMemTempStorageMaxSizeBytes is the default maximum budget
	// for in-memory temp storages.
	DefaultInMemTempStorageMaxSizeBytes = 100 * 1024 * 1024 /* 100MB */
)

// TempStorageConfig contains the details that can be specified in the cli
// pertaining to temp storage flags, specifically --temp-dir and
// --max-disk-temp-storage.
type TempStorageConfig struct {
	// InMemory specifies whether the temporary storage will remain
	// in-memory or occupy a temporary subdirectory on-disk.
	InMemory bool
	// Path is the filepath of the temporary subdirectory created for
	// the temp storage.
	Path string
	// Mon will be used by the temp storage to register all its capacity requests.
	// It can be used to limit the disk or memory that temp storage is allowed to
	// use. If InMemory is set, than this has to be a memory monitor; otherwise it
	// has to be a disk monitor.
	Mon *mon.BytesMonitor
	// StoreIdx stores the index of the StoreSpec this TempStorageConfig will use.
	SpecIdx int
}

// ExternalIOConfig describes various configuration options pertaining
// to external storage implementations.
type ExternalIOConfig struct {
	// Disables the use of external HTTP endpoints.
	// This turns off http:// external storage as well as any custom
	// endpoints cloud storage implementations.
	DisableHTTP bool
	// TODO(yevgeniy): Support disabling of implicit credentials in cloud storage.
}

// TempStorageConfigFromEnv creates a TempStorageConfig.
// If parentDir is not specified and the specified store is in-memory,
// then the temp storage will also be in-memory.
func TempStorageConfigFromEnv(
	ctx context.Context,
	st *cluster.Settings,
	firstStore StoreSpec,
	parentDir string,
	maxSizeBytes int64,
	specIdx int,
) TempStorageConfig {
	inMem := parentDir == "" && firstStore.InMemory
	var monitor mon.BytesMonitor
	if inMem {
		monitor = mon.MakeMonitor(
			"in-mem temp storage",
			mon.MemoryResource,
			nil,             /* curCount */
			nil,             /* maxHist */
			1024*1024,       /* increment */
			maxSizeBytes/10, /* noteworthy */
			st,
		)
		monitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(maxSizeBytes))
	} else {
		monitor = mon.MakeMonitor(
			"temp disk storage",
			mon.DiskResource,
			nil,             /* curCount */
			nil,             /* maxHist */
			1024*1024,       /* increment */
			maxSizeBytes/10, /* noteworthy */
			st,
		)
		monitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(maxSizeBytes))
	}

	return TempStorageConfig{
		InMemory: inMem,
		Mon:      &monitor,
		SpecIdx:  specIdx,
	}
}

// LeaseManagerConfig holds table lease manager parameters.
type LeaseManagerConfig struct {
	// TableDescriptorLeaseDuration is the mean duration a lease will be
	// acquired for.
	TableDescriptorLeaseDuration time.Duration

	// TableDescriptorLeaseJitterFraction is the factor that we use to
	// randomly jitter the lease duration when acquiring a new lease and
	// the lease renewal timeout.
	TableDescriptorLeaseJitterFraction float64

	// DefaultTableDescriptorLeaseRenewalTimeout is the default time
	// before a lease expires when acquisition to renew the lease begins.
	TableDescriptorLeaseRenewalTimeout time.Duration
}

// NewLeaseManagerConfig initializes a LeaseManagerConfig with default values.
func NewLeaseManagerConfig() *LeaseManagerConfig {
	return &LeaseManagerConfig{
		TableDescriptorLeaseDuration:       DefaultTableDescriptorLeaseDuration,
		TableDescriptorLeaseJitterFraction: DefaultTableDescriptorLeaseJitterFraction,
		TableDescriptorLeaseRenewalTimeout: DefaultTableDescriptorLeaseRenewalTimeout,
	}
}
