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
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
	defaultSQLAddr  = ":" + DefaultPort
	defaultHTTPAddr = ":" + DefaultHTTPPort

	// NetworkTimeout is the timeout used for network operations.
	NetworkTimeout = 3 * time.Second

	// DefaultCertsDirectory is the default value for the cert directory flag.
	DefaultCertsDirectory = "${HOME}/.cockroach-certs"

	// defaultRaftTickInterval is the default resolution of the Raft timer.
	defaultRaftTickInterval = 200 * time.Millisecond

	// defaultRangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the
	// leader lease active duration should be of the raft election timeout.
	defaultRangeLeaseRaftElectionTimeoutMultiplier = 3

	// NB: this can't easily become a variable as the UI hard-codes it to 10s.
	// See https://github.com/cockroachdb/cockroach/issues/20310.
	DefaultMetricsSampleInterval = 10 * time.Second

	// defaultRaftHeartbeatIntervalTicks is the default value for
	// RaftHeartbeatIntervalTicks, which determines the number of ticks between
	// each heartbeat.
	defaultRaftHeartbeatIntervalTicks = 5

	// defaultRPCHeartbeatInterval is the default value of RPCHeartbeatInterval
	// used by the rpc context.
	defaultRPCHeartbeatInterval = 3 * time.Second

	// defaultRangeLeaseRenewalFraction specifies what fraction the range lease
	// renewal duration should be of the range lease active time. For example,
	// with a value of 0.2 and a lease duration of 10 seconds, leases would be
	// eagerly renewed 8 seconds into each lease.
	defaultRangeLeaseRenewalFraction = 0.5

	// livenessRenewalFraction specifies what fraction the node liveness
	// renewal duration should be of the node liveness duration. For example,
	// with a value of 0.2 and a liveness duration of 10 seconds, each node's
	// liveness record would be eagerly renewed after 8 seconds.
	livenessRenewalFraction = 0.5

	// DefaultDescriptorLeaseDuration is the default mean duration a
	// lease will be acquired for. The actual duration is jittered using
	// the jitter fraction. Jittering is done to prevent multiple leases
	// from being renewed simultaneously if they were all acquired
	// simultaneously.
	DefaultDescriptorLeaseDuration = 5 * time.Minute

	// DefaultDescriptorLeaseJitterFraction is the default factor
	// that we use to randomly jitter the lease duration when acquiring a
	// new lease and the lease renewal timeout.
	DefaultDescriptorLeaseJitterFraction = 0.05

	// DefaultDescriptorLeaseRenewalTimeout is the default time
	// before a lease expires when acquisition to renew the lease begins.
	DefaultDescriptorLeaseRenewalTimeout = time.Minute
)

// DefaultHistogramWindowInterval returns the default rotation window for
// histograms.
func DefaultHistogramWindowInterval() time.Duration {
	const defHWI = 6 * DefaultMetricsSampleInterval

	// Rudimentary overflow detection; this can result if
	// DefaultMetricsSampleInterval is set to an extremely large number, likely
	// in the context of a test or an intentional attempt to disable metrics
	// collection. Just return the default in this case.
	if defHWI < DefaultMetricsSampleInterval {
		return DefaultMetricsSampleInterval
	}
	return defHWI
}

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
		"COCKROACH_RAFT_LOG_TRUNCATION_THRESHOLD", 16<<20 /* 16 MB */)

	// defaultRaftMaxSizePerMsg specifies the maximum aggregate byte size of Raft
	// log entries that a leader will send to followers in a single MsgApp.
	defaultRaftMaxSizePerMsg = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_MAX_SIZE_PER_MSG", 32<<10 /* 32 KB */)

	// defaultRaftMaxSizeCommittedSizePerReady specifies the maximum aggregate
	// byte size of the committed log entries which a node will receive in a
	// single Ready.
	defaultRaftMaxCommittedSizePerReady = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_MAX_COMMITTED_SIZE_PER_READY", 64<<20 /* 64 MB */)

	// defaultRaftMaxInflightMsgs specifies how many "inflight" MsgApps a leader
	// will send to a given follower without hearing a response.
	defaultRaftMaxInflightMsgs = envutil.EnvOrDefaultInt(
		"COCKROACH_RAFT_MAX_INFLIGHT_MSGS", 128)
)

// Config is embedded by server.Config. A base config is not meant to be used
// directly, but embedding configs should call cfg.InitDefaults().
type Config struct {
	// Insecure specifies whether to disable security checks throughout
	// the code base.
	// This is really not recommended.
	// See: https://github.com/cockroachdb/cockroach/issues/53404
	Insecure bool

	// AcceptSQLWithoutTLS, when set, makes it possible for SQL
	// clients to authenticate without TLS on a secure cluster.
	//
	// Authentication is, as usual, subject to the HBA configuration: in
	// the default case, password authentication is still mandatory.
	AcceptSQLWithoutTLS bool

	// SSLCAKey is used to sign new certs.
	SSLCAKey string
	// SSLCertsDir is the path to the certificate/key directory.
	SSLCertsDir string

	// User running this process. It could be the user under which
	// the server is running or the user passed in client calls.
	User security.SQLUsername

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

	// DisableTLSForHTTP, if set, disables TLS for the HTTP listener.
	DisableTLSForHTTP bool

	// HTTPAdvertiseAddr is the advertised HTTP address.
	// This is computed from HTTPAddr if specified otherwise Addr.
	HTTPAdvertiseAddr string

	// RPCHeartbeatInterval controls how often a Ping request is sent on peer
	// connections to determine connection health and update the local view
	// of remote clocks.
	RPCHeartbeatInterval time.Duration

	// Enables the use of an PTP hardware clock user space API for HLC current time.
	// This contains the path to the device to be used (i.e. /dev/ptp0)
	ClockDevicePath string

	// AutoInitializeCluster, if set, causes the server to bootstrap the
	// cluster. Note that if two nodes are started with this flag set
	// and also configured to join each other, each node will bootstrap
	// its own unique cluster and the join will fail.
	//
	// The flag exists mostly for the benefit of tests, and for
	// `cockroach start-single-node`.
	AutoInitializeCluster bool

	// IdleExistAfter, If nonzero, will cause the server to run normally for the
	// indicated amount of time, wait for all SQL connections to terminate,
	// start a `defaultCountdownDuration` countdown and exit upon countdown
	// reaching zero if no new connections occur. New connections will be
	// accepted at all times and will effectively delay the exit (indefinitely
	// if there is always at least one connection or there are no connection
	// for less than `defaultCountdownDuration`. A new `defaultCountdownDuration`
	// countdown will start when no more SQL connections exist.
	// The interval is specified with a suffix of 's' for seconds, 'm' for
	// minutes, and 'h' for hours.
	IdleExitAfter time.Duration
}

// HistogramWindowInterval is used to determine the approximate length of time
// that individual samples are retained in in-memory histograms. Currently,
// it is set to the arbitrary length of six times the Metrics sample interval.
//
// The length of the window must be longer than the sampling interval due to
// issue #12998, which was causing histograms to return zero values when sampled
// because all samples had been evicted.
//
// Note that this is only intended to be a temporary fix for the above issue,
// as our current handling of metric histograms have numerous additional
// problems. These are tracked in github issue #7896, which has been given
// a relatively high priority in light of recent confusion around histogram
// metrics. For more information on the issues underlying our histogram system
// and the proposed fixes, please see issue #7896.
func (*Config) HistogramWindowInterval() time.Duration {
	return DefaultHistogramWindowInterval()
}

// InitDefaults sets up the default values for a config.
// This is also used in tests to reset global objects.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.User = security.MakeSQLUsernameFromPreNormalizedString(defaultUser)
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
	cfg.DisableTLSForHTTP = false
	cfg.HTTPAdvertiseAddr = ""
	cfg.SplitListenSQL = false
	cfg.SQLAddr = defaultSQLAddr
	cfg.SQLAdvertiseAddr = cfg.SQLAddr
	cfg.SSLCertsDir = DefaultCertsDirectory
	cfg.RPCHeartbeatInterval = defaultRPCHeartbeatInterval
	cfg.ClusterName = ""
	cfg.DisableClusterNameVerification = false
	cfg.ClockDevicePath = ""
	cfg.AcceptSQLWithoutTLS = false
}

// HTTPRequestScheme returns "http" or "https" based on the value of
// Insecure and DisableTLSForHTTP.
func (cfg *Config) HTTPRequestScheme() string {
	if cfg.Insecure || cfg.DisableTLSForHTTP {
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

// RaftConfig holds raft tuning parameters.
type RaftConfig struct {
	// RaftTickInterval is the resolution of the Raft timer.
	RaftTickInterval time.Duration

	// RaftElectionTimeoutTicks is the number of raft ticks before the
	// previous election expires. This value is inherited by individual stores
	// unless overridden.
	RaftElectionTimeoutTicks int

	// RaftHeartbeatIntervalTicks is the number of ticks that pass between heartbeats.
	RaftHeartbeatIntervalTicks int

	// RangeLeaseRaftElectionTimeoutMultiplier specifies what multiple the leader
	// lease active duration should be of the raft election timeout.
	RangeLeaseRaftElectionTimeoutMultiplier float64
	// RangeLeaseRenewalFraction specifies what fraction the range lease renewal
	// duration should be of the range lease active time. For example, with a
	// value of 0.2 and a lease duration of 10 seconds, leases would be eagerly
	// renewed 8 seconds into each lease. A value of zero means use the default
	// and a value of -1 means never pre-emptively renew the lease. A value of 1
	// means always renew.
	RangeLeaseRenewalFraction float64

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
	// entries the leader will send to followers in a single MsgApp. Smaller
	// value lowers the raft recovery cost (during initial probing and after
	// message loss during normal operation). On the other hand, it limits the
	// throughput during normal replication.
	RaftMaxSizePerMsg uint64

	// RaftMaxCommittedSizePerReady controls the maximum aggregate byte size of
	// committed Raft log entries a replica will receive in a single Ready.
	RaftMaxCommittedSizePerReady uint64

	// RaftMaxInflightMsgs controls how many "inflight" MsgApps Raft will send
	// to a follower without hearing a response. The total number of Raft log
	// entries is a combination of this setting and RaftMaxSizePerMsg. The
	// current default settings provide for up to 4 MB of raft log to be sent
	// without acknowledgement. With an average entry size of 1 KB that
	// translates to ~4096 commands that might be executed in the handling of a
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
	if cfg.RaftHeartbeatIntervalTicks == 0 {
		cfg.RaftHeartbeatIntervalTicks = defaultRaftHeartbeatIntervalTicks
	}
	if cfg.RangeLeaseRaftElectionTimeoutMultiplier == 0 {
		cfg.RangeLeaseRaftElectionTimeoutMultiplier = defaultRangeLeaseRaftElectionTimeoutMultiplier
	}
	if cfg.RangeLeaseRenewalFraction == 0 {
		cfg.RangeLeaseRenewalFraction = defaultRangeLeaseRenewalFraction
	}
	// TODO(andrei): -1 is a special value for RangeLeaseRenewalFraction which
	// really means "0" (never renew), except that the zero value means "use
	// default". We can't turn the -1 into 0 here because, unfortunately,
	// SetDefaults is called multiple times (see NewStore()). So, we leave -1
	// alone and ask all the users to handle it specially.

	if cfg.RaftLogTruncationThreshold == 0 {
		cfg.RaftLogTruncationThreshold = defaultRaftLogTruncationThreshold
	}
	if cfg.RaftProposalQuota == 0 {
		// By default, set this to a fraction of RaftLogMaxSize. See the comment
		// on the field for the tradeoffs of setting this higher or lower.
		cfg.RaftProposalQuota = cfg.RaftLogTruncationThreshold / 2
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

	// Minor validation to ensure sane tuning.
	if cfg.RaftProposalQuota > int64(cfg.RaftMaxUncommittedEntriesSize) {
		panic("raft proposal quota should not be above max uncommitted entries size")
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
	renewalFraction := cfg.RangeLeaseRenewalFraction
	if renewalFraction == -1 {
		renewalFraction = 0
	}
	rangeLeaseRenewal = time.Duration(float64(rangeLeaseActive) * renewalFraction)
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
	// EncryptionOptions is a serialized protobuf set by Go CCL code and passed
	// through to C CCL code to set up encryption-at-rest.  Must be set if and
	// only if encryption is enabled, otherwise left empty.
	EncryptionOptions []byte
}

// IsEncrypted returns whether the StorageConfig has encryption enabled.
func (sc StorageConfig) IsEncrypted() bool {
	return len(sc.EncryptionOptions) > 0
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
	// Spec stores the StoreSpec this TempStorageConfig will use.
	Spec StoreSpec
	// Settings stores the cluster.Settings this TempStoreConfig will use.
	Settings *cluster.Settings
}

// ExternalIODirConfig describes various configuration options pertaining
// to external storage implementations.
// TODO(adityamaru): Rename ExternalIODirConfig to ExternalIOConfig because it
// is now used to configure both ExternalStorage and KMS.
type ExternalIODirConfig struct {
	// Disables the use of external HTTP endpoints.
	// This turns off http:// external storage as well as any custom
	// endpoints cloud storage implementations.
	DisableHTTP bool
	// Disables the use of implicit credentials when accessing external services.
	// Implicit credentials are obtained from the system environment.
	// This turns off implicit credentials, and requires the user to provide
	// necessary access keys.
	DisableImplicitCredentials bool

	// DisableOutbound disables the use of any external-io that dials out such as
	// to s3, gcs, or even `nodelocal` as it may need to dial another node.
	DisableOutbound bool
}

// TempStorageConfigFromEnv creates a TempStorageConfig.
// If parentDir is not specified and the specified store is in-memory,
// then the temp storage will also be in-memory.
func TempStorageConfigFromEnv(
	ctx context.Context,
	st *cluster.Settings,
	useStore StoreSpec,
	parentDir string,
	maxSizeBytes int64,
) TempStorageConfig {
	inMem := parentDir == "" && useStore.InMemory
	var monitorName string
	if inMem {
		monitorName = "in-mem temp storage"
	} else {
		monitorName = "temp disk storage"
	}
	monitor := mon.NewMonitor(
		monitorName,
		mon.DiskResource,
		nil,             /* curCount */
		nil,             /* maxHist */
		1024*1024,       /* increment */
		maxSizeBytes/10, /* noteworthy */
		st,
	)
	monitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(maxSizeBytes))
	return TempStorageConfig{
		InMemory: inMem,
		Mon:      monitor,
		Spec:     useStore,
		Settings: st,
	}
}
