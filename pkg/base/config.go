// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base

import (
	"context"
	"math"
	"math/big"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// Base config defaults.
//
// When changing these, TestDefaultRaftConfig must also be updated via -rewrite,
// and the result copied to the defaultRangeLeaseDuration comment with any
// adjustments to the surrounding reasoning.
const (
	defaultInsecure = false
	defaultUser     = username.RootUser
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

	// NB: this can't easily become a variable as the UI hard-codes it to 10s.
	// See https://github.com/cockroachdb/cockroach/issues/20310.
	DefaultMetricsSampleInterval = 10 * time.Second

	// defaultRangeLeaseRenewalFraction specifies what fraction the range lease
	// renewal duration should be of the range lease active time. For example,
	// with a value of 0.2 and a lease duration of 10 seconds, leases would be
	// eagerly renewed 8 seconds into each lease.
	//
	// A range lease extension requires 1 RTT (Raft consensus), assuming the
	// leaseholder is colocated with the Raft leader, so 3 seconds should be
	// sufficient (see NetworkTimeout). However, on user ranges, Raft consensus
	// uses the DefaultClass RPC class, and is thus subject to head-of-line
	// blocking by other RPC traffic which can cause very high latencies under
	// heavy load (several seconds).
	defaultRangeLeaseRenewalFraction = 0.5

	// livenessRenewalFraction specifies what fraction the node liveness renewal
	// duration should be of the node liveness duration. For example, with a value
	// of 0.2 and a liveness duration of 10 seconds, each node's liveness record
	// would be eagerly renewed after 8 seconds.
	//
	// A liveness record write requires 2 RTTs (RPC and Raft consensus). Assuming
	// a max RTT of 600ms (see NetworkTimeout), 3 seconds is enough for 2 RTTs
	// (2*600ms) and 1 RTO (900ms), with a 900ms buffer. The write is committed
	// 1/2 RTT before this. Liveness RPCs including Raft messages are sent via
	// SystemClass, and thus avoid head-of-line blocking by general RPC traffic.
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

	// DefaultLeaseRenewalCrossValidate is the default setting for if
	// we should validate descriptors on lease renewals.
	DefaultLeaseRenewalCrossValidate = false
)

// DefaultCertsDirectory is the default value for the cert directory flag.
var DefaultCertsDirectory = os.ExpandEnv("${HOME}/.cockroach-certs")

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
	// NetworkTimeout is the timeout used for network operations that require a
	// single network round trip. It is conservatively defined as one maximum
	// network round trip time (RTT) plus one TCP packet retransmit (RTO) with an
	// additional safety margin.
	//
	// The maximum RTT between cloud regions is roughly 400ms both in GCP
	// (asia-south2 to southamerica-west1) and AWS (af-south-1 to sa-east-1), but
	// p99 RTTs can occasionally approach 600ms.
	// https://datastudio.google.com/reporting/fc733b10-9744-4a72-a502-92290f608571/page/70YCB
	// https://www.cloudping.co/grid/p_99/timeframe/1W
	//
	// Linux has an RTT-dependant retransmission timeout (RTO) which we can
	// approximate as 1.5x RTT (smoothed RTT + 4x RTT variance), with a lower
	// bound of 200ms. It can thus be up to 900ms in the worst case.
	//
	// The NetworkTimeout is therefore set to 2 seconds: 600ms RTT plus 900ms RTO
	// plus a 500ms safety margin.
	NetworkTimeout = envutil.EnvOrDefaultDuration("COCKROACH_NETWORK_TIMEOUT", 2*time.Second)

	// DialTimeout is the timeout used when dialing a node. gRPC connections take
	// up to 3 roundtrips for the TCP + TLS handshakes. NetworkTimeout allows for
	// both a network roundtrip and a TCP retransmit, but we don't need to
	// tolerate more than 1 retransmit per connection attempt, so
	// 2 * NetworkTimeout is sufficient.
	DialTimeout = envutil.EnvOrDefaultDuration("COCKROACH_RPC_DIAL_TIMEOUT", 2*NetworkTimeout)

	// PingInterval is the interval between RPC heartbeat pings. It is set to 1
	// second in order to fail fast, but with large default timeouts to tolerate
	// high-latency multiregion clusters. The gRPC server keepalive interval is
	// also affected by this.
	PingInterval = envutil.EnvOrDefaultDuration("COCKROACH_PING_INTERVAL", time.Second)

	// defaultRangeLeaseDuration specifies the default range lease duration.
	//
	// Timers for Raft leadership election and lease expiration run in parallel.
	// Although not required, we would like to elect a leader before the lease
	// expires, such that we don't have to wait for a Raft election when we're
	// ready to acquire the lease.
	//
	// The relevant operations and default time intervals are listed below. RTTs
	// are assumed to range from 10ms to 400ms (see NetworkTimeout). Heartbeat
	// offsets refer to the duration from the last heartbeat to the node crash
	// (e.g. with a 1s heartbeat interval and 3s timeout, if a node crashes 1s
	// after heartbeating then the timeout fires after 2s of unavailability and
	// the offset is -1s).
	//
	// Raft election (fortification disabled):
	// - Heartbeat offset (0-1 heartbeat interval)                [-1.00s - 0.00s]
	// - Election timeout (random 1x-2x timeout)                  [ 2.00s - 4.00s]
	// - Election (3x RTT: prevote, vote, append)                 [ 0.03s - 1.20s]
	// Total latency                                              [ 1.03s - 5.20s]
	//
	// Expiration lease acquisition:
	// - Heartbeat offset (0-1 heartbeat interval)                [-3.00s - 0.00s]
	// - Lease expiration (constant)                              [ 6.00s - 6.00s]
	// - Lease acquisition (1x RTT: append)                       [ 0.01s - 0.40s]
	// Total latency                                              [ 3.01s - 6.40s]
	//
	// Epoch lease acquisition:
	// - Node Liveness heartbeat offset (0-1 heartbeat interval)  [-3.00s - 0.00s]
	// - Node Liveness record expiration (constant)               [ 6.00s - 6.00s]
	// - Node Liveness epoch bump (2x RTT: CPut + append)         [ 0.02s - 0.80s]
	// - Lease acquisition (1x RTT: append)                       [ 0.01s - 0.40s]
	// Total latency                                              [ 3.03s - 7.20s]
	//
	// Leader lease acquisition (including raft election):
	// - Store Liveness heartbeat offset (0-1 heartbeat interval) [-1.00s - 0.00s]
	// - Store Liveness expiration (constant)                     [ 3.00s - 3.00s]
	// - Store Liveness withdrawal (0-1 withdrawal interval)      [ 0.00s - 0.10s]
	// - Raft election timeout jitter (random 0x-1x timeout)      [ 0.00s - 2.00s]
	// - Election (3x RTT: prevote, vote, append)                 [ 0.03s - 1.20s]
	// - Lease acquisition (1x RTT: append)                       [ 0.01s - 0.40s]
	// Total latency                                              [ 2.04s - 6.70s]
	//
	// (generated by TestDefaultRaftConfig)
	//
	// From the above, we note that the worst-case Raft election latency
	// (4.03s-5.20s) is always less than the corresponding lease expiration +
	// epoch bump time (6.02s-6.80s) regardless of RTT, such that the upper bound
	// on unavailability is always given by the lease expiration time + 3x RTT
	// (6.03s to 7.20s). With negligible RTT, the average latency is 4.5s for
	// lease acquisition and 2.5s for Raft elections.
	defaultRangeLeaseDuration = envutil.EnvOrDefaultDuration(
		"COCKROACH_RANGE_LEASE_DURATION", 6*time.Second)

	// DefaultRPCHeartbeatTimeout is the default RPC heartbeat timeout. It is set
	// very high at 3 * NetworkTimeout for several reasons: the gRPC transport may
	// need to complete a dial/handshake before sending the heartbeat, the
	// heartbeat has occasionally been seen to require 3 RTTs even post-dial (for
	// unknown reasons), and under load the heartbeat may be head-of-line blocked
	// by other RPC traffic.
	//
	// High-latency experiments with 6s RPC heartbeat timeouts showed that
	// clusters running TPCC imports were stable at 400ms RTT, but started seeing
	// RPC heartbeat failures at 500ms RTT. With light load (e.g. rate-limited
	// kv50), clusters were stable at 1000ms RTT.
	//
	// The maximum p99 RPC heartbeat latency in any Cockroach Cloud cluster over a
	// 90-day period was found to be 557ms. This was a single-region US cluster
	// where the latency was caused by CPU overload.
	//
	// TODO(erikgrinaker): We should avoid head-of-line blocking for RPC
	// heartbeats and reduce this to NetworkTimeout (plus DialTimeout for the
	// initial heartbeat), see:
	// https://github.com/cockroachdb/cockroach/issues/93397.
	DefaultRPCHeartbeatTimeout = envutil.EnvOrDefaultDuration(
		"COCKROACH_RPC_HEARTBEAT_TIMEOUT", 3*NetworkTimeout)

	// defaultStoreLivenessHeartbeatInterval is the default value for
	// StoreLivenessHeartbeatInterval.
	defaultStoreLivenessHeartbeatInterval = envutil.EnvOrDefaultDuration(
		"COCKROACH_STORE_LIVENESS_HEARTBEAT_INTERVAL", time.Second)

	// defaultStoreLivenessSupportDuration is the default value for
	// StoreLivenessSupportDuration.
	defaultStoreLivenessSupportDuration = envutil.EnvOrDefaultDuration(
		"COCKROACH_STORE_LIVENESS_SUPPORT_DURATION", 3*time.Second)

	// defaultRaftTickInterval is the default resolution of the Raft timer.
	defaultRaftTickInterval = envutil.EnvOrDefaultDuration(
		"COCKROACH_RAFT_TICK_INTERVAL", 500*time.Millisecond)

	// defaultRaftHeartbeatIntervalTicks is the default value for
	// RaftHeartbeatIntervalTicks, which determines the number of ticks between
	// each heartbeat.
	defaultRaftHeartbeatIntervalTicks = envutil.EnvOrDefaultInt64(
		"COCKROACH_RAFT_HEARTBEAT_INTERVAL_TICKS", 2)

	// defaultRaftElectionTimeoutTicks specifies the minimum number of Raft ticks
	// before holding an election. The actual election timeout per replica is
	// multiplied by a random factor of 1-2, to avoid ties.
	//
	// A timeout of 2 seconds with a Raft heartbeat sent every second gives each
	// heartbeat 1 second to make it across the network. This is only half a
	// network roundtrip, and as seen in NetworkTimeout 1 second is generally
	// sufficient for a full network roundtrip. Raft heartbeats are also sent via
	// SystemClass, avoiding head-of-line blocking by general RPC traffic. The 1-2
	// random factor provides an additional buffer.
	defaultRaftElectionTimeoutTicks = envutil.EnvOrDefaultInt64(
		"COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS", 4)

	// defaultRaftReproposalTimeoutTicks is the number of ticks before reproposing
	// a Raft command.
	defaultRaftReproposalTimeoutTicks = envutil.EnvOrDefaultInt64(
		"COCKROACH_RAFT_REPROPOSAL_TIMEOUT_TICKS", 6)

	// defaultRaftEnableCheckQuorum specifies whether to enable CheckQuorum in
	// Raft. This makes a leader keep track of when it last heard from followers,
	// and automatically step down if it hasn't heard from a quorum in the last
	// election timeout. This is particularly important with asymmetric or partial
	// network partitions, where a leader can otherwise hold on to leadership via
	// heartbeats even though many/all replicas are unable to reach the leader.
	//
	// In etcd/raft, this also has the effect of fully enabling PreVote, such that
	// a follower will not grant a PreVote for a candidate if it has heard from an
	// established leader in the past election timeout. This is particularly
	// important to prevent nodes rejoining a cluster after a restart or network
	// partition from stealing away leadership from an established leader
	// (assuming they have an up-to-date log, which they do with a read-only
	// workload). With asymmetric or partial network partitions, this can allow an
	// unreachable node to steal leadership away from the leaseholder, leading to
	// range unavailability if the leaseholder can no longer reach the leader.
	//
	// The asymmetric partition concerns have largely been addressed by RPC
	// dialback (see rpc.dialback.enabled), but the partial partition concerns
	// remain.
	//
	// etcd/raft does register MsgHeartbeatResp as follower activity, and these
	// are sent across the low-latency system RPC class, so it shouldn't be
	// affected by RPC head-of-line blocking for MsgApp traffic.
	defaultRaftEnableCheckQuorum = envutil.EnvOrDefaultBool(
		"COCKROACH_RAFT_ENABLE_CHECKQUORUM", true)

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

	// defaultRaftMaxInflightBytes specifies the maximum aggregate byte size of
	// Raft log entries that a leader will send to a follower without hearing
	// responses. As such, it also bounds the amount of replication data buffered
	// in memory on the receiver. Individual messages can still exceed this limit
	// (consider the default command size limit at 64 MB).
	//
	// Normally, RaftMaxInflightMsgs * RaftMaxSizePerMsg will bound this at 4 MB
	// (128 messages at 32 KB each). However, individual messages are allowed to
	// exceed the 32 KB limit, typically large AddSSTable commands that can be
	// around 10 MB each. To prevent followers running out of memory, we place an
	// additional total byte limit of 32 MB, which is 8 times more than normal.
	// This was found to significantly reduce OOM incidence during RESTORE with
	// overloaded disks.
	//
	// Per the bandwidth-delay product, this will limit per-range throughput to
	// 64 MB/s at 500 ms RTT, 320 MB/s at 100 ms RTT, and 3.2 GB/s at 10 ms RTT.
	defaultRaftMaxInflightBytes = envutil.EnvOrDefaultBytes(
		"COCKROACH_RAFT_MAX_INFLIGHT_BYTES", 32<<20 /* 32 MB */)
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
	User username.SQLUsername

	// Addr is the address the server is listening on.
	Addr string

	// AdvertiseAddrH contains the address advertised by the server to
	// other nodes in the cluster. It should be reachable by all other
	// nodes and should route to an interface that Addr is listening on.
	//
	// It is set after the server instance has been created, when the
	// network listeners are being set up.
	AdvertiseAddrH

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

	// SQLAddrListener will only be considered if SplitListenSQL is set and
	// DisableSQLListener is not. Under these conditions, if not nil, it will be
	// used as a listener for incoming SQL connection requests. This allows
	// creating a listener early in the server initialization process and not
	// rejecting incoming connection requests that may come before the server is
	// fully ready.
	SQLAddrListener net.Listener

	// SQLAdvertiseAddrH contains the advertised SQL address.
	// This is computed from SQLAddr if specified otherwise Addr.
	//
	// It is set after the server instance has been created, when the
	// network listeners are being set up.
	SQLAdvertiseAddrH

	// SocketFile, if non-empty, sets up a TLS-free local listener using
	// a unix datagram socket at the specified path for SQL clients.
	// This is auto-populated from SQLAddr if it initially ends with '.0'.
	SocketFile string

	// HTTPAddr is the configured HTTP listen address.
	HTTPAddr string

	// DisableTLSForHTTP, if set, disables TLS for the HTTP listener.
	DisableTLSForHTTP bool

	// HTTPAdvertiseAddrH contains the advertised HTTP address.
	// This is computed from HTTPAddr if specified otherwise Addr.
	//
	// It is set after the server instance has been created, when the
	// network listeners are being set up.
	HTTPAdvertiseAddrH

	// RPCHeartbeatInterval controls how often Ping requests are sent on peer
	// connections to determine connection health and update the local view of
	// remote clocks.
	RPCHeartbeatInterval time.Duration

	// RPCHearbeatTimeout is the timeout for Ping requests.
	RPCHeartbeatTimeout time.Duration

	// ApplicationInternalRPCPortMin/PortMax define the range of TCP ports
	// used to start the internal RPC service for application-level
	// servers. This service is used for node-to-node RPC traffic and to
	// serve data for 'debug zip'.
	ApplicationInternalRPCPortMin int
	ApplicationInternalRPCPortMax int

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

	// LocalityAddresses contains private IP addresses that can only be accessed
	// in the corresponding locality.
	LocalityAddresses []roachpb.LocalityAddress

	// AcceptProxyProtocolHeaders allows CockroachDB to parse proxy protocol
	// headers, and use the client IP information contained within instead of
	// using the IP information in the source IP field of the incoming packets.
	AcceptProxyProtocolHeaders bool
}

// AdvertiseAddr is the type of the AdvertiseAddr field in Config.
type AdvertiseAddrH struct {
	AdvertiseAddr string
}

// SQLAdvertiseAddr is the type of the SQLAdvertiseAddr field in Config.
type SQLAdvertiseAddrH struct {
	SQLAdvertiseAddr string
}

// HTTPAdvertiseAddr is the type of the HTTPAdvertiseAddr field in Config.
type HTTPAdvertiseAddrH struct {
	HTTPAdvertiseAddr string
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
	cfg.User = username.MakeSQLUsernameFromPreNormalizedString(defaultUser)
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
	cfg.DisableTLSForHTTP = false
	cfg.HTTPAdvertiseAddr = ""
	cfg.SplitListenSQL = false
	cfg.SQLAddr = defaultSQLAddr
	cfg.SQLAdvertiseAddr = cfg.SQLAddr
	cfg.SocketFile = ""
	cfg.SSLCertsDir = DefaultCertsDirectory
	cfg.RPCHeartbeatInterval = PingInterval
	cfg.RPCHeartbeatTimeout = DefaultRPCHeartbeatTimeout
	cfg.ClusterName = ""
	cfg.DisableClusterNameVerification = false
	cfg.ClockDevicePath = ""
	cfg.AcceptSQLWithoutTLS = false
	cfg.ApplicationInternalRPCPortMin = 0
	cfg.ApplicationInternalRPCPortMax = 0
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

	// RaftElectionTimeoutTicks is the minimum number of raft ticks before holding
	// an election. The actual election timeout is randomized by each replica to
	// between 1-2 election timeouts. This value is inherited by individual stores
	// unless overridden.
	RaftElectionTimeoutTicks int64

	// RaftReproposalTimeoutTicks is the number of ticks before reproposing a Raft
	// command. This also specifies the number of ticks between each reproposal
	// check, so the actual timeout is 1-2 times this value.
	RaftReproposalTimeoutTicks int64

	// RaftHeartbeatIntervalTicks is the number of ticks that pass between heartbeats.
	RaftHeartbeatIntervalTicks int64

	// StoreLivenessHeartbeatInterval determines how ofter stores request and
	// extend store liveness support.
	StoreLivenessHeartbeatInterval time.Duration

	// StoreLivenessSupportDuration is the duration of store liveness support that
	// stores request and extend.
	StoreLivenessSupportDuration time.Duration

	// RangeLeaseRaftElectionTimeoutMultiplier specifies the range lease duration.
	RangeLeaseDuration time.Duration
	// RangeLeaseRenewalFraction specifies what fraction the range lease renewal
	// duration should be of the range lease active time. For example, with a
	// value of 0.2 and a lease duration of 10 seconds, leases would be eagerly
	// renewed 8 seconds into each lease. A value of zero means use the default
	// and a value of -1 means never preemptively renew the lease. A value of 1
	// means always renew.
	RangeLeaseRenewalFraction float64

	// RaftEnableCheckQuorum will enable Raft CheckQuorum, which in etcd/raft also
	// fully enables PreVote. See comment on defaultRaftEnableCheckQuorum for
	// details.
	RaftEnableCheckQuorum bool

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
	//
	// Used in combination with RaftMaxInflightMsgs and RaftMaxInflightBytes.
	RaftMaxSizePerMsg uint64

	// RaftMaxCommittedSizePerReady controls the maximum aggregate byte size of
	// committed Raft log entries a replica will receive in a single Ready.
	RaftMaxCommittedSizePerReady uint64

	// RaftMaxInflightMsgs controls how many "inflight" MsgApps Raft will send to
	// a follower without hearing a response. The total size of inflight Raft log
	// entries is thus roughly limited by RaftMaxInflightMsgs * RaftMaxSizePerMsg,
	// but also by RaftMaxInflightBytes. The current default settings provide for
	// up to 4 MB of Raft log to be sent without acknowledgement. With an average
	// entry size of 1 KB that translates to ~4096 commands that might be executed
	// in the handling of a single raft.Ready operation.
	//
	// This setting is used both by sending and receiving end of Raft messages. To
	// minimize dropped messages on the receiver, its size should at least match
	// the sender's (being it the default size, or taken from the env variables).
	RaftMaxInflightMsgs int

	// RaftMaxInflightBytes controls the maximum aggregate byte size of Raft log
	// entries that a leader will send to a follower without hearing responses.
	// As such, it also bounds the amount of replication data buffered in memory
	// on the receiver.
	//
	// Normally RaftMaxSizePerMsg * RaftMaxInflightMsgs is the actual limit. But
	// the RaftMaxSizePerMsg is soft, and Raft may send individual messages
	// arbitrarily larger than that (e.g. with a large write, or AddSST command),
	// so it's possible that the overall limit is exceeded by a large multiple.
	// RaftMaxInflightBytes is a stricter limit which can only be slightly
	// exceeded (by a single message).
	//
	// This effectively bounds the bandwidth-delay product. Note that especially
	// in high-latency deployments setting this too low can lead to a dramatic
	// reduction in throughput. For example, with a peer that has a round-trip
	// latency of 100ms to the leader and this setting is set to 1 MB, there is a
	// throughput limit of 10 MB/s for this group. With RTT of 400ms, this drops
	// to 2.5 MB/s. See Little's law to understand the maths behind.
	RaftMaxInflightBytes uint64

	// Splitting a range which has a replica needing a snapshot results in two
	// ranges in that state. The delay configured here slows down splits when in
	// that situation (limiting to those splits not run through the split
	// queue). The most important target here are the splits performed by
	// backup/restore.
	//
	// -1 to disable.
	RaftDelaySplitToSuppressSnapshot time.Duration
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
	if cfg.StoreLivenessHeartbeatInterval == 0 {
		cfg.StoreLivenessHeartbeatInterval = defaultStoreLivenessHeartbeatInterval
	}
	if cfg.StoreLivenessSupportDuration == 0 {
		cfg.StoreLivenessSupportDuration = defaultStoreLivenessSupportDuration
	}
	if cfg.RangeLeaseDuration == 0 {
		cfg.RangeLeaseDuration = defaultRangeLeaseDuration
	}
	if cfg.RangeLeaseRenewalFraction == 0 {
		cfg.RangeLeaseRenewalFraction = defaultRangeLeaseRenewalFraction
	}
	if cfg.RaftReproposalTimeoutTicks == 0 {
		cfg.RaftReproposalTimeoutTicks = defaultRaftReproposalTimeoutTicks
	}
	// TODO(andrei): -1 is a special value for RangeLeaseRenewalFraction which
	// really means "0" (never renew), except that the zero value means "use
	// default". We can't turn the -1 into 0 here because, unfortunately,
	// SetDefaults is called multiple times (see NewStore()). So, we leave -1
	// alone and ask all the users to handle it specially.

	if !cfg.RaftEnableCheckQuorum {
		cfg.RaftEnableCheckQuorum = defaultRaftEnableCheckQuorum
	}
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

	if cfg.RaftMaxInflightBytes == 0 {
		cfg.RaftMaxInflightBytes = uint64(defaultRaftMaxInflightBytes)
	}
	// Fixup RaftMaxInflightBytes if it is lower than reasonable.
	if other := maxInflightBytesFrom(
		cfg.RaftMaxInflightMsgs, cfg.RaftMaxSizePerMsg,
	); cfg.RaftMaxInflightBytes < other {
		cfg.RaftMaxInflightBytes = other
	}

	if cfg.RaftDelaySplitToSuppressSnapshot == 0 {
		// Use a generous delay to make sure even a backed up Raft snapshot queue is
		// going to make progress when a (not overly concurrent) amount of splits
		// happens. The generous amount should result in a delay sufficient to
		// transmit at least one snapshot with the slow delay, which with default
		// settings is max 512MB at 32MB/s, ie 16 seconds.
		cfg.RaftDelaySplitToSuppressSnapshot = 45 * time.Second
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
// renewal.
func (cfg RaftConfig) RangeLeaseDurations() (time.Duration, time.Duration) {
	return cfg.RangeLeaseDuration, cfg.RangeLeaseRenewalDuration()
}

// RangeLeaseRenewalDuration specifies a time interval at the end of the
// active lease interval (i.e. bounded to the right by the start of the stasis
// period) during which operations will trigger an asynchronous renewal of the
// lease.
func (cfg RaftConfig) RangeLeaseRenewalDuration() time.Duration {
	if cfg.RangeLeaseRenewalFraction == -1 {
		return 0
	}
	return time.Duration(cfg.RangeLeaseRenewalFraction * float64(cfg.RangeLeaseDuration))
}

// RangeLeaseAcquireTimeout is the timeout for lease acquisition.
func (cfg RaftConfig) RangeLeaseAcquireTimeout() time.Duration {
	// The Raft election timeout is randomized by a factor of 1-2x per replica
	// (the first one will trigger the election), and reproposing the lease
	// acquisition command can take up to 1 Raft election timeout. On average, we
	// should be able to elect a leader and acquire a lease within 2 election
	// timeouts, assuming negligible RTT; otherwise, lease acquisition will
	// typically be retried, only adding a bit of tail latency.
	return 2 * cfg.RaftElectionTimeout()
}

// NodeLivenessDurations computes durations for node liveness expiration and
// renewal based on a default multiple of Raft election timeout.
func (cfg RaftConfig) NodeLivenessDurations() (livenessActive, livenessRenewal time.Duration) {
	livenessActive = cfg.RangeLeaseDuration
	livenessRenewal = time.Duration(float64(livenessActive) * livenessRenewalFraction)
	return
}

// StoreLivenessDurations computes durations for store liveness heartbeat
// interval and support duration.
func (cfg RaftConfig) StoreLivenessDurations() (supportDuration, heartbeatInterval time.Duration) {
	return cfg.StoreLivenessSupportDuration, cfg.StoreLivenessHeartbeatInterval
}

// SentinelGossipTTL is time-to-live for the gossip sentinel, which is gossiped
// by the leaseholder of the first range. The sentinel informs a node whether or
// not it is connected to the primary gossip network and not just a partition.
// As such it must expire fairly quickly and be continually re-gossiped as a
// connected gossip network is necessary to propagate liveness. Notably, it must
// expire faster than the liveness records carried by the gossip network so that
// a gossip partition is detected and healed before that liveness information
// expires. Failure to do so can result in false positive dead node detection,
// which can show up as false positive range unavailability in metrics.
func (cfg RaftConfig) SentinelGossipTTL() time.Duration {
	return cfg.RangeLeaseDuration / 2
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

// maxInflightBytesFrom returns the minimal value for RaftMaxInflightBytes
// config option based on RaftMaxInflightMsgs and RaftMaxSizePerMsg.
func maxInflightBytesFrom(maxInflightMsgs int, maxSizePerMsg uint64) uint64 {
	// Compute min(maxInflightMsgs * maxSizePerMsg, MaxUint64) safely.
	if mul := new(big.Int).Mul(
		big.NewInt(int64(maxInflightMsgs)),
		new(big.Int).SetUint64(maxSizePerMsg),
	); mul.IsUint64() {
		return mul.Uint64()
	}
	return math.MaxUint64
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
	// Settings stores the cluster.Settings this TempStoreConfig will use. Must
	// not be nil.
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

	// EnableNonAdminImplicitAndArbitraryOutbound removes the usual restriction to
	// only admin users placed on usage of node-granted access, such as to the
	// implicit auth via the machine account for the node or to arbitrary network
	// addresses (which are accessed from the node and might otherwise not be
	// reachable). Instead, all users can use implicit auth, http addresses and
	// configure custom endpoints. This should only be used if all users with SQL
	// access should have access to anything the node has access to.
	EnableNonAdminImplicitAndArbitraryOutbound bool
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
	return newTempStorageConfig(ctx, st, inMem, useStore, maxSizeBytes)
}

// InheritTempStorageConfig creates a new TempStorageConfig using the
// configuration of the given TempStorageConfig. It assumes the given
// TempStorageConfig has been fully initialized.
func InheritTempStorageConfig(
	ctx context.Context, st *cluster.Settings, parentConfig TempStorageConfig,
) TempStorageConfig {
	return newTempStorageConfig(ctx, st, parentConfig.InMemory, parentConfig.Spec, parentConfig.Mon.Limit())
}

func newTempStorageConfig(
	ctx context.Context, st *cluster.Settings, inMemory bool, useStore StoreSpec, maxSizeBytes int64,
) TempStorageConfig {
	var monitorName mon.MonitorName
	if inMemory {
		monitorName = mon.MakeMonitorName("in-mem temp storage")
	} else {
		monitorName = mon.MakeMonitorName("temp disk storage")
	}
	monitor := mon.NewMonitor(mon.Options{
		Name:      monitorName,
		Res:       mon.DiskResource,
		Increment: 1024 * 1024,
		Settings:  st,
	})
	monitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(maxSizeBytes))
	return TempStorageConfig{
		InMemory: inMemory,
		Mon:      monitor,
		Spec:     useStore,
		Settings: st,
	}
}
