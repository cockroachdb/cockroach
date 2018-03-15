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

package base

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"

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
	DefaultPort = "26257"

	// The default port for HTTP-for-humans.
	DefaultHTTPPort = "8080"

	// NB: net.JoinHostPort is not a constant.
	defaultAddr     = ":" + DefaultPort
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
	DefaultTableDescriptorLeaseJitterFraction = 0.25

	// DefaultTableDescriptorLeaseRenewalTimeout is the default time
	// before a lease expires when acquisition to renew the lease begins.
	DefaultTableDescriptorLeaseRenewalTimeout = time.Minute
)

var defaultRaftElectionTimeoutTicks = envutil.EnvOrDefaultInt(
	"COCKROACH_RAFT_ELECTION_TIMEOUT_TICKS", 15)

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

	// HTTPAddr is server's public HTTP address.
	//
	// This is temporary, and will be removed when grpc.(*Server).ServeHTTP
	// performance problems are addressed upstream.
	//
	// See https://github.com/grpc/grpc-go/issues/586.
	HTTPAddr string

	// The certificate manager. Must be accessed through GetCertificateManager.
	certificateManager lazyCertificateManager

	// httpClient uses the client TLS config. It is initialized lazily.
	httpClient lazyHTTPClient

	// HistogramWindowInterval is used to determine the approximate length of time
	// that individual samples are retained in in-memory histograms. Currently,
	// it is set to the arbitrary length of six times the Metrics sample interval.
	// See the comment in server.Config for more details.
	HistogramWindowInterval time.Duration
}

func didYouMeanInsecureError(err error) error {
	return errors.Wrap(err, "problem using security settings, did you mean to use --insecure?")
}

// InitDefaults sets up the default values for a config.
// This is also used in tests to reset global objects.
func (cfg *Config) InitDefaults() {
	cfg.Insecure = defaultInsecure
	cfg.User = defaultUser
	cfg.Addr = defaultAddr
	cfg.AdvertiseAddr = cfg.Addr
	cfg.HTTPAddr = defaultHTTPAddr
	cfg.SSLCertsDir = DefaultCertsDirectory
	cfg.certificateManager = lazyCertificateManager{}
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
		Host:   cfg.HTTPAddr,
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
		options.Add("sslmode", "disable")
	} else {
		// Fetch CA cert. This is required.
		caCertPath, err := cfg.GetCACertPath()
		if err != nil {
			return didYouMeanInsecureError(err)
		}
		options.Add("sslmode", "verify-full")
		options.Add("sslrootcert", caCertPath)

		// Fetch certs, but don't fail, we may be using a password.
		certPath, keyPath, err := cfg.GetClientCertPaths(username)
		if err == nil {
			options.Add("sslcert", certPath)
			options.Add("sslkey", keyPath)
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
		Host:     cfg.AdvertiseAddr,
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
func (cfg *Config) GetClientTLSConfig() (*tls.Config, error) {
	// Early out.
	if cfg.Insecure {
		return nil, nil
	}

	cm, err := cfg.GetCertificateManager()
	if err != nil {
		return nil, didYouMeanInsecureError(err)
	}

	tlsCfg, err := cm.GetClientTLSConfig(cfg.User)
	if err != nil {
		return nil, didYouMeanInsecureError(err)
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
		return nil, didYouMeanInsecureError(err)
	}

	tlsCfg, err := cm.GetServerTLSConfig()
	if err != nil {
		return nil, didYouMeanInsecureError(err)
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
		transport.TLSClientConfig, cfg.httpClient.err = cfg.GetClientTLSConfig()
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
			64*1024*1024,    /* increment */
			maxSizeBytes/10, /* noteworthy */
			st,
		)
		monitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(maxSizeBytes))
	}

	return TempStorageConfig{
		InMemory: inMem,
		Mon:      &monitor,
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
