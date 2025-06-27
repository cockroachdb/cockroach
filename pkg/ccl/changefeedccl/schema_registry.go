// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const confluentSchemaContentType = `application/vnd.schemaregistry.v1+json`

type schemaRegistry interface {
	// Ping tests the connectivity to the schema registry. A nil
	// error is returned if the schema registry appears to be
	// available.
	Ping(ctx context.Context) error

	// RegisterSchemaForSubject registers the given schema for the
	// given subject. The returned int32 is a schema ID that can
	// be used in Avro wire messages or in other calls to the
	// schema registry.
	RegisterSchemaForSubject(ctx context.Context, subject string, schema string) (int32, error)
}

type confluentSchemaVersionRequest struct {
	Schema string `json:"schema"`
}

type confluentSchemaVersionResponse struct {
	ID int32 `json:"id"`
}

type confluentSchemaRegistry struct {
	baseURL *url.URL
	// The current defaults for httputil.Client sets
	// DisableKeepAlive's true so we don't have persistent
	// connections to clean up on teardown.
	client     *httputil.Client
	retryOpts  retry.Options
	sliMetrics *sliMetrics
}

var _ schemaRegistry = (*confluentSchemaRegistry)(nil)

type schemaRegistryParams struct {
	params  map[string][]byte
	timeout time.Duration
}

func (s schemaRegistryParams) caCert() []byte {
	return s.params[changefeedbase.RegistryParamCACert]
}

func (s schemaRegistryParams) clientCert() []byte {
	return s.params[changefeedbase.RegistryParamClientCert]
}

func (s schemaRegistryParams) clientKey() []byte {
	return s.params[changefeedbase.RegistryParamClientKey]
}

const timeoutParam = "timeout"
const defaultSchemaRegistryTimeout = 30 * time.Second

func getAndDeleteParams(u *url.URL) (*schemaRegistryParams, error) {
	query := u.Query()
	s := schemaRegistryParams{params: make(map[string][]byte, 3)}
	for _, k := range []string{
		changefeedbase.RegistryParamCACert,
		changefeedbase.RegistryParamClientCert,
		changefeedbase.RegistryParamClientKey} {
		if stringParam := query.Get(k); stringParam != "" {
			var decoded []byte
			err := changefeedbase.DecodeBase64FromString(stringParam, &decoded)
			if err != nil {
				return nil, errors.Wrapf(err, "param %s must be base 64 encoded", k)
			}
			s.params[k] = decoded
			query.Del(k)
		}
	}

	if strTimeout := query.Get(timeoutParam); strTimeout != "" {
		dur, err := time.ParseDuration(strTimeout)
		if err != nil {
			return nil, err
		}
		s.timeout = dur
	} else {
		// Default timeout in httputil is way too low. Use something more reasonable.
		s.timeout = defaultSchemaRegistryTimeout
	}

	// remove crdb query params to ensure compatibility with schema
	// registry implementation
	u.RawQuery = query.Encode()
	return &s, nil
}

func newConfluentSchemaRegistry(
	baseURL string, p externalConnectionProvider, sliMetrics *sliMetrics,
) (schemaRegistry, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrap(err, "malformed schema registry url")
	}

	if u.Scheme == changefeedbase.SinkSchemeExternalConnection {
		actual, err := p.lookup(u.Host)
		if err != nil {
			return nil, err
		}
		return newConfluentSchemaRegistry(actual, p, sliMetrics)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.Errorf("unsupported scheme: %q", u.Scheme)
	}

	var src *schemaRegistryCache
	var ok bool
	func() {
		schemaRegistrySingletons.mu.Lock()
		defer schemaRegistrySingletons.mu.Unlock()
		src, ok = schemaRegistrySingletons.cachePerEndpoint[baseURL]
		if !ok {
			src = &schemaRegistryCache{entries: cache.NewUnorderedCache(
				cache.Config{Policy: cache.CacheLRU, ShouldEvict: func(size int, _, _ interface{}) bool {
					return size > 1023
				}}),
			}
			schemaRegistrySingletons.cachePerEndpoint[baseURL] = src
		}
	}()

	s, err := getAndDeleteParams(u)
	if err != nil {
		return nil, err
	}

	httpClient, err := setupHTTPClient(u, s)
	if err != nil {
		return nil, err
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 5
	reg := schemaRegistryWithCache{
		base: &confluentSchemaRegistry{
			baseURL:    u,
			client:     httpClient,
			retryOpts:  retryOpts,
			sliMetrics: sliMetrics,
		},
		cache: src,
	}
	return &reg, nil
}

// Setup the httputil.Client to use when dialing Confluent schema registry. If `ca_cert`
// is set as a query param in the registry URL, client should trust the corresponding
// cert while dialing. Otherwise, use the DefaultClient.
func setupHTTPClient(baseURL *url.URL, s *schemaRegistryParams) (*httputil.Client, error) {
	if len(s.params) == 0 {
		return httputil.NewClientWithTimeout(s.timeout), nil
	}

	httpClient, err := newClientFromTLSKeyPair(s.caCert(), s.clientCert(), s.clientKey())
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = s.timeout

	if baseURL.Scheme == "http" {
		log.Warningf(context.Background(), "TLS configuration provided but schema registry %s uses HTTP", baseURL)
	}
	return httpClient, nil
}

// Ping checks connectivity to the schema registry using the /mode
// endpoint. Note that we only return an error if there was an error
// making a request or if the server returns a response in the 500-599
// range. We consider 404s and other errors as success to avoid
// failing for schema registries that don't implement the /mode
// endoint.
func (r *confluentSchemaRegistry) Ping(ctx context.Context) error {
	u := r.urlForPath("mode")
	return r.doWithRetry(ctx, func() error {
		resp, err := r.client.Get(ctx, u)
		if err != nil {
			return err
		}
		defer gracefulClose(ctx, resp.Body)
		// We allow other non-Success statuses because we
		// don't care about the response here, only that the
		// service is up.
		if resp.StatusCode >= 500 {
			return errors.Errorf("unexpected schema registry response: %s", resp.Status)
		}
		return nil
	})
}

// RegisterSchemaForSubject registers the given schema for the given
// subject. The schema type is assumed to be AVRO.
//
//	https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
func (r *confluentSchemaRegistry) RegisterSchemaForSubject(
	ctx context.Context, subject string, schema string,
) (int32, error) {
	u := r.urlForPath(fmt.Sprintf("subjects/%s/versions", subject))
	if log.V(1) {
		log.Infof(ctx, "registering avro schema %s %s", u, schema)
	}

	req := confluentSchemaVersionRequest{Schema: schema}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(req); err != nil {
		return 0, err
	}

	var id int32
	err := r.doWithRetry(ctx, func() (e error) {
		resp, err := r.client.Post(ctx, u, confluentSchemaContentType, bytes.NewReader(buf.Bytes()))
		if err != nil {
			return errors.Wrap(err, "contacting confluent schema registry")
		}
		defer gracefulClose(ctx, resp.Body)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			return errors.Errorf("registering schema to %s %s: %s", u, resp.Status, body)
		}
		var res confluentSchemaVersionResponse
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return errors.Wrap(err, "decoding confluent schema registry reply")
		}
		id = res.ID
		return nil
	})
	if err != nil {
		return 0, err
	}
	if r.sliMetrics != nil {
		r.sliMetrics.SchemaRegistrations.Inc(1)
	}
	return id, nil
}

func (r *confluentSchemaRegistry) doWithRetry(ctx context.Context, fn func() error) error {
	// Since network services are often a source of flakes, add a few retries here
	// before we give up and return an error that will bubble up and tear down the
	// entire changefeed, though that error is marked as retryable so that the job
	// itself can attempt to start the changefeed again. TODO(dt): If the registry
	// is down or constantly returning errors, we can't make progress. Continuing
	// to indicate that we're "running" in this case can be misleading, as we
	// really aren't anymore. Right now the MO in CDC is try and try again
	// forever, so doing so here is consistent with the behavior elsewhere, but we
	// should revisit this more broadly as this pattern can easily mask real,
	// actionable issues in the operator's environment that which they might be
	// able to resolve if we made them visible in a failure instead.
	var err error
	for retrier := retry.StartWithCtx(ctx, r.retryOpts); retrier.Next(); {
		err = fn()
		if err == nil {
			return nil
		}
		if r.sliMetrics != nil {
			r.sliMetrics.SchemaRegistryRetries.Inc(1)
		}
		log.VInfof(ctx, 1, "retrying schema registry operation: %s", err.Error())
	}
	return changefeedbase.MarkRetryableError(err)
}

func gracefulClose(ctx context.Context, toClose io.ReadCloser) {
	// NOTE(ssd): To reuse the connection we have to be sure to
	// read to EOF and close the response body.
	//
	// Right now this is wasted worked, since currently, httputil
	// sets options that will mean we never actually re-use
	// connections.
	//
	// We read upto 4k to try to reach io.EOF.
	const respExtraReadLimit = 4096
	_, _ = io.CopyN(io.Discard, toClose, respExtraReadLimit)
	if err := toClose.Close(); err != nil {
		log.VInfof(ctx, 2, "failure to close schema registry connection", err)
	}
}

func (r *confluentSchemaRegistry) urlForPath(relPath string) string {
	u := *r.baseURL
	u.Path = path.Join(u.EscapedPath(), relPath)
	return u.String()
}

type schemaRegistryCacheKey struct {
	subject string
	schema  string
}

type schemaRegistryCache struct {
	mu syncutil.Mutex
	// cache[schemaRegistryCacheKey]registeredSchemaID
	entries *cache.UnorderedCache
}

// Get returns the already-registered id for this key if present, and
// a bool indicating a hit or miss.
func (src *schemaRegistryCache) Get(key schemaRegistryCacheKey) (int32, bool) {
	v, ok := src.entries.Get(key)
	if ok {
		return v.(int32), true
	}
	return 0, false
}

// Add caches a registered schema id.
func (src *schemaRegistryCache) Add(key schemaRegistryCacheKey, id int32) {
	src.entries.Add(key, id)
}

type schemaRegistryWithCache struct {
	base  schemaRegistry
	cache *schemaRegistryCache
}

// Ping implements the schemaRegistry interface.
func (csr *schemaRegistryWithCache) Ping(ctx context.Context) error {
	return csr.base.Ping(ctx)
}

// RegisterSchemaForSubject implements the schemaRegistry interface.
func (csr *schemaRegistryWithCache) RegisterSchemaForSubject(
	ctx context.Context, subject string, schema string,
) (int32, error) {
	cacheKey := schemaRegistryCacheKey{
		subject: subject, schema: schema,
	}
	csr.cache.mu.Lock()
	defer csr.cache.mu.Unlock()
	id, ok := csr.cache.Get(cacheKey)
	if ok {
		return id, nil
	}
	id, err := csr.base.RegisterSchemaForSubject(ctx, subject, schema)
	if err == nil {
		csr.cache.Add(cacheKey, id)
	}
	return id, err
}

type sharedSchemaRegistryCaches struct {
	mu               syncutil.Mutex
	cachePerEndpoint map[string]*schemaRegistryCache
}

var schemaRegistrySingletons = &sharedSchemaRegistryCaches{cachePerEndpoint: make(map[string]*schemaRegistryCache)}

// TestingClearSchemaRegistrySingleton clears out the singleton so that different tests don't pollute each other
func TestingClearSchemaRegistrySingleton() {
	schemaRegistrySingletons.mu.Lock()
	defer schemaRegistrySingletons.mu.Unlock()
	schemaRegistrySingletons.cachePerEndpoint = make(map[string]*schemaRegistryCache)
}
