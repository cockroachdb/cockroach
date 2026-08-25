// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
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

	// LookupSchemaIDForSubject resolves a schema ID for subject using strategy
	// without modifying the registry. Each candidate version is offered to
	// accept, which rejects it by returning an error; under latest_compatible,
	// rejected versions are skipped until one is accepted.
	LookupSchemaIDForSubject(
		ctx context.Context,
		subject string,
		localSchema string,
		strategy changefeedbase.AvroVersionStrategy,
		accept func(registeredSchema string) error,
	) (int32, error)
}

type confluentSchemaVersionRequest struct {
	Schema string `json:"schema"`
}

type confluentSchemaVersionResponse struct {
	ID int32 `json:"id"`
}

// confluentSchemaVersionMetadata is the response returned against a
// registered subject version.
type confluentSchemaVersionMetadata struct {
	Subject string `json:"subject"`
	Version int32  `json:"version"`
	ID      int32  `json:"id"`
	Schema  string `json:"schema"`
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
		if resp.StatusCode != http.StatusOK {
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

// LookupSchemaIDForSubject implements the schemaRegistry interface. It fetches
// registered versions of the subject over HTTP and selects one according to
// strategy, validating each candidate through accept.
func (r *confluentSchemaRegistry) LookupSchemaIDForSubject(
	ctx context.Context,
	subject string,
	localSchema string,
	strategy changefeedbase.AvroVersionStrategy,
	accept func(registeredSchema string) error,
) (int32, error) {
	if strategy.Kind == changefeedbase.AvroVersionLatestCompatible {
		return r.lookupLatestCompatibleSchema(ctx, subject, accept)
	}
	version := "latest"
	if strategy.Kind == changefeedbase.AvroVersionPin {
		version = strconv.Itoa(int(strategy.PinnedVersion))
	}
	meta, err := r.getVersion(ctx, subject, version)
	if err != nil {
		return 0, err
	}
	if err := accept(meta.Schema); err != nil {
		return 0, err
	}
	return meta.ID, nil
}

// getVersion fetches the metadata, including schema ID and schema text, for a
// single registered version. version is either a 1-based version integer or the
// literal "latest".
//
// See: https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)
func (r *confluentSchemaRegistry) getVersion(
	ctx context.Context, subject string, version string,
) (confluentSchemaVersionMetadata, error) {
	u := r.urlForPath(fmt.Sprintf("subjects/%s/versions/%s", subject, version))
	if log.V(1) {
		log.Infof(ctx, "looking up avro schema id %s", u)
	}
	var meta confluentSchemaVersionMetadata
	err := r.doWithRetry(ctx, func() error {
		resp, err := r.client.Get(ctx, u)
		if err != nil {
			return errors.Wrap(err, "contacting confluent schema registry")
		}
		defer gracefulClose(ctx, resp.Body)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return errors.Errorf("looking up schema id for subject %q version %q at %s: %s: %s",
				subject, version, u, resp.Status, body)
		}
		if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
			return errors.Wrap(err, "decoding confluent schema registry reply")
		}
		return nil
	})
	if err != nil {
		return confluentSchemaVersionMetadata{}, err
	}
	return meta, nil
}

// lookUpAllVersions returns the registered version integers for subject.
//
// See: https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions
func (r *confluentSchemaRegistry) lookUpAllVersions(
	ctx context.Context, subject string,
) ([]int32, error) {
	u := r.urlForPath(fmt.Sprintf("subjects/%s/versions", subject))
	if log.V(1) {
		log.Infof(ctx, "listing avro schema versions %s", u)
	}
	var versions []int32
	err := r.doWithRetry(ctx, func() error {
		resp, err := r.client.Get(ctx, u)
		if err != nil {
			return errors.Wrap(err, "contacting confluent schema registry")
		}
		defer gracefulClose(ctx, resp.Body)
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return errors.Errorf("listing versions for subject %q at %s: %s: %s",
				subject, u, resp.Status, body)
		}
		versions = nil
		if err := json.NewDecoder(resp.Body).Decode(&versions); err != nil {
			return errors.Wrap(err, "decoding confluent schema registry reply")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return versions, nil
}

// lookupLatestCompatibleSchema resolves the ID of the newest registered
// version of the subject that accept approves, walking versions newest to
// oldest.
func (r *confluentSchemaRegistry) lookupLatestCompatibleSchema(
	ctx context.Context, subject string, accept func(registeredSchema string) error,
) (int32, error) {
	versions, err := r.lookUpAllVersions(ctx, subject)
	if err != nil {
		return 0, err
	}
	slices.SortFunc(versions, func(a, b int32) int { return cmp.Compare(b, a) })

	var rejectionReasons []string
	for _, version := range versions {
		meta, err := r.getVersion(ctx, subject, strconv.Itoa(int(version)))
		if err != nil {
			return 0, err
		}
		if err := accept(meta.Schema); err != nil {
			rejectionReasons = append(rejectionReasons,
				fmt.Sprintf("version %d: %v", version, err))
			log.VInfof(ctx, 1,
				"subject %q version %d is not avro-compatible with the generated schema: %v",
				subject, version, err)
			continue
		}
		return meta.ID, nil
	}

	return 0, errors.Newf(
		"no compatible registered avro schema version found for subject %q using strategy %s; "+
			"rejections: %s",
		subject, changefeedbase.AvroVersionStrategy{
			Kind: changefeedbase.AvroVersionLatestCompatible,
		}.String(), strings.Join(rejectionReasons, "; "))
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
		log.VInfof(ctx, 2, "failure to close schema registry connection: %+v", err)
	}
}

func (r *confluentSchemaRegistry) urlForPath(relPath string) string {
	u := *r.baseURL
	u.Path = path.Join(u.EscapedPath(), relPath)
	return u.String()
}

type schemaRegistryCacheKey struct {
	subject  string
	schema   string
	strategy string
}

// schemaRegistryCacheEntry is a cached schema resolution.
type schemaRegistryCacheEntry struct {
	id               int32
	registeredSchema string
}

type schemaRegistryCache struct {
	mu syncutil.Mutex
	// entries maps schemaRegistryCacheKey to schemaRegistryCacheEntry.
	entries *cache.UnorderedCache
}

// Get returns the cached resolution for this key if present, and a bool
// indicating a hit or miss.
func (src *schemaRegistryCache) Get(key schemaRegistryCacheKey) (schemaRegistryCacheEntry, bool) {
	v, ok := src.entries.Get(key)
	if ok {
		return v.(schemaRegistryCacheEntry), true
	}
	return schemaRegistryCacheEntry{}, false
}

// Add caches a schema resolution.
func (src *schemaRegistryCache) Add(key schemaRegistryCacheKey, entry schemaRegistryCacheEntry) {
	src.entries.Add(key, entry)
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
	if entry, ok := csr.cache.Get(cacheKey); ok {
		return entry.id, nil
	}
	id, err := csr.base.RegisterSchemaForSubject(ctx, subject, schema)
	if err == nil {
		csr.cache.Add(cacheKey, schemaRegistryCacheEntry{id: id})
	}
	return id, err
}

// LookupSchemaIDForSubject implements the schemaRegistry interface. The cache is
// keyed on {subject, schema, strategy}; keying on the local schema is what
// causes a table schema change to re-resolve the ID rather than reuse a stale
// one, and keying on strategy keeps version selections independent.
func (csr *schemaRegistryWithCache) LookupSchemaIDForSubject(
	ctx context.Context,
	subject string,
	schema string,
	strategy changefeedbase.AvroVersionStrategy,
	accept func(registeredSchema string) error,
) (int32, error) {
	cacheKey := schemaRegistryCacheKey{
		subject: subject, schema: schema, strategy: strategy.String(),
	}
	csr.cache.mu.Lock()
	defer csr.cache.mu.Unlock()
	if entry, ok := csr.cache.Get(cacheKey); ok {
		if err := accept(entry.registeredSchema); err != nil {
			return 0, err
		}
		return entry.id, nil
	}
	var accepted string
	id, err := csr.base.LookupSchemaIDForSubject(ctx, subject, schema, strategy,
		func(registeredSchema string) error {
			accepted = registeredSchema
			return accept(registeredSchema)
		})
	if err == nil {
		csr.cache.Add(cacheKey, schemaRegistryCacheEntry{id: id, registeredSchema: accepted})
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
