// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
	client    *httputil.Client
	retryOpts retry.Options
}

var _ schemaRegistry = (*confluentSchemaRegistry)(nil)

func newConfluentSchemaRegistry(baseURL string) (*confluentSchemaRegistry, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrap(err, "malformed schema registry url")
	}

	query := u.Query()
	var caCert []byte
	if caCertString := query.Get(changefeedbase.RegistryParamCACert); caCertString != "" {
		err := decodeBase64FromString(caCertString, &caCert)
		if err != nil {
			return nil, errors.Wrapf(err, "param %s must be base 64 encoded", changefeedbase.RegistryParamCACert)
		}
	}
	// remove query param to ensure compatibility with schema
	// registry implementation
	query.Del(changefeedbase.RegistryParamCACert)
	u.RawQuery = query.Encode()

	httpClient, err := setupHTTPClient(u, caCert)
	if err != nil {
		return nil, err
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 2
	return &confluentSchemaRegistry{
		baseURL:   u,
		client:    httpClient,
		retryOpts: retryOpts,
	}, nil
}

// Setup the httputil.Client to use when dialing Confluent schema registry. If `ca_cert`
// is set as a query param in the registry URL, client should trust the corresponding
// cert while dialing. Otherwise, use the DefaultClient.
func setupHTTPClient(baseURL *url.URL, caCert []byte) (*httputil.Client, error) {
	if caCert != nil {
		httpClient, err := newClientFromTLSKeyPair(caCert)
		if err != nil {
			return nil, err
		}
		if baseURL.Scheme == "http" {
			log.Warningf(context.Background(), "CA certificate provided but schema registry %s uses HTTP", baseURL)
		}
		return httpClient, nil
	}
	return httputil.DefaultClient, nil
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
//   https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
//
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
	err := r.doWithRetry(ctx, func() error {
		resp, err := r.client.Post(ctx, u, confluentSchemaContentType, &buf)
		if err != nil {
			return errors.Wrap(err, "contacting confluent schema registry")
		}
		defer gracefulClose(ctx, resp.Body)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := ioutil.ReadAll(resp.Body)
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
		log.VInfof(ctx, 2, "retrying schema registry operation: %s", err.Error())
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
	_, _ = io.CopyN(ioutil.Discard, toClose, respExtraReadLimit)
	if err := toClose.Close(); err != nil {
		log.VInfof(ctx, 2, "failure to close schema registry connection", err)
	}
}

func (r *confluentSchemaRegistry) urlForPath(relPath string) string {
	u := *r.baseURL
	u.Path = path.Join(u.EscapedPath(), relPath)
	return u.String()
}
