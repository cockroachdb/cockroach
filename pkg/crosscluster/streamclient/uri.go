// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"net"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

var allowedConfigUriSchemes = map[string]bool{
	"external":   true,
	"postgres":   true,
	"postgresql": true,
	"randomgen":  true,
}

// ConfigUri is a URI supplied as configuration to the job.
type ConfigUri struct {
	uri url.URL
}

func ParseConfigUri(uri string) (ConfigUri, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return ConfigUri{}, err
	}

	if !allowedConfigUriSchemes[url.Scheme] {
		return ConfigUri{}, errors.Newf("stream replication from scheme %q is unsupported", url.Scheme)
	}

	return ConfigUri{uri: *url}, nil
}

func (c ConfigUri) Serialize() string {
	return c.uri.String()
}

func (sa *ConfigUri) Redacted() string {
	copy := sa.uri
	if copy.User != nil {
		if _, passwordSet := copy.User.Password(); passwordSet {
			copy.User = url.UserPassword(copy.User.Username(), "redacted")
		}
	}
	copy.RawQuery = "redacted"
	return copy.String()
}

func (c *ConfigUri) AsClusterUri(ctx context.Context, db isql.DB) (ClusterUri, error) {
	if c.uri.Scheme != "external" {
		return ParseClusterUri(c.uri.String())
	}

	// Retrieve the external connection object from the system table.
	var ec externalconn.ExternalConnection
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		ec, err = externalconn.LoadExternalConnection(ctx, c.uri.Host, txn)
		return err
	}); err != nil {
		return ClusterUri{}, errors.Wrap(err, "converting external connection into crdb cluster connection")
	}
	return ParseClusterUri(ec.ConnectionProto().UnredactedURI())
}

// ClusterUri is a connection uri for a crdb cluster.
type ClusterUri struct {
	uri url.URL
}

func ParseClusterUri(uri string) (ClusterUri, error) {
	url, err := url.Parse(uri)
	if err != nil {
		return ClusterUri{}, err
	}
	if url.Scheme == "external" {
		return ClusterUri{}, errors.AssertionFailedf("external uri %q must be resolved before constructing a cluster uri", uri)
	}
	if !allowedConfigUriSchemes[url.Scheme] {
		return ClusterUri{}, errors.Newf("stream replication from scheme %q is unsupported", url.Scheme)
	}
	return ClusterUri{uri: *url}, nil
}

func LookupClusterUri(ctx context.Context, configUri string, db isql.DB) (ClusterUri, error) {
	config, err := ParseConfigUri(configUri)
	if err != nil {
		return ClusterUri{}, err
	}
	return config.AsClusterUri(ctx, db)
}

// MakeTestClusterUri creates an unvalidated ClusterUri for testing.
func MakeTestClusterUri(url url.URL) ClusterUri {
	return ClusterUri{uri: url}
}

func (sa *ClusterUri) ResolveNode(hostname util.UnresolvedAddr) (ClusterUri, error) {
	host, port, err := net.SplitHostPort(hostname.AddressField)
	if err != nil {
		return ClusterUri{}, err
	}
	copy := sa.uri
	copy.Host = net.JoinHostPort(host, port)
	return ClusterUri{uri: copy}, nil
}

func (sa *ClusterUri) Serialize() string {
	return sa.uri.String()
}

// URL parses the uri into a URL.
func (sa *ClusterUri) URL() url.URL {
	return sa.uri
}

func (sa *ClusterUri) Redacted() string {
	return redactUrl(sa.uri)
}

func redactUrl(u url.URL) string {
	if u.User != nil {
		if _, passwordSet := u.User.Password(); passwordSet {
			u.User = url.UserPassword(u.User.Username(), "redacted")
		}
	}
	u.RawQuery = "redacted"
	return u.String()
}
