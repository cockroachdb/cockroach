// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestutil

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/pkg/errors"
)

// SystemInterfaceSystemdUnitName is a convenience function that
// returns the systemd unit name for the system interface
func SystemInterfaceSystemdUnitName() string {
	return install.VirtualClusterLabel(install.SystemInterfaceName, 0)
}

// DefaultPGUrl is a wrapper over roachprod.PgUrl that calls it with the arguments
// that *almost* all roachtests want: single tenant and only a single node.
func DefaultPGUrl(
	ctx context.Context, c cluster.Cluster, l *logger.Logger, node option.NodeListOption,
) (string, error) {
	opts := roachprod.PGURLOptions{}
	opts.Secure = c.IsSecure()
	pgurl, err := roachprod.PgURL(ctx, l, c.MakeNodes(node), "certs", opts)
	if err != nil {
		return "", err
	}
	return pgurl[0], nil
}

// SetDefaultSQLPort sets the SQL port to the default of 26257 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultSQLPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.SQLPort = config.DefaultSQLPort
	}
}

// SetDefaultAdminUIPort sets the AdminUI port to the default of 26258 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultAdminUIPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.AdminUIPort = config.DefaultAdminUIPort
	}
}

func getSessionIDOnSingleNode(
	ctx context.Context, c cluster.Cluster, l *logger.Logger, node option.NodeListOption,
) (string, error) {
	loginCmd := fmt.Sprintf(
		"%s auth-session login root --port={pgport%s} --certs-dir ./certs --format raw",
		test.DefaultCockroachPath, node,
	)
	res, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(node), loginCmd)
	if err != nil {
		return "", errors.Wrap(err, "failed to authenticate")
	}

	var sessionCookie string
	for _, line := range strings.Split(res.Stdout, "\n") {
		if strings.HasPrefix(line, "session=") {
			sessionCookie = line
		}
	}
	if sessionCookie == "" {
		return "", fmt.Errorf("failed to find session cookie in `login` output")
	}

	return sessionCookie, nil
}

func GetSessionID(
	ctx context.Context, c cluster.Cluster, l *logger.Logger, nodes option.NodeListOption,
) (string, error) {
	var err error
	var cookie string
	// The session ID should be the same for all nodes so stop after we successfully
	// get it from one node.
	for _, node := range nodes {
		cookie, err = getSessionIDOnSingleNode(ctx, c, l, c.Node(node))
		if err == nil {
			break
		}
	}
	if err != nil {
		return "", errors.Wrapf(err, "roachtestutils.GetSessionID")
	}
	sessionID := strings.Split(cookie, ";")[0]
	return sessionID, nil
}

// DefaultHttpClientWithSessionCookie returns a default Http client as defined in httputil,
// and adds the session id to the cookie jar for the specified url.
func DefaultHttpClientWithSessionCookie(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	nodes option.NodeListOption,
	cookieUrl string,
) (http.Client, error) {
	sessionID, err := GetSessionID(ctx, c, l, nodes)
	if err != nil {
		return http.Client{}, err
	}
	name, value, found := strings.Cut(sessionID, "=")
	if !found {
		return http.Client{}, errors.New("Cookie not formatted correctly")
	}
	client := httputil.DefaultClient.Client
	url, err := url.Parse(cookieUrl)
	if err != nil {
		return http.Client{}, err
	}
	err = httputil.SetCookies(client, url, []*http.Cookie{{Name: name, Value: value}})
	if err != nil {
		return http.Client{}, err
	}
	return *client, nil
}
