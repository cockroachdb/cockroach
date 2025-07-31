// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// RoachtestHTTPClient is a wrapper over httputil.client that:
//   - Adds cookies for every request, allowing the client to
//     authenticate with secure clusters.
//   - Sets InsecureSkipVerify to true. Roachprod certs are not
//     signed by a verified root authority.
//   - Unsets DialContext and DisableKeepAlives set in httputil.
//     These two settings are not needed for testing purposes.
//
// This hides the details of creating authenticated https requests
// from the roachtest user, as most of it is just boilerplate and
// the same across all tests.
//
// Note that this currently only supports requests to CRDB clusters.
type RoachtestHTTPClient struct {
	client             *httputil.Client
	sessionID          string
	cluster            cluster.Cluster
	l                  *logger.Logger
	virtualClusterName string
	// Used for safely adding to the cookie jar.
	mu syncutil.Mutex
}

type RoachtestHTTPOptions struct {
	Timeout            time.Duration
	VirtualClusterName string
}

func HTTPTimeout(timeout time.Duration) func(options *RoachtestHTTPOptions) {
	return func(options *RoachtestHTTPOptions) {
		options.Timeout = timeout
	}
}

func VirtualCluster(name string) func(*RoachtestHTTPOptions) {
	return func(options *RoachtestHTTPOptions) {
		options.VirtualClusterName = name
	}
}

// DefaultHTTPClient returns a default RoachtestHTTPClient.
// This should be the default HTTP client used if you are
// trying to make a request to a secure cluster.
func DefaultHTTPClient(
	c cluster.Cluster, l *logger.Logger, opts ...func(options *RoachtestHTTPOptions),
) *RoachtestHTTPClient {
	roachtestHTTP := RoachtestHTTPClient{
		client:  httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout),
		cluster: c,
		l:       l,
	}

	// Certificates created by roachprod start are not signed by
	// a verified root authority.
	roachtestHTTP.client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	httpOptions := &RoachtestHTTPOptions{}
	for _, opt := range opts {
		opt(httpOptions)
	}
	if httpOptions.Timeout != 0 {
		roachtestHTTP.client.Timeout = httpOptions.Timeout
	}
	roachtestHTTP.virtualClusterName = httpOptions.VirtualClusterName

	return &roachtestHTTP
}

func (r *RoachtestHTTPClient) Get(ctx context.Context, url string) (*http.Response, error) {
	if err := r.addCookies(ctx, url); err != nil {
		return nil, err
	}
	return r.client.Get(ctx, url)
}

func (r *RoachtestHTTPClient) GetJSON(
	ctx context.Context, path string, response protoutil.Message, opts ...httputil.JSONOption,
) error {
	if err := r.addCookies(ctx, path); err != nil {
		return err
	}
	return httputil.GetJSONWithOptions(*r.client.Client, path, response, opts...)
}

func (r *RoachtestHTTPClient) PostProtobuf(
	ctx context.Context, path string, request, response protoutil.Message,
) error {
	if err := r.addCookies(ctx, path); err != nil {
		return err
	}
	return httputil.PostProtobuf(ctx, *r.client.Client, path, request, response)
}

// ResetSession removes the saved sessionID so that it is retrieved
// again the next time a request is made. This is usually not required
// as the sessionID does not change unless the cluster is restarted.
func (r *RoachtestHTTPClient) ResetSession() {
	r.sessionID = ""
}

func (r *RoachtestHTTPClient) addCookies(ctx context.Context, cookieUrl string) error {
	// If the cluster is not running in secure mode, don't try to add cookies.
	if !r.cluster.IsSecure() {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// If we haven't extracted the sessionID yet, do so.
	if r.sessionID == "" {
		id, err := getSessionID(ctx, r.cluster, r.l, r.cluster.All(), r.virtualClusterName)
		if err != nil {
			return errors.Wrapf(err, "roachtestutil.addCookies: unable to extract sessionID")
		}
		r.sessionID = id
	}

	name, value, found := strings.Cut(r.sessionID, "=")
	if !found {
		return errors.Newf("cookie not formatted correctly: %q", r.sessionID)
	}

	url, err := url.Parse(cookieUrl)
	if err != nil {
		return errors.Wrapf(err, "roachtestutil.addCookies: unable to parse cookieUrl")
	}

	cookies := []*http.Cookie{{Name: name, Value: value}}
	if r.virtualClusterName != "" {
		cookies = append(cookies, &http.Cookie{Name: "tenant", Value: r.virtualClusterName})
	}

	err = r.SetCookies(url, cookies)
	if err != nil {
		return errors.Wrapf(err, "roachtestutil.addCookies: unable to set cookies")
	}

	return nil
}

// SetCookies is a helper that checks if a client.CookieJar exists and creates
// one if it doesn't. It then sets the provided cookies through CookieJar.SetCookies.
func (r *RoachtestHTTPClient) SetCookies(u *url.URL, cookies []*http.Cookie) error {
	if r.client.Jar == nil {
		jar, err := cookiejar.New(nil)
		if err != nil {
			return err
		}
		r.client.Jar = jar
	}
	r.client.Jar.SetCookies(u, cookies)
	return nil
}

func getSessionIDOnSingleNode(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	node option.NodeListOption,
	virtualClusterName string,
) (string, error) {
	var virtualClusterSelector string
	if virtualClusterName != "" {
		virtualClusterSelector = fmt.Sprintf(":%s", virtualClusterName)
	}

	loginCmd := fmt.Sprintf(
		"%s auth-session login root --url={pgurl%s%s} --certs-dir ./certs --only-cookie",
		test.DefaultCockroachPath, node, virtualClusterSelector,
	)
	res, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(node), loginCmd)
	if err != nil {
		return "", errors.Wrap(err, "failed to authenticate")
	}

	return res.Stdout, nil
}

func getSessionID(
	ctx context.Context,
	c cluster.Cluster,
	l *logger.Logger,
	nodes option.NodeListOption,
	virtualClusterName string,
) (string, error) {
	var err error
	var cookie string
	// The session ID should be the same for all nodes so stop after we successfully
	// get it from one node.
	for _, node := range nodes {
		cookie, err = getSessionIDOnSingleNode(ctx, c, l, c.Node(node), virtualClusterName)
		if err == nil {
			break
		}
		l.Printf("%s auth-session login failed on node %d: %v", test.DefaultCockroachPath, node, err)
	}
	if err != nil {
		return "", errors.Wrapf(err, "roachtestutil.GetSessionID")
	}
	sessionID := strings.Split(cookie, ";")[0]
	return sessionID, nil
}

// Download downloads the file at the given url and saves it to filename.
func (r *RoachtestHTTPClient) Download(ctx context.Context, url string, filename string) error {
	out, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := r.Get(ctx, url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		return err
	}
	return nil
}
