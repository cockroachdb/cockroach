// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import (
	"context"
	"net/http"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/google/go-github/v32/github"
)

type installationID int64

// getGithubClientFromInstallation returns a GitHub client which acts
// on behalf of the given installation.
func (srv *blathersServer) getGithubClientFromInstallation(
	ctx context.Context, id installationID,
) *github.Client {
	return github.NewClient(&http.Client{
		Transport: &accessTokenHTTPTransport{srv: srv, installationID: id, ctx: ctx},
	})
}

// fetchAccessToken fetches the access token for the local installation from the
// local cache, fetching a fresh access token from GitHub if it is expired
// or not found.
func (srv *blathersServer) fetchAccessToken(
	ctx context.Context, id installationID,
) (string, error) {
	srv.tokenStoreMu.Lock()
	defer srv.tokenStoreMu.Unlock()

	if srv.tokenStoreMu.store == nil {
		srv.tokenStoreMu.store = map[installationID]*github.InstallationToken{}
	}

	token, ok := srv.tokenStoreMu.store[id]
	if ok && token.GetExpiresAt().After(time.Now()) {
		return token.GetToken(), nil
	}

	gh := github.NewClient(&http.Client{
		Transport: &installationTokenHTTPTransport{
			srv: srv,
			ctx: ctx,
		},
	})

	var err error
	token, _, err = gh.Apps.CreateInstallationToken(ctx, int64(id), &github.InstallationTokenOptions{})
	if err != nil {
		return "", wrapf(ctx, err, "failed getting installation token")
	}
	srv.tokenStoreMu.store[id] = token
	return token.GetToken(), nil
}

// accessTokenHTTPTransport is used when we expect an access token to handle a request.
type accessTokenHTTPTransport struct {
	srv            *blathersServer
	installationID installationID
	ctx            context.Context
}

var _ http.RoundTripper = (*accessTokenHTTPTransport)(nil)

func (c *accessTokenHTTPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	accessToken, err := c.srv.fetchAccessToken(context.Background(), c.installationID)
	if err != nil {
		return nil, wrap(c.ctx, err, "failed getting signed token")
	}
	r.Header.Add("Authorization", "token "+accessToken)
	return http.DefaultTransport.RoundTrip(r)
}

// installationTokenHTTPTransport represents a client with no access
// to the installation access token as one needs to be minted.
//
// This should be used for the GetInstallationToken command when
// attempting to fetch an access token.
type installationTokenHTTPTransport struct {
	srv *blathersServer
	ctx context.Context
}

var _ http.RoundTripper = (*installationTokenHTTPTransport)(nil)

func (c *installationTokenHTTPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	now := time.Now()
	t := jwt.NewWithClaims(
		jwt.SigningMethodRS256,
		jwt.MapClaims{
			"iss": c.srv.githubClientID,
			"iat": now.Unix(),
			"exp": now.Add(time.Minute).Unix(),
		},
	)
	signedStr, err := t.SignedString(c.srv.githubAppPrivateKey)
	if err != nil {
		return nil, wrap(c.ctx, err, "failed getting signed signautre: %v")
	}
	r.Header.Add("Authorization", "Bearer "+signedStr)
	return http.DefaultTransport.RoundTrip(r)
}
