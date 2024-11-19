// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodutil

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
)

var (
	userHome, _ = os.UserHomeDir()
	// serviceAccountCredFile is where the service account credentials are
	// stored that enable Google IDP auth.
	serviceAccountCredFile = filepath.Join(userHome, ".roachprod", "service-account-secrets")
)

// FetchedFrom indicates where the credentials have been fetched from.
// this helps in debugging and testing.
type FetchedFrom string

const (
	ServiceAccountJson     = "ROACHPROD_SERVICE_ACCOUNT_JSON"
	ServiceAccountAudience = "ROACHPROD_SERVICE_ACCOUNT_AUDIENCE"

	Env   FetchedFrom = "Env"   // fetched from environment
	File  FetchedFrom = "File"  // fetched from the serviceAccountCredFile
	Store FetchedFrom = "Store" // fetched from the store

	// secretsDelimiter is used as a delimiter between service account audience and JSON when stored in
	// serviceAccountCredFile or cloud storage
	secretsDelimiter = "--||--"

	// bucket and objectLocation are for fetching the creds for store.
	//
	// N.B. although the bucket is called promhelpers, the service account stored
	// works for any service that requires Google IDP, i.e. Grafana annotations.
	bucket         = "promhelpers"
	objectLocation = "promhelpers-secrets"
)

// SetServiceAccountCredsEnv sets the environment variables ServiceAccountAudience and
// ServiceAccountJson based on the following conditions:
// > check if forceFetch is false
//
//	> forceFetch is false
//	  > if env is set return
//	  > if env is not set read from the serviceAccountCredFile
//	  > if file is available and the creds can be read, set env return
//	> read the creds from secrets manager
//
// > set the env variable and save the creds to the serviceAccountCredFile
func SetServiceAccountCredsEnv(ctx context.Context, forceFetch bool) (FetchedFrom, error) {
	creds := ""
	fetchedFrom := Env
	if !forceFetch { // bypass environment and creds file if forceFetch is false
		// check if environment is set
		audience := os.Getenv(ServiceAccountAudience)
		saJson := os.Getenv(ServiceAccountJson)
		if audience != "" && saJson != "" {
			return fetchedFrom, nil
		}
		// check if the secrets file is available
		b, err := os.ReadFile(serviceAccountCredFile)
		if err == nil {
			creds = string(b)
			fetchedFrom = File
		}
	}
	if creds == "" {
		// creds == "" means (env is not set and the file does not have the creds) or forceFetch is true
		options := []option.ClientOption{option.WithScopes(storage.ScopeReadOnly)}
		cj := os.Getenv("GOOGLE_EPHEMERAL_CREDENTIALS")
		if cj != "" {
			options = append(options, option.WithCredentialsJSON([]byte(cj)))
		}

		client, err := storage.NewClient(ctx, options...)
		if err != nil {
			return "", err
		}
		defer func() { _ = client.Close() }()
		fetchedFrom = Store
		obj := client.Bucket(bucket).Object(objectLocation)
		r, err := obj.NewReader(ctx)
		if err != nil {
			return "", err
		}
		defer func() { _ = r.Close() }()
		body, err := io.ReadAll(r)
		creds = string(body)
		if err != nil {
			return "", err
		}
		_ = os.WriteFile(serviceAccountCredFile, body, 0700)
	}
	secretValues := strings.Split(creds, secretsDelimiter)
	if len(secretValues) == 2 {
		_ = os.Setenv(ServiceAccountAudience, secretValues[0])
		_ = os.Setenv(ServiceAccountJson, secretValues[1])
		return fetchedFrom, nil
	}
	return "", errors.Newf("invalid secret values - %s", creds)
}

// GetServiceAccountToken returns a GCS oauth token based on the service account key and audience
// set through the ServiceAccountJson and ServiceAccountAudience env vars.
// Assumes that the env vars have been set already, i.e. through SetServiceAccountCredsEnv.
func GetServiceAccountToken(
	ctx context.Context,
	tokenSource func(ctx context.Context, audience string, opts ...idtoken.ClientOption) (oauth2.TokenSource, error),
) (string, error) {
	key := os.Getenv(ServiceAccountJson)
	audience := os.Getenv(ServiceAccountAudience)
	ts, err := tokenSource(ctx, audience, idtoken.WithCredentialsJSON([]byte(key)))
	if err != nil {
		return "", errors.Wrap(err, "error creating GCS oauth token source from specified credential")
	}
	token, err := ts.Token()
	if err != nil {
		return "", errors.Wrap(err, "error getting identity token")
	}
	return token.AccessToken, nil
}
