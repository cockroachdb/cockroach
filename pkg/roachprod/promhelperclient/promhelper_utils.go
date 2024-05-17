// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package promhelperclient

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"google.golang.org/api/option"
)

var (
	userHome, _ = os.UserHomeDir()
	// promCredFile is where the prom helper credentials are stored
	promCredFile = filepath.Join(userHome, ".roachprod", "promhelper-secrets")
)

// FetchedFrom indicates where the credentials have been fetched from.
// this helps in debugging and testing.
type FetchedFrom string

const (
	Env   FetchedFrom = "Env"   // fetched from environment
	File  FetchedFrom = "File"  // fetched from the promCredFile
	Store FetchedFrom = "Store" // fetched from the store

	// secretsDelimiter is used as a delimiter between service account audience and JSON when stored in
	// promCredFile or cloud storage
	secretsDelimiter = "--||--"

	// bucket and objectLocation are for fetching the creds for store
	bucket         = "promhelpers"
	objectLocation = "promhelpers-secrets"
)

// SetPromHelperCredsEnv sets the environment variables ServiceAccountAudience and
// ServiceAccountJson based on the following conditions:
// > check if forFetch is false
//
//	> forceFetch is false
//	  > if env is set return
//	  > if env is not set read from the promCredFile
//	  > if file is available and the creds can be read, set env return
//	> read the creds from secrets manager
//
// > set the env variable and save the creds to the promCredFile
func SetPromHelperCredsEnv(
	ctx context.Context, forceFetch bool, l *logger.Logger,
) (FetchedFrom, error) {
	creds := ""
	fetchedFrom := Env
	if !forceFetch { // bypass environment and creds file if forceFetch is false
		// check if environment is set
		audience := os.Getenv(ServiceAccountAudience)
		saJson := os.Getenv(ServiceAccountJson)
		if audience != "" && saJson != "" {
			l.Printf("Secrets obtained from environment.")
			return fetchedFrom, nil
		}
		// check if the secrets file is available
		b, err := os.ReadFile(promCredFile)
		if err == nil {
			l.Printf("Secrets obtained from temp file: %s", promCredFile)
			creds = string(b)
			fetchedFrom = File
		}
	}
	if creds == "" {
		// creds == "" means (env is not set and the file does not have the creds) or forceFetch is true
		l.Printf("creds need to be fetched from store.")
		client, err := storage.NewClient(ctx, option.WithScopes(storage.ScopeReadOnly))
		if err != nil {
			return fetchedFrom, err
		}
		defer func() { _ = client.Close() }()
		fetchedFrom = Store
		obj := client.Bucket(bucket).Object(objectLocation)
		r, err := obj.NewReader(ctx)
		if err != nil {
			return fetchedFrom, err
		}
		defer func() { _ = r.Close() }()
		body, err := io.ReadAll(r)
		creds = string(body)
		if err != nil {
			return fetchedFrom, err
		}
		err = os.WriteFile(promCredFile, body, 0700)
		if err != nil {
			l.Errorf("error writing to the credential file: %v", err)
		}
	}
	secretValues := strings.Split(creds, secretsDelimiter)
	if len(secretValues) == 2 {
		_ = os.Setenv(ServiceAccountAudience, secretValues[0])
		_ = os.Setenv(ServiceAccountJson, secretValues[1])
		return fetchedFrom, nil
	}
	return fetchedFrom, fmt.Errorf("invalid secret values - %s", creds)
}
