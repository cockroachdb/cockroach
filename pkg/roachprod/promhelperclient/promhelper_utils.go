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
	"os"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

var (
	// promCredFile is where the prom helper credentials are stored
	promCredFile = os.TempDir() + "promhelpers-secrets"
)

// FetchedFrom indicates where the credentials have been fetched from.
// this helps in debugging and testing.
type FetchedFrom string

const (
	ENV        FetchedFrom = "ENV"        // fetched from environment
	FILE       FetchedFrom = "FILE"       // fetched from the promCredFile
	SECRET_MGR FetchedFrom = "SECRET_MGR" // fetched from the secrets manager

	// secretsDelimiter is used as a delimeter between service account audience and JSON when stored in promCredFile or
	// secrets manager
	secretsDelimiter = "--||--"

	// project secrets and versions are for fetching the creds from secrets manager
	project  = "cockroach-ephemeral"
	secrets  = "prom-helpers-access"
	versions = "latest"
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
	fetchedFrom := ENV
	if !forceFetch { // bypass environment anf creds file if forceFetch is false
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
			fetchedFrom = FILE
		}
	}
	if creds == "" {
		// creds == "" means (env is not set and the file does not have the creds) or forFetch is true
		l.Printf("creds need to be fetched from secret manager.")
		client, err := secretmanager.NewClient(ctx)
		if err != nil {
			return fetchedFrom, err
		}
		defer func() { _ = client.Close() }()
		req := &secretmanagerpb.AccessSecretVersionRequest{
			Name: fmt.Sprintf("projects/%s/secrets/%s/versions/%s", project, secrets, versions),
		}
		fetchedFrom = SECRET_MGR
		secrets, err := client.AccessSecretVersion(ctx, req)
		if err != nil {
			return fetchedFrom, err
		}
		creds = string(secrets.GetPayload().GetData())
		err = os.WriteFile(promCredFile, []byte(creds), 0700)
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
