// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package secrets

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GCPResolver resolves secrets from GCP Secret Manager using the official
// client library. It uses Application Default Credentials.
//
// References are fully qualified secret version resource names, e.g.
// "projects/my-project/secrets/my-secret/versions/latest".
// The "gcp:" prefix is already stripped by the Registry before calling Resolve.
//
// The project and optional secretPrefix are configured at construction time.
// When writing, secrets are created with ID "{secretPrefix}-{secretID}" in the
// configured project. The prefix enables instance isolation when multiple
// roachprod-centralized deployments share the same GCP project.
type GCPResolver struct {
	client       *secretmanager.Client
	project      string
	secretPrefix string
}

// NewGCPResolver creates a new GCP Secret Manager resolver that reads and
// writes secrets in the given project. The optional secretPrefix is prepended
// to all secret IDs during writes (separated by "-"), enabling isolation
// between multiple instances sharing a GCP project.
func NewGCPResolver(
	ctx context.Context, project string, secretPrefix string,
) (*GCPResolver, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "create secretmanager client")
	}
	// Normalize prefix so it always ends with "-" when non-empty,
	// ensuring a clean separator between prefix and secret ID.
	if secretPrefix != "" && secretPrefix[len(secretPrefix)-1] != '-' {
		secretPrefix += "-"
	}
	return &GCPResolver{
		client:       client,
		project:      project,
		secretPrefix: secretPrefix,
	}, nil
}

// Resolve accesses a secret version and returns the payload as a string.
func (r *GCPResolver) Resolve(ctx context.Context, reference string) (string, error) {
	result, err := r.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: reference,
	})
	if err != nil {
		return "", errors.Wrapf(err, "access secret version %s", reference)
	}
	return string(result.Payload.Data), nil
}

// Write creates or updates a secret in GCP Secret Manager and adds a new
// version with the given value. The secret ID is prefixed with the
// resolver's configured secretPrefix. Returns the resource name that can
// be used to access the latest version.
func (r *GCPResolver) Write(ctx context.Context, secretID, value string) (string, error) {
	fullID := r.secretPrefix + secretID
	parent := fmt.Sprintf("projects/%s", r.project)
	_, err := r.client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   parent,
		SecretId: fullID,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil && !isAlreadyExists(err) {
		return "", errors.Wrapf(err, "create secret %s/%s", r.project, fullID)
	}

	secretName := fmt.Sprintf("projects/%s/secrets/%s", r.project, fullID)
	_, err = r.client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: secretName,
		Payload: &secretmanagerpb.SecretPayload{
			Data: []byte(value),
		},
	})
	if err != nil {
		return "", errors.Wrapf(err, "add secret version %s/%s", r.project, fullID)
	}

	return fmt.Sprintf(
		"projects/%s/secrets/%s/versions/latest", r.project, fullID,
	), nil
}

// Verify checks that the secret version referenced is accessible.
func (r *GCPResolver) Verify(ctx context.Context, reference string) error {
	_, err := r.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: reference,
	})
	if err != nil {
		return errors.Wrapf(err, "verify secret version %s", reference)
	}
	return nil
}

// isAlreadyExists returns true if the error represents a gRPC
// AlreadyExists status code.
func isAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

// Close releases the underlying gRPC connection.
func (r *GCPResolver) Close() error {
	return r.client.Close()
}
