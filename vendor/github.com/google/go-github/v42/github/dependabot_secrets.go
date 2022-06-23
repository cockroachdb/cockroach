// Copyright 2022 The go-github AUTHORS. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package github

import (
	"context"
	"fmt"
)

func (s *DependabotService) getPublicKey(ctx context.Context, url string) (*PublicKey, *Response, error) {
	req, err := s.client.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	pubKey := new(PublicKey)
	resp, err := s.client.Do(ctx, req, pubKey)
	if err != nil {
		return nil, resp, err
	}

	return pubKey, resp, nil
}

// GetRepoPublicKey gets a public key that should be used for Dependabot secret encryption.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#get-a-repository-public-key
func (s *DependabotService) GetRepoPublicKey(ctx context.Context, owner, repo string) (*PublicKey, *Response, error) {
	url := fmt.Sprintf("repos/%v/%v/dependabot/secrets/public-key", owner, repo)
	return s.getPublicKey(ctx, url)
}

// GetOrgPublicKey gets a public key that should be used for Dependabot secret encryption.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#get-an-organization-public-key
func (s *DependabotService) GetOrgPublicKey(ctx context.Context, org string) (*PublicKey, *Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/public-key", org)
	return s.getPublicKey(ctx, url)
}

func (s *DependabotService) listSecrets(ctx context.Context, url string, opts *ListOptions) (*Secrets, *Response, error) {
	u, err := addOptions(url, opts)
	if err != nil {
		return nil, nil, err
	}

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	secrets := new(Secrets)
	resp, err := s.client.Do(ctx, req, &secrets)
	if err != nil {
		return nil, resp, err
	}

	return secrets, resp, nil
}

// ListRepoSecrets lists all Dependabot secrets available in a repository
// without revealing their encrypted values.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#list-repository-secrets
func (s *DependabotService) ListRepoSecrets(ctx context.Context, owner, repo string, opts *ListOptions) (*Secrets, *Response, error) {
	url := fmt.Sprintf("repos/%v/%v/dependabot/secrets", owner, repo)
	return s.listSecrets(ctx, url, opts)
}

// ListOrgSecrets lists all Dependabot secrets available in an organization
// without revealing their encrypted values.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#list-organization-secrets
func (s *DependabotService) ListOrgSecrets(ctx context.Context, org string, opts *ListOptions) (*Secrets, *Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets", org)
	return s.listSecrets(ctx, url, opts)
}

func (s *DependabotService) getSecret(ctx context.Context, url string) (*Secret, *Response, error) {
	req, err := s.client.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}

	secret := new(Secret)
	resp, err := s.client.Do(ctx, req, secret)
	if err != nil {
		return nil, resp, err
	}

	return secret, resp, nil
}

// GetRepoSecret gets a single repository Dependabot secret without revealing its encrypted value.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#get-a-repository-secret
func (s *DependabotService) GetRepoSecret(ctx context.Context, owner, repo, name string) (*Secret, *Response, error) {
	url := fmt.Sprintf("repos/%v/%v/dependabot/secrets/%v", owner, repo, name)
	return s.getSecret(ctx, url)
}

// GetOrgSecret gets a single organization Dependabot secret without revealing its encrypted value.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#get-an-organization-secret
func (s *DependabotService) GetOrgSecret(ctx context.Context, org, name string) (*Secret, *Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v", org, name)
	return s.getSecret(ctx, url)
}

func (s *DependabotService) putSecret(ctx context.Context, url string, eSecret *EncryptedSecret) (*Response, error) {
	req, err := s.client.NewRequest("PUT", url, eSecret)
	if err != nil {
		return nil, err
	}

	return s.client.Do(ctx, req, nil)
}

// CreateOrUpdateRepoSecret creates or updates a repository Dependabot secret with an encrypted value.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#create-or-update-a-repository-secret
func (s *DependabotService) CreateOrUpdateRepoSecret(ctx context.Context, owner, repo string, eSecret *EncryptedSecret) (*Response, error) {
	url := fmt.Sprintf("repos/%v/%v/dependabot/secrets/%v", owner, repo, eSecret.Name)
	return s.putSecret(ctx, url, eSecret)
}

// CreateOrUpdateOrgSecret creates or updates an organization Dependabot secret with an encrypted value.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#create-or-update-an-organization-secret
func (s *DependabotService) CreateOrUpdateOrgSecret(ctx context.Context, org string, eSecret *EncryptedSecret) (*Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v", org, eSecret.Name)
	return s.putSecret(ctx, url, eSecret)
}

func (s *DependabotService) deleteSecret(ctx context.Context, url string) (*Response, error) {
	req, err := s.client.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	return s.client.Do(ctx, req, nil)
}

// DeleteRepoSecret deletes a Dependabot secret in a repository using the secret name.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#delete-a-repository-secret
func (s *DependabotService) DeleteRepoSecret(ctx context.Context, owner, repo, name string) (*Response, error) {
	url := fmt.Sprintf("repos/%v/%v/dependabot/secrets/%v", owner, repo, name)
	return s.deleteSecret(ctx, url)
}

// DeleteOrgSecret deletes a Dependabot secret in an organization using the secret name.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#delete-an-organization-secret
func (s *DependabotService) DeleteOrgSecret(ctx context.Context, org, name string) (*Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v", org, name)
	return s.deleteSecret(ctx, url)
}

// ListSelectedReposForOrgSecret lists all repositories that have access to a Dependabot secret.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#list-selected-repositories-for-an-organization-secret
func (s *DependabotService) ListSelectedReposForOrgSecret(ctx context.Context, org, name string, opts *ListOptions) (*SelectedReposList, *Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v/repositories", org, name)
	u, err := addOptions(url, opts)
	if err != nil {
		return nil, nil, err
	}

	req, err := s.client.NewRequest("GET", u, nil)
	if err != nil {
		return nil, nil, err
	}

	result := new(SelectedReposList)
	resp, err := s.client.Do(ctx, req, result)
	if err != nil {
		return nil, resp, err
	}

	return result, resp, nil
}

// SetSelectedReposForOrgSecret sets the repositories that have access to a Dependabot secret.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#set-selected-repositories-for-an-organization-secret
func (s *DependabotService) SetSelectedReposForOrgSecret(ctx context.Context, org, name string, ids SelectedRepoIDs) (*Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v/repositories", org, name)
	type repoIDs struct {
		SelectedIDs SelectedRepoIDs `json:"selected_repository_ids"`
	}

	req, err := s.client.NewRequest("PUT", url, repoIDs{SelectedIDs: ids})
	if err != nil {
		return nil, err
	}

	return s.client.Do(ctx, req, nil)
}

// AddSelectedRepoToOrgSecret adds a repository to an organization Dependabot secret.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#add-selected-repository-to-an-organization-secret
func (s *DependabotService) AddSelectedRepoToOrgSecret(ctx context.Context, org, name string, repo *Repository) (*Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v/repositories/%v", org, name, *repo.ID)
	req, err := s.client.NewRequest("PUT", url, nil)
	if err != nil {
		return nil, err
	}

	return s.client.Do(ctx, req, nil)
}

// RemoveSelectedRepoFromOrgSecret removes a repository from an organization Dependabot secret.
//
// GitHub API docs: https://docs.github.com/en/rest/reference/dependabot#remove-selected-repository-from-an-organization-secret
func (s *DependabotService) RemoveSelectedRepoFromOrgSecret(ctx context.Context, org, name string, repo *Repository) (*Response, error) {
	url := fmt.Sprintf("orgs/%v/dependabot/secrets/%v/repositories/%v", org, name, *repo.ID)
	req, err := s.client.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	return s.client.Do(ctx, req, nil)
}
