// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/database"

// GetAuthMigrations returns all migrations for the auth repository.
func GetAuthMigrations() []database.Migration {
	return []database.Migration{
		{
			Version:     1,
			Description: "Initial auth tables",
			SQL: `
-- Users table: stores user accounts provisioned via SCIM
CREATE TABLE IF NOT EXISTS users (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  okta_user_id     VARCHAR(255) UNIQUE NOT NULL,
  email            VARCHAR(255) NOT NULL,
  slack_handle     VARCHAR(255),
  full_name        VARCHAR(255),
  active           BOOLEAN NOT NULL DEFAULT true,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at    TIMESTAMPTZ
);

-- Groups table: stores groups provisioned via SCIM
CREATE TABLE IF NOT EXISTS groups (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  external_id  VARCHAR(255) UNIQUE,
  display_name VARCHAR(255) NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_groups_display_name ON groups (display_name);

-- Group members table: many-to-many relationship between users and groups
CREATE TABLE IF NOT EXISTS group_members (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  group_id   UUID NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (group_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_user_id ON group_members (user_id);
CREATE INDEX IF NOT EXISTS idx_group_members_group_id ON group_members (group_id);

-- Group permissions table: maps group display names to permissions
-- Allows multiple permissions per group/provider/account combination
CREATE TABLE IF NOT EXISTS group_permissions (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  group_name VARCHAR(255) NOT NULL,
  provider   VARCHAR(50)  NOT NULL,
  account    VARCHAR(255) NOT NULL,
  permission VARCHAR(50)  NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (group_name, provider, account, permission)
);

-- Service accounts table: stores service accounts for API access
CREATE TABLE IF NOT EXISTS service_accounts (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name           VARCHAR(255) NOT NULL,
  description    TEXT,
  created_by     UUID,
  delegated_from UUID REFERENCES users(id) ON DELETE CASCADE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  enabled        BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_sa_name ON service_accounts (name);

-- Service account origins table: allowed IP ranges for service accounts
CREATE TABLE IF NOT EXISTS service_account_origins (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  service_account_id UUID NOT NULL REFERENCES service_accounts(id) ON DELETE CASCADE,
  cidr               INET NOT NULL,
  description        TEXT,
  created_by         UUID,  -- Can be user ID or service account ID (no FK constraint)
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (service_account_id, cidr)
);

CREATE INDEX IF NOT EXISTS idx_service_account_origins_sa_id ON service_account_origins (service_account_id);

-- Service account permissions table: permissions granted to service accounts
CREATE TABLE IF NOT EXISTS service_account_permissions (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  service_account_id UUID NOT NULL REFERENCES service_accounts(id) ON DELETE CASCADE,
  provider           VARCHAR(50) NOT NULL,
  permission         VARCHAR(50) NOT NULL,
  account            VARCHAR(255),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (service_account_id, provider, account, permission)
);

CREATE INDEX IF NOT EXISTS idx_service_account_permissions_sa_id ON service_account_permissions (service_account_id);

-- API tokens table: stores hashed tokens for authentication
CREATE TABLE IF NOT EXISTS api_tokens (
  id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  token_hash         VARCHAR(64) UNIQUE NOT NULL,
  token_suffix       VARCHAR(30) NOT NULL,
  token_type         VARCHAR(20) NOT NULL,
  user_id            UUID REFERENCES users(id) ON DELETE CASCADE,
  service_account_id UUID REFERENCES service_accounts(id) ON DELETE CASCADE,
  status             VARCHAR(50) NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at         TIMESTAMPTZ NOT NULL,
  activated_at       TIMESTAMPTZ,
  last_used_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_token_hash ON api_tokens (token_hash);
CREATE INDEX IF NOT EXISTS idx_token_suffix ON api_tokens (token_suffix);
CREATE INDEX IF NOT EXISTS idx_status_expire ON api_tokens (status, expires_at);
CREATE INDEX IF NOT EXISTS idx_tokens_user_status ON api_tokens (user_id, status);
CREATE INDEX IF NOT EXISTS idx_tokens_sa_status ON api_tokens (service_account_id, status);
CREATE INDEX IF NOT EXISTS idx_tokens_status_updated ON api_tokens (status, updated_at);
`,
		},
		{
			Version:     2,
			Description: "Replace provider+account with scope",
			SQL: `
-- group_permissions: replace provider+account with scope
-- DEFAULT '*' grants wildcard scope to existing rows. This is safe because the
-- system has not been deployed to production, so no real data needs migration.
-- All statements use IF NOT EXISTS / IF EXISTS to be idempotent in case a
-- partial run left the schema in an intermediate state.
ALTER TABLE group_permissions ADD COLUMN IF NOT EXISTS scope VARCHAR(255) NOT NULL DEFAULT '*';
DROP INDEX IF EXISTS group_permissions_group_name_provider_account_permission_key CASCADE;
ALTER TABLE group_permissions DROP COLUMN IF EXISTS provider;
ALTER TABLE group_permissions DROP COLUMN IF EXISTS account;
CREATE UNIQUE INDEX IF NOT EXISTS group_permissions_group_name_scope_permission_key
    ON group_permissions (group_name, scope, permission);

-- service_account_permissions: replace provider+account with scope
-- DEFAULT '*' grants wildcard scope to existing rows. This is safe because the
-- system has not been deployed to production, so no real data needs migration.
ALTER TABLE service_account_permissions ADD COLUMN IF NOT EXISTS scope VARCHAR(255) NOT NULL DEFAULT '*';
DROP INDEX IF EXISTS service_account_permissions_service_account_id_provider_acco_key CASCADE;
ALTER TABLE service_account_permissions DROP COLUMN IF EXISTS provider;
ALTER TABLE service_account_permissions DROP COLUMN IF EXISTS account;
CREATE UNIQUE INDEX IF NOT EXISTS service_account_permissions_sa_scope_permission_key
    ON service_account_permissions (service_account_id, scope, permission);
`,
		},
	}
}
