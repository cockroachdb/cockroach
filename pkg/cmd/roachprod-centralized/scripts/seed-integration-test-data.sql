-- Copyright 2026 The Cockroach Authors.
--
-- Use of this software is governed by the CockroachDB Software License
-- included in the /LICENSE file.

-- Seed test data for authentication and authorization testing
-- Run this against your roachprod-centralized database
--
-- Schema overview:
--   - users: human users provisioned via SCIM
--   - groups: groups provisioned via SCIM
--   - group_members: user-group associations (managed via SCIM)
--   - group_permissions: maps group names to permissions (configured by admins)
--   - service_accounts: machine accounts for API access
--   - service_account_permissions: permissions granted to service accounts
--   - service_account_origins: IP allowlists for service accounts
--   - api_tokens: hashed tokens for authentication

-- Clean up any existing test data (in order to avoid foreign key violations)
-- This includes both seed data AND resources created by manual-tests.sh
DELETE FROM api_tokens WHERE user_id IN (SELECT id FROM users WHERE email LIKE '%@test.example.com');
DELETE FROM api_tokens WHERE service_account_id IN (SELECT id FROM service_accounts WHERE name LIKE 'test-%');
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email LIKE '%@test.example.com');
DELETE FROM group_members WHERE group_id IN (SELECT id FROM groups WHERE display_name LIKE 'Test-%');
DELETE FROM service_account_permissions WHERE service_account_id IN (SELECT id FROM service_accounts WHERE name LIKE 'test-%');
DELETE FROM service_account_origins WHERE service_account_id IN (SELECT id FROM service_accounts WHERE name LIKE 'test-%');
DELETE FROM service_accounts WHERE name LIKE 'test-%';
DELETE FROM users WHERE email LIKE '%@test.example.com';
DELETE FROM groups WHERE display_name LIKE 'Test-%';
DELETE FROM group_permissions WHERE group_name LIKE 'Test-%';

-- Also clean up resources that may have been created by manual-tests.sh if it failed mid-run
DELETE FROM api_tokens WHERE user_id IN (SELECT id FROM users WHERE email = 'manual-test@test.example.com');
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email = 'manual-test@test.example.com');
DELETE FROM users WHERE email = 'manual-test@test.example.com';
DELETE FROM group_members WHERE group_id IN (SELECT id FROM groups WHERE display_name LIKE 'Test-Manual-%');
DELETE FROM groups WHERE display_name LIKE 'Test-Manual-%';
DELETE FROM group_permissions WHERE group_name LIKE 'Test-Manual-%';

-- Clean up SCIM test resources from manual-tests.sh sections 15-18
-- Section 15: Pagination test users
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email LIKE 'pagination-user-%@test.example.com');
DELETE FROM users WHERE email LIKE 'pagination-user-%@test.example.com';

-- Section 16: Transformation test users
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email IN ('transform-test@test.example.com', 'multi-email@test.example.com'));
DELETE FROM users WHERE email IN ('transform-test@test.example.com', 'multi-email@test.example.com');

-- Section 17: Combined filter+pagination test users
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email LIKE 'combined-active-%@test.example.com' OR email LIKE 'combined-inactive-%@test.example.com');
DELETE FROM users WHERE email LIKE 'combined-active-%@test.example.com' OR email LIKE 'combined-inactive-%@test.example.com';

-- Section 18: Batch group member test resources
DELETE FROM group_members WHERE group_id IN (SELECT id FROM groups WHERE display_name = 'Batch Test Group');
DELETE FROM groups WHERE display_name = 'Batch Test Group';
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email LIKE 'batch-user-%@test.example.com');
DELETE FROM users WHERE email LIKE 'batch-user-%@test.example.com';

-- Section 20: Admin API User Management test resources
DELETE FROM api_tokens WHERE user_id IN (SELECT id FROM users WHERE email LIKE 'admin-api-user-%@test.example.com');
DELETE FROM group_members WHERE user_id IN (SELECT id FROM users WHERE email LIKE 'admin-api-user-%@test.example.com');
DELETE FROM users WHERE email LIKE 'admin-api-user-%@test.example.com';

-- Section 21: Admin API Group Management test resources
DELETE FROM group_members WHERE group_id IN (SELECT id FROM groups WHERE display_name LIKE 'Admin API Test Group%');
DELETE FROM groups WHERE display_name LIKE 'Admin API Test Group%';
DELETE FROM groups WHERE external_id LIKE 'okta-admin-api-group-%';

-- ============================================================================
-- 1. Create test users
-- ============================================================================
-- These users represent different roles within the organization:
--   - Alice: Admin with full access (Test Engineering team)
--   - Bob: Engineer with limited scope (Engineering division)
--   - Charlie: Contractor with minimal access
--   - Diana: Deactivated user (should not be able to authenticate)
--   - Eve: Revenue division user (different cloud access)
--   - Frank: New user with no group memberships
--   - Grace: User in multiple groups

INSERT INTO users (id, okta_user_id, email, full_name, slack_handle, active, created_at, updated_at)
VALUES
  ('11111111-1111-1111-1111-111111111111', 'okta-user-1', 'alice@test.example.com', 'Alice Admin', 'alice', true, now(), now()),
  ('22222222-2222-2222-2222-222222222222', 'okta-user-2', 'bob@test.example.com', 'Bob Engineer', 'bob', true, now(), now()),
  ('33333333-3333-3333-3333-333333333333', 'okta-user-3', 'charlie@test.example.com', 'Charlie Contractor', 'charlie', true, now(), now()),
  ('44444444-4444-4444-4444-444444444444', 'okta-user-4', 'diana@test.example.com', 'Diana Deactivated', 'diana', false, now(), now()),
  ('55555555-5555-5555-5555-555555555555', 'okta-user-5', 'eve@test.example.com', 'Eve Revenue', 'eve', true, now(), now()),
  ('66666666-6666-6666-6666-666666666666', 'okta-user-6', 'frank@test.example.com', 'Frank Newbie', 'frank', true, now(), now()),
  ('77777777-7777-7777-7777-777777777777', 'okta-user-7', 'grace@test.example.com', 'Grace MultiGroup', 'grace', true, now(), now());

-- ============================================================================
-- 2. Create test groups (SCIM-provisioned groups)
-- ============================================================================
-- Groups represent organizational units and are synced from Okta via SCIM.
-- Permissions are derived from group_permissions table based on display_name.

INSERT INTO groups (id, external_id, display_name, created_at, updated_at)
VALUES
  -- Division-level groups (broad access)
  ('aaaa1111-1111-1111-1111-111111111111', 'okta-group-eng', 'Test-Division-Engineering', now(), now()),
  ('aaaa2222-2222-2222-2222-222222222222', 'okta-group-rev', 'Test-Division-Revenue', now(), now()),

  -- Team-level groups (specific access)
  ('bbbb1111-1111-1111-1111-111111111111', 'okta-group-test-eng', 'Test-SubTeam-TestEngineering', now(), now()),
  ('bbbb2222-2222-2222-2222-222222222222', 'okta-group-sec-eng', 'Test-SubTeam-SecurityEngineering', now(), now()),
  ('bbbb3333-3333-3333-3333-333333333333', 'okta-group-contractors', 'Test-SubTeam-Contractors', now(), now()),

  -- Empty group for SCIM testing
  ('cccc1111-1111-1111-1111-111111111111', 'okta-group-empty', 'Test-Empty-Group', now(), now()),

  -- Large group for pagination testing
  ('dddd1111-1111-1111-1111-111111111111', 'okta-group-all', 'Test-All-Employees', now(), now());

-- ============================================================================
-- 3. Create group memberships
-- ============================================================================
-- These represent the SCIM-synced group memberships from Okta.

INSERT INTO group_members (id, group_id, user_id, created_at)
VALUES
  -- Alice is in Test Engineering (admin team)
  (gen_random_uuid(), 'bbbb1111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111', now()),

  -- Bob is in Engineering division
  (gen_random_uuid(), 'aaaa1111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', now()),

  -- Charlie is in Contractors team
  (gen_random_uuid(), 'bbbb3333-3333-3333-3333-333333333333', '33333333-3333-3333-3333-333333333333', now()),

  -- Diana (deactivated) is still in a group but should fail auth
  (gen_random_uuid(), 'aaaa1111-1111-1111-1111-111111111111', '44444444-4444-4444-4444-444444444444', now()),

  -- Eve is in Revenue division
  (gen_random_uuid(), 'aaaa2222-2222-2222-2222-222222222222', '55555555-5555-5555-5555-555555555555', now()),

  -- Frank has no group memberships (no entry here)

  -- Grace is in multiple groups (Engineering division + Security Engineering)
  (gen_random_uuid(), 'aaaa1111-1111-1111-1111-111111111111', '77777777-7777-7777-7777-777777777777', now()),
  (gen_random_uuid(), 'bbbb2222-2222-2222-2222-222222222222', '77777777-7777-7777-7777-777777777777', now()),

  -- All-Employees group has multiple members for pagination testing
  (gen_random_uuid(), 'dddd1111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111', now()),
  (gen_random_uuid(), 'dddd1111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', now()),
  (gen_random_uuid(), 'dddd1111-1111-1111-1111-111111111111', '33333333-3333-3333-3333-333333333333', now()),
  (gen_random_uuid(), 'dddd1111-1111-1111-1111-111111111111', '55555555-5555-5555-5555-555555555555', now()),
  (gen_random_uuid(), 'dddd1111-1111-1111-1111-111111111111', '77777777-7777-7777-7777-777777777777', now());

-- ============================================================================
-- 4. Create group permissions
-- ============================================================================
-- These define what permissions each group grants.
-- Permissions are matched by group display_name and scope.

-- Test Engineering team gets admin permissions everywhere (SCIM management + all cluster access)
INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
VALUES
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'clusters:create', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'clusters:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'clusters:update:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'clusters:delete:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'clusters:sync', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'tasks:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:scim:manage-user', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:service-accounts:create', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:service-accounts:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:service-accounts:update:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:service-accounts:delete:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:service-accounts:mint:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:tokens:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:tokens:view:own', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:tokens:revoke:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-TestEngineering', '*', 'auth:tokens:revoke:own', now(), now());

-- Security Engineering team also gets admin permissions everywhere
INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
VALUES
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'clusters:create', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'clusters:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'clusters:update:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'clusters:delete:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'tasks:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'auth:scim:manage-user', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'auth:service-accounts:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'auth:tokens:view:all', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-SecurityEngineering', '*', 'auth:tokens:revoke:all', now(), now());

-- Engineering division gets access to GCP and AWS engineering accounts
INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
VALUES
  (gen_random_uuid(), 'Test-Division-Engineering', 'gcp-cockroach-ephemeral', 'clusters:create', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'gcp-cockroach-ephemeral', 'clusters:view:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'gcp-cockroach-ephemeral', 'clusters:update:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'gcp-cockroach-ephemeral', 'clusters:delete:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'aws-engineering-account', 'clusters:create', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'aws-engineering-account', 'clusters:view:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'aws-engineering-account', 'clusters:update:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', 'aws-engineering-account', 'clusters:delete:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', '*', 'clusters:view:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', '*', 'clusters:delete:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', '*', 'tasks:view:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', '*', 'auth:tokens:view:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', '*', 'auth:tokens:revoke:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Engineering', '*', 'auth:service-accounts:view:own', now(), now());

-- Revenue division gets access only to AWS revenue account
INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
VALUES
  (gen_random_uuid(), 'Test-Division-Revenue', 'aws-revenue-account', 'clusters:create', now(), now()),
  (gen_random_uuid(), 'Test-Division-Revenue', 'aws-revenue-account', 'clusters:view:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Revenue', 'aws-revenue-account', 'clusters:update:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Revenue', 'aws-revenue-account', 'clusters:delete:all', now(), now()),
  (gen_random_uuid(), 'Test-Division-Revenue', '*', 'clusters:view:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Revenue', '*', 'auth:tokens:view:own', now(), now()),
  (gen_random_uuid(), 'Test-Division-Revenue', '*', 'auth:tokens:revoke:own', now(), now());

-- Contractors have limited permissions (only view and delete their own clusters)
INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
VALUES
  (gen_random_uuid(), 'Test-SubTeam-Contractors', 'gcp-cockroach-ephemeral', 'clusters:view:own', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-Contractors', 'gcp-cockroach-ephemeral', 'clusters:delete:own', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-Contractors', '*', 'auth:tokens:view:own', now(), now()),
  (gen_random_uuid(), 'Test-SubTeam-Contractors', '*', 'auth:tokens:revoke:own', now(), now());

-- All-Employees group gets basic self-service permissions
INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
VALUES
  (gen_random_uuid(), 'Test-All-Employees', '*', 'clusters:view:own', now(), now()),
  (gen_random_uuid(), 'Test-All-Employees', '*', 'auth:tokens:view:own', now(), now()),
  (gen_random_uuid(), 'Test-All-Employees', '*', 'auth:tokens:revoke:own', now(), now());

-- ============================================================================
-- 5. Create service accounts
-- ============================================================================
-- Service accounts can be "orphan" (delegated_from = NULL, have their own permissions)
-- or "delegated" (delegated_from = user_id, inherit permissions from that user).
INSERT INTO service_accounts (id, name, description, created_by, delegated_from, enabled, created_at, updated_at)
VALUES
  -- Orphan SAs (delegated_from = NULL) - have their own explicit permissions
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'test-scim-provisioner', 'Service account for SCIM provisioning (Okta)', '11111111-1111-1111-1111-111111111111', NULL, true, now(), now()),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'test-ci-automation', 'Service account for CI/CD automation', '11111111-1111-1111-1111-111111111111', NULL, true, now(), now()),
  ('cccccccc-cccc-cccc-cccc-cccccccccccc', 'test-monitoring', 'Service account for monitoring (read-only)', '11111111-1111-1111-1111-111111111111', NULL, true, now(), now()),
  ('dddddddd-dddd-dddd-dddd-dddddddddddd', 'test-disabled-sa', 'Disabled service account for testing', '11111111-1111-1111-1111-111111111111', NULL, false, now(), now()),
  ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', 'test-no-origins', 'Service account with no IP restrictions', '11111111-1111-1111-1111-111111111111', NULL, true, now(), now()),

  -- Delegated SAs (delegated_from = created_by) - inherit permissions from the delegated user
  -- These SAs do NOT have entries in service_account_permissions; they inherit from user's groups.
  ('ffffffff-ffff-ffff-ffff-ffffffffffff', 'test-alice-delegated', 'SA delegated from Alice (inherits her admin permissions)', '11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111', true, now(), now()),
  ('eeee1111-1111-1111-1111-111111111111', 'test-bob-delegated', 'SA delegated from Bob (inherits his limited permissions)', '22222222-2222-2222-2222-222222222222', '22222222-2222-2222-2222-222222222222', true, now(), now());

-- ============================================================================
-- 6. Create service account permissions
-- ============================================================================

-- SCIM provisioner needs scim:manage-user permission and service account management
INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
VALUES
  (gen_random_uuid(), 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '*', 'auth:scim:manage-user', now()),
  (gen_random_uuid(), 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '*', 'auth:service-accounts:create', now());

-- CI automation needs full cluster permissions on GCP ephemeral
INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
VALUES
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'gcp-cockroach-ephemeral', 'clusters:create', now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'gcp-cockroach-ephemeral', 'clusters:view:all', now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'gcp-cockroach-ephemeral', 'clusters:update:all', now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'gcp-cockroach-ephemeral', 'clusters:delete:all', now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '*', 'auth:tokens:view:own', now());

-- Monitoring service account has read-only permissions everywhere
INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
VALUES
  (gen_random_uuid(), 'cccccccc-cccc-cccc-cccc-cccccccccccc', '*', 'clusters:view:all', now()),
  (gen_random_uuid(), 'cccccccc-cccc-cccc-cccc-cccccccccccc', '*', 'tasks:view:all', now());

-- Disabled SA has permissions but is disabled (should fail auth)
INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
VALUES
  (gen_random_uuid(), 'dddddddd-dddd-dddd-dddd-dddddddddddd', '*', 'clusters:view:all', now());

-- No-origins SA has permissions but no IP restrictions
INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
VALUES
  (gen_random_uuid(), 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', '*', 'clusters:view:all', now()),
  (gen_random_uuid(), 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', '*', 'tasks:view:all', now());

-- ============================================================================
-- 7. Create service account IP allowlists (origins)
-- ============================================================================

INSERT INTO service_account_origins (id, service_account_id, cidr, description, created_by, created_at, updated_at)
VALUES
  -- SCIM provisioner: include localhost for testing + internal networks for production
  (gen_random_uuid(), 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '127.0.0.1/32', 'Localhost (testing)', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '::1/128', 'Localhost IPv6 (testing)', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '10.0.0.0/8', 'Internal network', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '172.16.0.0/12', 'Okta datacenter', '11111111-1111-1111-1111-111111111111', now(), now()),

  -- CI automation: include localhost for testing + CI networks
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '127.0.0.1/32', 'Localhost (testing)', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '::1/128', 'Localhost IPv6 (testing)', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '192.168.1.0/24', 'CI/CD runners', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '10.100.0.0/16', 'Build infrastructure', '11111111-1111-1111-1111-111111111111', now(), now()),

  -- Monitoring from anywhere (commonly needed for external monitoring)
  (gen_random_uuid(), 'cccccccc-cccc-cccc-cccc-cccccccccccc', '0.0.0.0/0', 'Allow from anywhere', '11111111-1111-1111-1111-111111111111', now(), now()),

  -- Delegated SA origins (localhost for testing)
  (gen_random_uuid(), 'ffffffff-ffff-ffff-ffff-ffffffffffff', '127.0.0.1/32', 'Localhost (testing)', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'ffffffff-ffff-ffff-ffff-ffffffffffff', '::1/128', 'Localhost IPv6 (testing)', '11111111-1111-1111-1111-111111111111', now(), now()),
  (gen_random_uuid(), 'eeee1111-1111-1111-1111-111111111111', '127.0.0.1/32', 'Localhost (testing)', '22222222-2222-2222-2222-222222222222', now(), now()),
  (gen_random_uuid(), 'eeee1111-1111-1111-1111-111111111111', '::1/128', 'Localhost IPv6 (testing)', '22222222-2222-2222-2222-222222222222', now(), now());

-- Note: test-no-origins SA has no origins (should work from any IP unless the code requires origins)
-- Note: test-disabled-sa has no origins (doesn't matter since it's disabled)

-- ============================================================================
-- 8. Create sample API tokens
-- ============================================================================
-- NOTE: These are example hashes - in real usage, tokens are generated via the API.
-- Token format: rp$user$1$<43-random-chars> or rp$sa$1$<43-random-chars>
-- The hash stored is SHA-256 of the full token.
--
-- For testing, we're inserting token hashes directly into the database.
-- In production, tokens should ONLY be generated via the API endpoints.
--
-- The hashes are computed using: encode(digest(token, 'sha256'), 'hex')

-- Alice's token (admin user)
-- Token: rp$user$1$TEST_ALICE_TOKEN_123456789012345678901234
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$user$1$TEST_ALICE_TOKEN_123456789012345678901234', 'sha256'), 'hex'),
   '901234',
   'user',
   '11111111-1111-1111-1111-111111111111',
   NULL,
   'valid',
   now(),
   now(),
   now() + interval '7 days',
   NULL);

-- Bob's token (engineer)
-- Token: rp$user$1$TEST_BOB_TOKEN_12345678901234567890123456
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$user$1$TEST_BOB_TOKEN_12345678901234567890123456', 'sha256'), 'hex'),
   '123456',
   'user',
   '22222222-2222-2222-2222-222222222222',
   NULL,
   'valid',
   now(),
   now(),
   now() + interval '7 days',
   NULL);

-- Charlie's token (contractor)
-- Token: rp$user$1$TEST_CHARLIE_TOKEN_1234567890123456789012
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$user$1$TEST_CHARLIE_TOKEN_1234567890123456789012', 'sha256'), 'hex'),
   '789012',
   'user',
   '33333333-3333-3333-3333-333333333333',
   NULL,
   'valid',
   now(),
   now(),
   now() + interval '7 days',
   NULL);

-- Grace's token (multi-group user)
-- Token: rp$user$1$TEST_GRACE_TOKEN_12345678901234567890123
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$user$1$TEST_GRACE_TOKEN_12345678901234567890123', 'sha256'), 'hex'),
   '890123',
   'user',
   '77777777-7777-7777-7777-777777777777',
   NULL,
   'valid',
   now(),
   now(),
   now() + interval '7 days',
   NULL);

-- SCIM provisioner SA token
-- Token: rp$sa$1$TEST_SCIM_TOKEN_1234567890123456789012345
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$sa$1$TEST_SCIM_TOKEN_1234567890123456789012345', 'sha256'), 'hex'),
   '012345',
   'service-account',
   NULL,
   'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
   'valid',
   now(),
   now(),
   now() + interval '30 days',
   NULL);

-- CI automation SA token
-- Token: rp$sa$1$TEST_CI_TOKEN_12345678901234567890123456
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$sa$1$TEST_CI_TOKEN_12345678901234567890123456', 'sha256'), 'hex'),
   '123456',
   'service-account',
   NULL,
   'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
   'valid',
   now(),
   now(),
   now() + interval '90 days',
   NULL);

-- Monitoring SA token
-- Token: rp$sa$1$TEST_MONITOR_TOKEN_123456789012345678901
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$sa$1$TEST_MONITOR_TOKEN_123456789012345678901', 'sha256'), 'hex'),
   '678901',
   'service-account',
   NULL,
   'cccccccc-cccc-cccc-cccc-cccccccccccc',
   'valid',
   now(),
   now(),
   now() + interval '365 days',
   NULL);

-- No-origins SA token (for testing IP validation bypass)
-- Token: rp$sa$1$TEST_NOORIGIN_TOKEN_12345678901234567890
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$sa$1$TEST_NOORIGIN_TOKEN_12345678901234567890', 'sha256'), 'hex'),
   '567890',
   'service-account',
   NULL,
   'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee',
   'valid',
   now(),
   now(),
   now() + interval '30 days',
   NULL);

-- Alice's delegated SA token (inherits Alice's admin permissions)
-- Token: rp$sa$1$TEST_ALICE_DELEGATED_TOKEN_12345678901234
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$sa$1$TEST_ALICE_DELEGATED_TOKEN_12345678901234', 'sha256'), 'hex'),
   '901234',
   'service-account',
   NULL,
   'ffffffff-ffff-ffff-ffff-ffffffffffff',
   'valid',
   now(),
   now(),
   now() + interval '30 days',
   NULL);

-- Bob's delegated SA token (inherits Bob's limited permissions)
-- Token: rp$sa$1$TEST_BOB_DELEGATED_TOKEN_123456789012345
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$sa$1$TEST_BOB_DELEGATED_TOKEN_123456789012345', 'sha256'), 'hex'),
   '012345',
   'service-account',
   NULL,
   'eeee1111-1111-1111-1111-111111111111',
   'valid',
   now(),
   now(),
   now() + interval '30 days',
   NULL);

-- Revoked token (for testing revocation logic)
-- Token: rp$user$1$TEST_REVOKED_TOKEN_1234567890123456789
-- Note: updated_at reflects when the token was revoked (1 hour ago)
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$user$1$TEST_REVOKED_TOKEN_1234567890123456789', 'sha256'), 'hex'),
   '456789',
   'user',
   '22222222-2222-2222-2222-222222222222',
   NULL,
   'revoked',
   now() - interval '1 day',
   now() - interval '1 hour',
   now() + interval '6 days',
   now() - interval '1 hour');

-- Expired token (for testing expiration logic)
-- Token: rp$user$1$TEST_EXPIRED_TOKEN_1234567890123456789
INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, last_used_at)
VALUES
  (gen_random_uuid(),
   encode(digest('rp$user$1$TEST_EXPIRED_TOKEN_1234567890123456789', 'sha256'), 'hex'),
   '234567',
   'user',
   '22222222-2222-2222-2222-222222222222',
   NULL,
   'valid',
   now() - interval '10 days',
   now() - interval '10 days',
   now() - interval '3 days',
   now() - interval '4 days');

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

SELECT '=== USERS ===' AS section;
SELECT id, email, full_name, active FROM users WHERE email LIKE '%@test.example.com' ORDER BY email;

SELECT '=== GROUPS ===' AS section;
SELECT id, external_id, display_name FROM groups WHERE display_name LIKE 'Test-%' ORDER BY display_name;

SELECT '=== GROUP MEMBERS ===' AS section;
SELECT g.display_name AS group_name, u.email AS user_email
FROM group_members gm
JOIN groups g ON gm.group_id = g.id
JOIN users u ON gm.user_id = u.id
WHERE g.display_name LIKE 'Test-%'
ORDER BY g.display_name, u.email;

SELECT '=== GROUP PERMISSIONS ===' AS section;
SELECT group_name, scope, permission
FROM group_permissions
WHERE group_name LIKE 'Test-%'
ORDER BY group_name, scope, permission;

SELECT '=== USER EFFECTIVE PERMISSIONS (via groups) ===' AS section;
SELECT DISTINCT u.email, gp.scope, gp.permission
FROM users u
JOIN group_members gm ON gm.user_id = u.id
JOIN groups g ON g.id = gm.group_id
JOIN group_permissions gp ON gp.group_name = g.display_name
WHERE u.email LIKE '%@test.example.com'
ORDER BY u.email, gp.scope, gp.permission;

SELECT '=== SERVICE ACCOUNTS ===' AS section;
SELECT id, name, description, enabled FROM service_accounts WHERE name LIKE 'test-%' ORDER BY name;

SELECT '=== SERVICE ACCOUNT PERMISSIONS ===' AS section;
SELECT sa.name, sap.scope, sap.permission
FROM service_account_permissions sap
JOIN service_accounts sa ON sap.service_account_id = sa.id
WHERE sa.name LIKE 'test-%'
ORDER BY sa.name, sap.scope, sap.permission;

SELECT '=== SERVICE ACCOUNT ORIGINS (IP ALLOWLISTS) ===' AS section;
SELECT sa.name, sao.cidr, sao.description
FROM service_account_origins sao
JOIN service_accounts sa ON sao.service_account_id = sa.id
WHERE sa.name LIKE 'test-%'
ORDER BY sa.name, sao.cidr;

SELECT '=== API TOKENS ===' AS section;
SELECT
  at.id,
  at.token_suffix,
  at.token_type,
  COALESCE(u.email, sa.name) AS principal,
  at.status,
  at.expires_at,
  at.last_used_at
FROM api_tokens at
LEFT JOIN users u ON at.user_id = u.id
LEFT JOIN service_accounts sa ON at.service_account_id = sa.id
WHERE (u.email LIKE '%@test.example.com' OR sa.name LIKE 'test-%')
ORDER BY at.created_at DESC;

-- ============================================================================
-- TEST TOKEN REFERENCE
-- ============================================================================
-- Use these tokens for manual testing (copy-paste into Authorization header):
--
-- User Tokens:
--   Alice (Admin):     rp$user$1$TEST_ALICE_TOKEN_123456789012345678901234
--   Bob (Engineer):    rp$user$1$TEST_BOB_TOKEN_12345678901234567890123456
--   Charlie (Contractor): rp$user$1$TEST_CHARLIE_TOKEN_1234567890123456789012
--   Grace (MultiGroup): rp$user$1$TEST_GRACE_TOKEN_12345678901234567890123
--
-- Service Account Tokens (Orphan - have their own permissions):
--   SCIM Provisioner:  rp$sa$1$TEST_SCIM_TOKEN_1234567890123456789012345
--   CI Automation:     rp$sa$1$TEST_CI_TOKEN_12345678901234567890123456
--   Monitoring:        rp$sa$1$TEST_MONITOR_TOKEN_123456789012345678901
--   No-Origins SA:     rp$sa$1$TEST_NOORIGIN_TOKEN_12345678901234567890
--
-- Service Account Tokens (Delegated - inherit permissions from user):
--   Alice Delegated:   rp$sa$1$TEST_ALICE_DELEGATED_TOKEN_12345678901234
--   Bob Delegated:     rp$sa$1$TEST_BOB_DELEGATED_TOKEN_123456789012345
--
-- Invalid Tokens (should fail):
--   Revoked:           rp$user$1$TEST_REVOKED_TOKEN_1234567890123456789
--   Expired:           rp$user$1$TEST_EXPIRED_TOKEN_1234567890123456789
--
-- ============================================================================
