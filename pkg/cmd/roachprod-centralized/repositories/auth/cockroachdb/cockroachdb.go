// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	gosql "database/sql"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	rauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/repositories/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters"
	filtertypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/filters/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CRDBAuthRepo is a CockroachDB implementation of the auth repository.
type CRDBAuthRepo struct {
	db *gosql.DB
}

// NewAuthRepository creates a new CockroachDB auth repository.
func NewAuthRepository(db *gosql.DB) *CRDBAuthRepo {
	return &CRDBAuthRepo{
		db: db,
	}
}

// Ensure CRDBAuthRepo implements IAuthRepository
var _ rauth.IAuthRepository = &CRDBAuthRepo{}

// Users

func (r *CRDBAuthRepo) GetUser(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.User, error) {
	query := `
		SELECT id, okta_user_id, email, slack_handle, full_name, active, created_at, updated_at, last_login_at
		FROM users WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())
	return r.scanUser(row)
}

func (r *CRDBAuthRepo) GetUserByOktaID(
	ctx context.Context, l *logger.Logger, oktaID string,
) (*auth.User, error) {
	query := `
		SELECT id, okta_user_id, email, slack_handle, full_name, active, created_at, updated_at, last_login_at
		FROM users WHERE okta_user_id = $1
	`
	row := r.db.QueryRowContext(ctx, query, oktaID)
	return r.scanUser(row)
}

func (r *CRDBAuthRepo) scanUser(
	scanner interface {
		Scan(dest ...interface{}) error
	},
) (*auth.User, error) {
	var id string
	var oktaUserID, email string
	var slackHandle, fullName gosql.NullString
	var active bool
	var createdAt, updatedAt time.Time
	var lastLoginAt gosql.NullTime

	err := scanner.Scan(
		&id, &oktaUserID, &email, &slackHandle, &fullName, &active,
		&createdAt, &updatedAt, &lastLoginAt,
	)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rauth.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to scan user")
	}

	userID, err := uuid.FromString(id)
	if err != nil {
		return nil, errors.Wrap(err, "invalid user id in db")
	}

	user := &auth.User{
		ID:         userID,
		OktaUserID: oktaUserID,
		Email:      email,
		Active:     active,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}

	if slackHandle.Valid {
		user.SlackHandle = slackHandle.String
	}
	if fullName.Valid {
		user.FullName = fullName.String
	}
	if lastLoginAt.Valid {
		user.LastLoginAt = &lastLoginAt.Time
	}

	return user, nil
}

// Service Accounts

func (r *CRDBAuthRepo) GetServiceAccount(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ServiceAccount, error) {
	query := `
		SELECT id, name, description, created_by, created_at, updated_at, enabled, delegated_from
		FROM service_accounts WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())
	return r.scanServiceAccount(row)
}

func (r *CRDBAuthRepo) scanServiceAccount(
	scanner interface {
		Scan(dest ...interface{}) error
	},
) (*auth.ServiceAccount, error) {
	var id, name string
	var description, createdBy, delegatedFrom gosql.NullString
	var enabled bool
	var createdAt, updatedAt time.Time

	err := scanner.Scan(
		&id, &name, &description, &createdBy, &createdAt, &updatedAt, &enabled, &delegatedFrom,
	)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rauth.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to scan service account")
	}

	saID, err := uuid.FromString(id)
	if err != nil {
		return nil, errors.Wrap(err, "invalid sa id in db")
	}

	sa := &auth.ServiceAccount{
		ID:        saID,
		Name:      name,
		Enabled:   enabled,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	if description.Valid {
		sa.Description = description.String
	}
	if createdBy.Valid {
		uid, err := uuid.FromString(createdBy.String)
		if err != nil {
			return nil, errors.Wrap(err, "invalid created_by UUID in database")
		}
		sa.CreatedBy = &uid
	}
	if delegatedFrom.Valid {
		uid, err := uuid.FromString(delegatedFrom.String)
		if err != nil {
			return nil, errors.Wrap(err, "invalid delegated_from UUID in database")
		}
		sa.DelegatedFrom = &uid
	}

	return sa, nil
}

// Service Account Origins

func (r *CRDBAuthRepo) ListServiceAccountOrigins(
	ctx context.Context, l *logger.Logger, saID uuid.UUID, filterSet filtertypes.FilterSet,
) ([]*auth.ServiceAccountOrigin, int, error) {
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.ServiceAccountOrigin{}),
	)
	baseSelectQuery := `
		SELECT id, service_account_id, cidr, description, created_by, created_at, updated_at
		FROM service_account_origins
	`
	baseCountQuery := "SELECT count(*) FROM service_account_origins"

	// Add required filter for service_account_id
	filterSet.AddFilter("ServiceAccountID", filtertypes.OpEqual, saID)
	filterSet.Logic = filtertypes.LogicAnd

	// Default sort by created_at
	defaultSort := &filtertypes.SortParams{
		SortBy:    "CreatedAt",
		SortOrder: filtertypes.SortAscending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count origins")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list service account origins")
	}
	defer rows.Close()

	var origins []*auth.ServiceAccountOrigin
	for rows.Next() {
		var id, saIDStr, cidr string
		var description, createdBy gosql.NullString
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &saIDStr, &cidr, &description, &createdBy, &createdAt, &updatedAt); err != nil {
			return nil, 0, errors.Wrap(err, "failed to scan origin")
		}

		originID, err := uuid.FromString(id)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid origin id UUID in database")
		}
		saUUID, err := uuid.FromString(saIDStr)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid service_account_id UUID in database")
		}

		origin := &auth.ServiceAccountOrigin{
			ID:               originID,
			ServiceAccountID: saUUID,
			CIDR:             normalizeCIDR(cidr),
			CreatedAt:        createdAt,
			UpdatedAt:        updatedAt,
		}
		if description.Valid {
			origin.Description = description.String
		}
		if createdBy.Valid {
			uid, err := uuid.FromString(createdBy.String)
			if err != nil {
				return nil, 0, errors.Wrap(err, "invalid created_by UUID in database")
			}
			origin.CreatedBy = &uid
		}

		origins = append(origins, origin)
	}

	return origins, totalCount, nil
}

// Tokens

func (r *CRDBAuthRepo) CreateToken(
	ctx context.Context, l *logger.Logger, token *auth.ApiToken,
) error {
	query := `
		INSERT INTO api_tokens (id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, activated_at, last_used_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`
	var userID interface{}
	if token.UserID != nil {
		userID = token.UserID.String()
	}
	var saID interface{}
	if token.ServiceAccountID != nil {
		saID = token.ServiceAccountID.String()
	}

	_, err := r.db.ExecContext(ctx, query,
		token.ID.String(),
		token.TokenHash,
		token.TokenSuffix,
		token.TokenType,
		userID,
		saID,
		token.Status,
		token.CreatedAt,
		token.UpdatedAt,
		token.ExpiresAt,
		token.ActivatedAt,
		token.LastUsedAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create token")
	}
	return nil
}

func (r *CRDBAuthRepo) GetTokenByHash(
	ctx context.Context, l *logger.Logger, hash string,
) (*auth.ApiToken, error) {
	query := `
		SELECT id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, activated_at, last_used_at
		FROM api_tokens WHERE token_hash = $1
	`
	row := r.db.QueryRowContext(ctx, query, hash)
	return r.scanToken(row)
}

func (r *CRDBAuthRepo) GetToken(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ApiToken, error) {
	query := `
		SELECT id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, activated_at, last_used_at
		FROM api_tokens WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())
	return r.scanToken(row)
}

func (r *CRDBAuthRepo) UpdateTokenLastUsed(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	query := `
		UPDATE api_tokens SET last_used_at = $1 WHERE id = $2
	`
	_, err := r.db.ExecContext(ctx, query, timeutil.Now(), id.String())
	if err != nil {
		return errors.Wrap(err, "failed to update token last_used_at")
	}
	return nil
}

func (r *CRDBAuthRepo) RevokeToken(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	query := `
		UPDATE api_tokens SET status = $1, updated_at = $2 WHERE id = $3
	`
	_, err := r.db.ExecContext(ctx, query, auth.TokenStatusRevoked, timeutil.Now(), id.String())
	if err != nil {
		return errors.Wrap(err, "failed to revoke token")
	}
	return nil
}

func (r *CRDBAuthRepo) ListAllTokens(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.ApiToken, int, error) {
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.ApiToken{}),
	)
	baseSelectQuery := `
		SELECT id, token_hash, token_suffix, token_type, user_id, service_account_id, status, created_at, updated_at, expires_at, activated_at, last_used_at
		FROM api_tokens
	`
	baseCountQuery := "SELECT count(*) FROM api_tokens"

	// Default sort by created_at descending
	defaultSort := &filtertypes.SortParams{
		SortBy:    "CreatedAt",
		SortOrder: filtertypes.SortDescending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count tokens")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list all tokens")
	}
	defer rows.Close()

	var tokens []*auth.ApiToken
	for rows.Next() {
		token, err := r.scanToken(rows)
		if err != nil {
			return nil, 0, err
		}
		tokens = append(tokens, token)
	}
	return tokens, totalCount, nil
}

func (r *CRDBAuthRepo) scanToken(
	scanner interface {
		Scan(dest ...interface{}) error
	},
) (*auth.ApiToken, error) {
	var id, tokenHash, tokenSuffix, tokenType, status string
	var userID, saID gosql.NullString
	var createdAt, updatedAt, expiresAt time.Time
	var activatedAt, lastUsedAt gosql.NullTime

	err := scanner.Scan(
		&id, &tokenHash, &tokenSuffix, &tokenType, &userID, &saID, &status,
		&createdAt, &updatedAt, &expiresAt, &activatedAt, &lastUsedAt,
	)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rauth.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to scan token")
	}

	tokenID, err := uuid.FromString(id)
	if err != nil {
		return nil, errors.Wrap(err, "invalid token id in db")
	}

	token := &auth.ApiToken{
		ID:          tokenID,
		TokenHash:   tokenHash,
		TokenSuffix: tokenSuffix,
		TokenType:   auth.TokenType(tokenType),
		Status:      auth.TokenStatus(status),
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		ExpiresAt:   expiresAt,
	}

	if userID.Valid {
		uid, err := uuid.FromString(userID.String)
		if err != nil {
			return nil, errors.Wrap(err, "invalid user_id UUID in database")
		}
		token.UserID = &uid
	}
	if saID.Valid {
		uid, err := uuid.FromString(saID.String)
		if err != nil {
			return nil, errors.Wrap(err, "invalid service_account_id UUID in database")
		}
		token.ServiceAccountID = &uid
	}
	if activatedAt.Valid {
		token.ActivatedAt = &activatedAt.Time
	}
	if lastUsedAt.Valid {
		token.LastUsedAt = &lastUsedAt.Time
	}

	return token, nil
}

// CleanupTokens deletes tokens based on status and retention period.
func (r *CRDBAuthRepo) CleanupTokens(
	ctx context.Context, l *logger.Logger, status auth.TokenStatus, retention time.Duration,
) (int, error) {
	cutoff := timeutil.Now().Add(-retention)

	var query string
	switch status {
	case auth.TokenStatusValid:
		// For valid/expired tokens: check ExpiresAt
		query = `DELETE FROM api_tokens WHERE status = $1 AND expires_at < $2`
	case auth.TokenStatusRevoked:
		// For revoked tokens: check UpdatedAt (when it was revoked)
		query = `DELETE FROM api_tokens WHERE status = $1 AND updated_at < $2`
	default:
		return 0, errors.Newf("unsupported token status for cleanup: %s", status)
	}

	result, err := r.db.ExecContext(ctx, query, status, cutoff)
	if err != nil {
		return 0, errors.Wrap(err, "failed to cleanup tokens")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get rows affected")
	}

	return int(rowsAffected), nil
}

func (r *CRDBAuthRepo) GetUserPermissionsFromGroups(
	ctx context.Context, l *logger.Logger, userID uuid.UUID,
) ([]*auth.GroupPermission, error) {
	// Use DISTINCT ON to deduplicate by (provider, account, permission)
	// This ensures a user gets each permission only once, even if granted by multiple groups
	// New query: joins group_members → groups → group_permissions
	query := `
		SELECT DISTINCT ON (gp.provider, gp.account, gp.permission)
			gp.id, gp.group_name, gp.provider, gp.account, gp.permission, gp.created_at, gp.updated_at
		FROM group_members gm
		JOIN groups g ON gm.group_id = g.id
		JOIN group_permissions gp ON g.display_name = gp.group_name
		WHERE gm.user_id = $1
		ORDER BY gp.provider, gp.account, gp.permission, gp.created_at
	`
	rows, err := r.db.QueryContext(ctx, query, userID.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get user permissions from groups")
	}
	defer rows.Close()

	return r.scanGroupPermissions(rows)
}

// Permissions

func (r *CRDBAuthRepo) ListServiceAccountPermissions(
	ctx context.Context, l *logger.Logger, saID uuid.UUID, filterSet filtertypes.FilterSet,
) ([]*auth.ServiceAccountPermission, int, error) {
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.ServiceAccountPermission{}),
	)
	baseSelectQuery := `
		SELECT id, service_account_id, provider, permission, account, created_at
		FROM service_account_permissions
	`
	baseCountQuery := "SELECT count(*) FROM service_account_permissions"

	// Add required filter for service_account_id
	filterSet.AddFilter("ServiceAccountID", filtertypes.OpEqual, saID)
	filterSet.Logic = filtertypes.LogicAnd

	// Default sort by provider
	defaultSort := &filtertypes.SortParams{
		SortBy:    "Provider",
		SortOrder: filtertypes.SortAscending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count service account permissions")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list service account permissions")
	}
	defer rows.Close()

	var perms []*auth.ServiceAccountPermission
	for rows.Next() {
		var id, saIDStr, provider, permission string
		var account gosql.NullString
		var createdAt time.Time

		if err := rows.Scan(&id, &saIDStr, &provider, &permission, &account, &createdAt); err != nil {
			return nil, 0, errors.Wrap(err, "failed to scan service account permission")
		}

		permID, err := uuid.FromString(id)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid permission id UUID in database")
		}
		sid, err := uuid.FromString(saIDStr)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid service_account_id UUID in database")
		}

		p := &auth.ServiceAccountPermission{
			ID:               permID,
			ServiceAccountID: sid,
			Provider:         provider,
			Permission:       permission,
			CreatedAt:        createdAt,
		}
		if account.Valid {
			p.Account = account.String
		}

		perms = append(perms, p)
	}

	return perms, totalCount, nil
}

// GetServiceAccountPermission retrieves a single permission by ID.
func (r *CRDBAuthRepo) GetServiceAccountPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ServiceAccountPermission, error) {
	query := `
		SELECT id, service_account_id, provider, permission, account, created_at
		FROM service_account_permissions WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())

	var idStr, saIDStr, provider, permission string
	var account gosql.NullString
	var createdAt time.Time

	if err := row.Scan(&idStr, &saIDStr, &provider, &permission, &account, &createdAt); err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rauth.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to scan service account permission")
	}

	permID, _ := uuid.FromString(idStr)
	saID, _ := uuid.FromString(saIDStr)

	return &auth.ServiceAccountPermission{
		ID:               permID,
		ServiceAccountID: saID,
		Provider:         provider,
		Permission:       permission,
		Account:          account.String,
		CreatedAt:        createdAt,
	}, nil
}

func (r *CRDBAuthRepo) scanGroupPermissions(rows *gosql.Rows) ([]*auth.GroupPermission, error) {
	var perms []*auth.GroupPermission
	for rows.Next() {
		var id, groupName, provider, account, permission string
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &groupName, &provider, &account, &permission, &createdAt, &updatedAt); err != nil {
			return nil, errors.Wrap(err, "failed to scan group permission")
		}

		permID, err := uuid.FromString(id)
		if err != nil {
			return nil, errors.Wrap(err, "invalid permission id UUID in database")
		}

		perms = append(perms, &auth.GroupPermission{
			ID:         permID,
			GroupName:  groupName,
			Provider:   provider,
			Account:    account,
			Permission: permission,
			CreatedAt:  createdAt,
			UpdatedAt:  updatedAt,
		})
	}

	return perms, nil
}

// GetStatistics returns current counts for metrics gauges.
func (r *CRDBAuthRepo) GetStatistics(
	ctx context.Context, l *logger.Logger,
) (*rauth.Statistics, error) {
	stats := &rauth.Statistics{
		TokensByTypeAndStatus: make(map[string]map[string]int),
	}

	// Users by status
	userQuery := `SELECT active, count(*) FROM users GROUP BY active`
	rows, err := r.db.QueryContext(ctx, userQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get user statistics")
	}
	defer rows.Close()
	for rows.Next() {
		var active bool
		var count int
		if err := rows.Scan(&active, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan user statistics")
		}
		if active {
			stats.UsersActive = count
		} else {
			stats.UsersInactive = count
		}
	}

	// Groups count
	if err := r.db.QueryRowContext(ctx, `SELECT count(*) FROM groups`).Scan(&stats.Groups); err != nil {
		return nil, errors.Wrap(err, "failed to get group count")
	}

	// Service accounts by enabled
	saQuery := `SELECT enabled, count(*) FROM service_accounts GROUP BY enabled`
	rows, err = r.db.QueryContext(ctx, saQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get service account statistics")
	}
	defer rows.Close()
	for rows.Next() {
		var enabled bool
		var count int
		if err := rows.Scan(&enabled, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan service account statistics")
		}
		if enabled {
			stats.ServiceAccountsEnabled = count
		} else {
			stats.ServiceAccountsDisabled = count
		}
	}

	// Tokens by type and status
	tokenQuery := `SELECT token_type, status, count(*) FROM api_tokens GROUP BY token_type, status`
	rows, err = r.db.QueryContext(ctx, tokenQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get token statistics")
	}
	defer rows.Close()
	for rows.Next() {
		var tokenType, status string
		var count int
		if err := rows.Scan(&tokenType, &status, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan token statistics")
		}
		if stats.TokensByTypeAndStatus[tokenType] == nil {
			stats.TokensByTypeAndStatus[tokenType] = make(map[string]int)
		}
		stats.TokensByTypeAndStatus[tokenType][status] = count
	}

	return stats, nil
}

// normalizeCIDR ensures a CIDR string has a mask suffix.
// CockroachDB's INET type normalizes single-host CIDRs by stripping the mask
// (e.g., "127.0.0.1/32" becomes "127.0.0.1"). This function adds the appropriate
// mask (/32 for IPv4, /128 for IPv6) if missing.
func normalizeCIDR(cidr string) string {
	if strings.Contains(cidr, "/") {
		return cidr
	}
	ip := net.ParseIP(cidr)
	if ip == nil {
		return cidr
	}
	if ip.To4() != nil {
		return cidr + "/32"
	}
	return cidr + "/128"
}
