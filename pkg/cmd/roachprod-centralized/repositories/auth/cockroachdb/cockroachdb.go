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
	"strconv"
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

func (r *CRDBAuthRepo) CreateUser(ctx context.Context, l *logger.Logger, user *auth.User) error {
	query := `
		INSERT INTO users (id, okta_user_id, email, slack_handle, full_name, active, created_at, updated_at, last_login_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.db.ExecContext(ctx, query,
		user.ID.String(),
		user.OktaUserID,
		user.Email,
		user.SlackHandle,
		user.FullName,
		user.Active,
		user.CreatedAt,
		user.UpdatedAt,
		user.LastLoginAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create user")
	}
	return nil
}

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

func (r *CRDBAuthRepo) GetUserByEmail(
	ctx context.Context, l *logger.Logger, email string,
) (*auth.User, error) {
	query := `
		SELECT id, okta_user_id, email, slack_handle, full_name, active, created_at, updated_at, last_login_at
		FROM users WHERE email = $1
	`
	row := r.db.QueryRowContext(ctx, query, email)
	return r.scanUser(row)
}

func (r *CRDBAuthRepo) UpdateUser(ctx context.Context, l *logger.Logger, user *auth.User) error {
	query := `
		UPDATE users
		SET okta_user_id = $1, email = $2, slack_handle = $3, full_name = $4, active = $5, updated_at = $6, last_login_at = $7
		WHERE id = $8
	`
	_, err := r.db.ExecContext(ctx, query,
		user.OktaUserID,
		user.Email,
		user.SlackHandle,
		user.FullName,
		user.Active,
		user.UpdatedAt,
		user.LastLoginAt,
		user.ID.String(),
	)
	if err != nil {
		return errors.Wrap(err, "failed to update user")
	}
	return nil
}

func (r *CRDBAuthRepo) ListUsers(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.User, int, error) {

	// Set the FilteredType to auth.User for proper type and columns handling
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.User{}),
	)
	baseSelectQuery := `
		SELECT id, okta_user_id, email, slack_handle, full_name, active, created_at, updated_at, last_login_at
		FROM users
	`
	baseCountQuery := "SELECT count(*) FROM users"

	// Default sort by email
	defaultSort := &filtertypes.SortParams{
		SortBy:    "Email",
		SortOrder: filtertypes.SortAscending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count users")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list users")
	}
	defer rows.Close()

	var usersList []*auth.User
	for rows.Next() {
		user, err := r.scanUser(rows)
		if err != nil {
			return nil, 0, err
		}
		usersList = append(usersList, user)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating users")
	}
	return usersList, totalCount, nil
}

func (r *CRDBAuthRepo) DeleteUser(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	query := "DELETE FROM users WHERE id = $1"
	_, err := r.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to delete user")
	}
	return nil
}

func (r *CRDBAuthRepo) DeactivateUser(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (retErr error) {
	// Begin transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// 1. Set user to inactive
	updateQuery := `
		UPDATE users
		SET active = false, updated_at = $1
		WHERE id = $2
	`
	now := timeutil.Now()
	result, err := tx.ExecContext(ctx, updateQuery, now, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to deactivate user")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}
	if rowsAffected == 0 {
		return rauth.ErrNotFound
	}

	// 2. Revoke all user's tokens
	revokeQuery := `
		UPDATE api_tokens
		SET status = $1, updated_at = $2
		WHERE user_id = $3 AND status = $4
	`
	_, err = tx.ExecContext(ctx, revokeQuery, auth.TokenStatusRevoked, now, id.String(), auth.TokenStatusValid)
	if err != nil {
		return errors.Wrap(err, "failed to revoke user tokens")
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	committed = true
	return nil
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

func (r *CRDBAuthRepo) CreateServiceAccount(
	ctx context.Context, l *logger.Logger, sa *auth.ServiceAccount,
) error {
	query := `
		INSERT INTO service_accounts (id, name, description, created_by, created_at, updated_at, enabled, delegated_from)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	// Handle created_by as nullable UUID
	var createdBy interface{}
	if sa.CreatedBy != nil {
		createdBy = sa.CreatedBy.String()
	}
	// Handle delegated_from as nullable UUID
	var delegatedFrom interface{}
	if sa.DelegatedFrom != nil {
		delegatedFrom = sa.DelegatedFrom.String()
	}

	_, err := r.db.ExecContext(ctx, query,
		sa.ID.String(),
		sa.Name,
		sa.Description,
		createdBy,
		sa.CreatedAt,
		sa.UpdatedAt,
		sa.Enabled,
		delegatedFrom,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create service account")
	}
	return nil
}

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

func (r *CRDBAuthRepo) UpdateServiceAccount(
	ctx context.Context, l *logger.Logger, sa *auth.ServiceAccount,
) error {
	query := `
		UPDATE service_accounts
		SET name = $1, description = $2, enabled = $3, updated_at = $4
		WHERE id = $5
	`
	_, err := r.db.ExecContext(ctx, query,
		sa.Name,
		sa.Description,
		sa.Enabled,
		sa.UpdatedAt,
		sa.ID.String(),
	)
	if err != nil {
		return errors.Wrap(err, "failed to update service account")
	}
	return nil
}

func (r *CRDBAuthRepo) ListServiceAccounts(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.ServiceAccount, int, error) {
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.ServiceAccount{}),
	)
	baseSelectQuery := `
		SELECT id, name, description, created_by, created_at, updated_at, enabled, delegated_from
		FROM service_accounts
	`
	baseCountQuery := "SELECT count(*) FROM service_accounts"

	// Default sort by name
	defaultSort := &filtertypes.SortParams{
		SortBy:    "Name",
		SortOrder: filtertypes.SortAscending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count service accounts")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list service accounts")
	}
	defer rows.Close()

	var saList []*auth.ServiceAccount
	for rows.Next() {
		sa, err := r.scanServiceAccount(rows)
		if err != nil {
			return nil, 0, err
		}
		saList = append(saList, sa)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating service accounts")
	}
	return saList, totalCount, nil
}

func (r *CRDBAuthRepo) DeleteServiceAccount(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	query := "DELETE FROM service_accounts WHERE id = $1"
	_, err := r.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to delete service account")
	}
	return nil
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

func (r *CRDBAuthRepo) AddServiceAccountOrigin(
	ctx context.Context, l *logger.Logger, origin *auth.ServiceAccountOrigin,
) error {
	query := `
		INSERT INTO service_account_origins (id, service_account_id, cidr, description, created_by, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	var createdBy interface{}
	if origin.CreatedBy != nil {
		createdBy = origin.CreatedBy.String()
	}

	_, err := r.db.ExecContext(ctx, query,
		origin.ID.String(),
		origin.ServiceAccountID.String(),
		origin.CIDR,
		origin.Description,
		createdBy,
		origin.CreatedAt,
		origin.UpdatedAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to add service account origin")
	}
	return nil
}

func (r *CRDBAuthRepo) RemoveServiceAccountOrigin(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	query := "DELETE FROM service_account_origins WHERE id = $1"
	_, err := r.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to remove service account origin")
	}
	return nil
}

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
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating service account origins")
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
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating tokens")
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
	// Use DISTINCT ON to deduplicate by (scope, permission)
	// This ensures a user gets each permission only once, even if granted by multiple groups
	// Joins group_members → groups → group_permissions
	query := `
		SELECT DISTINCT ON (gp.scope, gp.permission)
			gp.id, gp.group_name, gp.scope, gp.permission, gp.created_at, gp.updated_at
		FROM group_members gm
		JOIN groups g ON gm.group_id = g.id
		JOIN group_permissions gp ON g.display_name = gp.group_name
		WHERE gm.user_id = $1
		ORDER BY gp.scope, gp.permission, gp.created_at
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
		SELECT id, service_account_id, scope, permission, created_at
		FROM service_account_permissions
	`
	baseCountQuery := "SELECT count(*) FROM service_account_permissions"

	// Add required filter for service_account_id
	filterSet.AddFilter("ServiceAccountID", filtertypes.OpEqual, saID)
	filterSet.Logic = filtertypes.LogicAnd

	// Default sort by scope
	defaultSort := &filtertypes.SortParams{
		SortBy:    "Scope",
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
		var id, saIDStr, scope, permission string
		var createdAt time.Time

		if err := rows.Scan(&id, &saIDStr, &scope, &permission, &createdAt); err != nil {
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

		perms = append(perms, &auth.ServiceAccountPermission{
			ID:               permID,
			ServiceAccountID: sid,
			Scope:            scope,
			Permission:       permission,
			CreatedAt:        createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating service account permissions")
	}

	return perms, totalCount, nil
}

func (r *CRDBAuthRepo) UpdateServiceAccountPermissions(
	ctx context.Context,
	l *logger.Logger,
	saID uuid.UUID,
	permissions []*auth.ServiceAccountPermission,
) (retErr error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// 1. Delete existing permissions
	_, err = tx.ExecContext(ctx, "DELETE FROM service_account_permissions WHERE service_account_id = $1", saID.String())
	if err != nil {
		return errors.Wrap(err, "failed to delete existing permissions")
	}

	// 2. Insert new permissions
	insertQuery := `
		INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	now := timeutil.Now()

	for _, perm := range permissions {
		_, err := tx.ExecContext(ctx, insertQuery,
			perm.ID.String(),
			perm.ServiceAccountID.String(),
			perm.Scope,
			perm.Permission,
			now,
		)
		if err != nil {
			return errors.Wrap(err, "failed to insert service account permission")
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	committed = true
	return nil
}

// AddServiceAccountPermission adds a single permission to a service account.
func (r *CRDBAuthRepo) AddServiceAccountPermission(
	ctx context.Context, l *logger.Logger, perm *auth.ServiceAccountPermission,
) error {
	query := `
		INSERT INTO service_account_permissions (id, service_account_id, scope, permission, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := r.db.ExecContext(ctx, query,
		perm.ID.String(),
		perm.ServiceAccountID.String(),
		perm.Scope,
		perm.Permission,
		perm.CreatedAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to insert service account permission")
	}

	return nil
}

// GetServiceAccountPermission retrieves a single permission by ID.
func (r *CRDBAuthRepo) GetServiceAccountPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.ServiceAccountPermission, error) {
	query := `
		SELECT id, service_account_id, scope, permission, created_at
		FROM service_account_permissions WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())

	var idStr, saIDStr, scope, permission string
	var createdAt time.Time

	if err := row.Scan(&idStr, &saIDStr, &scope, &permission, &createdAt); err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rauth.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to scan service account permission")
	}

	permID, err := uuid.FromString(idStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid permission id UUID in database")
	}
	saID, err := uuid.FromString(saIDStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid service_account_id UUID in database")
	}

	return &auth.ServiceAccountPermission{
		ID:               permID,
		ServiceAccountID: saID,
		Scope:            scope,
		Permission:       permission,
		CreatedAt:        createdAt,
	}, nil
}

// RemoveServiceAccountPermission removes a single permission by ID.
func (r *CRDBAuthRepo) RemoveServiceAccountPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	query := `DELETE FROM service_account_permissions WHERE id = $1`

	result, err := r.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to delete service account permission")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}

	if rowsAffected == 0 {
		return rauth.ErrNotFound
	}

	return nil
}

// Groups (SCIM-managed)

func (r *CRDBAuthRepo) GetGroup(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) (*auth.Group, error) {
	query := `
		SELECT id, external_id, display_name, created_at, updated_at
		FROM groups WHERE id = $1
	`
	row := r.db.QueryRowContext(ctx, query, id.String())
	return r.scanGroup(row)
}

func (r *CRDBAuthRepo) GetGroupByExternalID(
	ctx context.Context, l *logger.Logger, externalID string,
) (*auth.Group, error) {
	query := `
		SELECT id, external_id, display_name, created_at, updated_at
		FROM groups WHERE external_id = $1
	`
	row := r.db.QueryRowContext(ctx, query, externalID)
	return r.scanGroup(row)
}

func (r *CRDBAuthRepo) UpdateGroup(ctx context.Context, l *logger.Logger, group *auth.Group) error {
	query := `
		UPDATE groups
		SET external_id = $1, display_name = $2, updated_at = $3
		WHERE id = $4
	`
	result, err := r.db.ExecContext(ctx, query,
		group.ExternalID,
		group.DisplayName,
		group.UpdatedAt,
		group.ID.String(),
	)
	if err != nil {
		return errors.Wrap(err, "failed to update group")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to check rows affected")
	}
	if rowsAffected == 0 {
		return rauth.ErrNotFound
	}

	return nil
}

func (r *CRDBAuthRepo) DeleteGroup(ctx context.Context, l *logger.Logger, id uuid.UUID) error {
	query := "DELETE FROM groups WHERE id = $1"
	result, err := r.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to delete group")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to check rows affected")
	}
	if rowsAffected == 0 {
		return rauth.ErrNotFound
	}

	return nil
}

func (r *CRDBAuthRepo) ListGroups(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.Group, int, error) {
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.Group{}),
	)
	baseSelectQuery := `
		SELECT id, external_id, display_name, created_at, updated_at
		FROM groups
	`
	baseCountQuery := "SELECT count(*) FROM groups"

	// Default sort by display_name
	defaultSort := &filtertypes.SortParams{
		SortBy:    "DisplayName",
		SortOrder: filtertypes.SortAscending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count groups")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list groups")
	}
	defer rows.Close()

	var groupsList []*auth.Group
	for rows.Next() {
		group, err := r.scanGroup(rows)
		if err != nil {
			return nil, 0, err
		}
		groupsList = append(groupsList, group)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating groups")
	}
	return groupsList, totalCount, nil
}

func (r *CRDBAuthRepo) scanGroup(
	scanner interface {
		Scan(dest ...interface{}) error
	},
) (*auth.Group, error) {
	var id string
	var externalID gosql.NullString
	var displayName string
	var createdAt, updatedAt time.Time

	err := scanner.Scan(&id, &externalID, &displayName, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, gosql.ErrNoRows) {
			return nil, rauth.ErrNotFound
		}
		return nil, errors.Wrap(err, "failed to scan group")
	}

	groupID, err := uuid.FromString(id)
	if err != nil {
		return nil, errors.Wrap(err, "invalid group id in db")
	}

	group := &auth.Group{
		ID:          groupID,
		DisplayName: displayName,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}

	if externalID.Valid {
		group.ExternalID = &externalID.String
	}

	return group, nil
}

// Group Members

func (r *CRDBAuthRepo) GetGroupMembers(
	ctx context.Context, l *logger.Logger, groupID uuid.UUID, filterSet filtertypes.FilterSet,
) ([]*auth.GroupMember, int, error) {

	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.GroupMember{}),
	)
	baseSelectQuery := `
		SELECT id, group_id, user_id, created_at
		FROM group_members
	`
	baseCountQuery := "SELECT count(*) FROM group_members"

	filterSet.AddFilter("GroupID", filtertypes.OpEqual, groupID)
	filterSet.Logic = filtertypes.LogicAnd

	// Default sort by display_name
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
		return nil, 0, errors.Wrap(err, "failed to count group members")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list group members")
	}
	defer rows.Close()

	var members []*auth.GroupMember
	for rows.Next() {
		var id, gIDStr, uIDStr string
		var createdAt time.Time

		if err := rows.Scan(&id, &gIDStr, &uIDStr, &createdAt); err != nil {
			return nil, 0, errors.Wrap(err, "failed to scan group member")
		}

		memberID, err := uuid.FromString(id)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid member id UUID in database")
		}
		gID, err := uuid.FromString(gIDStr)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid group_id UUID in database")
		}
		uID, err := uuid.FromString(uIDStr)
		if err != nil {
			return nil, 0, errors.Wrap(err, "invalid user_id UUID in database")
		}

		members = append(members, &auth.GroupMember{
			ID:        memberID,
			GroupID:   gID,
			UserID:    uID,
			CreatedAt: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Wrap(err, "error iterating group members")
	}

	return members, totalCount, nil
}

// CreateGroupWithMembers atomically creates a group and adds initial members.
func (r *CRDBAuthRepo) CreateGroupWithMembers(
	ctx context.Context, l *logger.Logger, group *auth.Group, memberIDs []uuid.UUID,
) (retErr error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// Create the group
	_, err = tx.ExecContext(ctx,
		`INSERT INTO groups (id, external_id, display_name, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		group.ID.String(),
		group.ExternalID,
		group.DisplayName,
		group.CreatedAt,
		group.UpdatedAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create group")
	}

	// Add members
	for _, userID := range memberIDs {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO group_members (id, group_id, user_id, created_at)
			 VALUES ($1, $2, $3, now())
			 ON CONFLICT (group_id, user_id) DO NOTHING`,
			uuid.MakeV4().String(),
			group.ID.String(),
			userID.String(),
		)
		if err != nil {
			return errors.Wrapf(err, "failed to add member %s", userID)
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	committed = true
	return nil
}

// UpdateGroupWithMembers atomically updates a group and applies member operations.
func (r *CRDBAuthRepo) UpdateGroupWithMembers(
	ctx context.Context, l *logger.Logger, group *auth.Group, operations []rauth.GroupMemberOperation,
) (retErr error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// Update the group if provided
	if group != nil {
		_, err := tx.ExecContext(ctx,
			`UPDATE groups SET external_id = $2, display_name = $3, updated_at = $4 WHERE id = $1`,
			group.ID.String(),
			group.ExternalID,
			group.DisplayName,
			group.UpdatedAt,
		)
		if err != nil {
			return errors.Wrap(err, "failed to update group")
		}
	}

	// Apply member operations
	for _, op := range operations {
		groupIDStr := group.ID.String()
		switch op.Op {
		case "add":
			_, err := tx.ExecContext(ctx,
				`INSERT INTO group_members (id, group_id, user_id, created_at)
				 VALUES ($1, $2, $3, now())
				 ON CONFLICT (group_id, user_id) DO NOTHING`,
				uuid.MakeV4().String(),
				groupIDStr,
				op.UserID.String(),
			)
			if err != nil {
				return errors.Wrapf(err, "failed to add member %s", op.UserID)
			}
		case "remove":
			_, err := tx.ExecContext(ctx,
				`DELETE FROM group_members
				 WHERE group_id = $1 AND user_id = $2`,
				groupIDStr,
				op.UserID.String(),
			)
			if err != nil {
				return errors.Wrapf(err, "failed to remove member %s", op.UserID)
			}
		default:
			return errors.Newf("unsupported operation: %s", op.Op)
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	committed = true
	return nil
}

// Group Permissions (new table name after migration)

func (r *CRDBAuthRepo) ListGroupPermissions(
	ctx context.Context, l *logger.Logger, filterSet filtertypes.FilterSet,
) ([]*auth.GroupPermission, int, error) {
	qb := filters.NewSQLQueryBuilderWithTypeHint(
		reflect.TypeOf(auth.GroupPermission{}),
	)
	baseSelectQuery := `
		SELECT id, group_name, scope, permission, created_at, updated_at
		FROM group_permissions
	`
	baseCountQuery := "SELECT count(*) FROM group_permissions"

	// Default sort by group_name
	defaultSort := &filtertypes.SortParams{
		SortBy:    "GroupName",
		SortOrder: filtertypes.SortAscending,
	}

	selectQuery, countQuery, args, err := qb.BuildWithCount(baseSelectQuery, baseCountQuery, &filterSet, defaultSort)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to build queries")
	}

	// Execute COUNT query
	var totalCount int
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, 0, errors.Wrap(err, "failed to count group permissions")
	}

	// Execute SELECT query
	rows, err := r.db.QueryContext(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, errors.Wrap(err, "failed to list group permissions")
	}
	defer rows.Close()

	perms, err := r.scanGroupPermissions(rows)
	if err != nil {
		return nil, 0, err
	}

	return perms, totalCount, nil
}

func (r *CRDBAuthRepo) GetPermissionsForGroups(
	ctx context.Context, l *logger.Logger, groups []string,
) ([]*auth.GroupPermission, error) {
	if len(groups) == 0 {
		return nil, nil
	}

	// Build placeholders for IN clause: $1, $2, $3, ...
	placeholders := make([]string, len(groups))
	args := make([]interface{}, len(groups))
	for i, g := range groups {
		placeholders[i] = "$" + strconv.Itoa(i+1)
		args[i] = g
	}

	query := `
		SELECT id, group_name, scope, permission, created_at, updated_at
		FROM group_permissions
		WHERE group_name IN (` + strings.Join(placeholders, ", ") + `)
		ORDER BY group_name, scope, permission
	`
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get permissions for groups")
	}
	defer rows.Close()

	return r.scanGroupPermissions(rows)
}

func (r *CRDBAuthRepo) scanGroupPermissions(rows *gosql.Rows) ([]*auth.GroupPermission, error) {
	var perms []*auth.GroupPermission
	for rows.Next() {
		var id, groupName, scope, permission string
		var createdAt, updatedAt time.Time

		if err := rows.Scan(&id, &groupName, &scope, &permission, &createdAt, &updatedAt); err != nil {
			return nil, errors.Wrap(err, "failed to scan group permission")
		}

		permID, err := uuid.FromString(id)
		if err != nil {
			return nil, errors.Wrap(err, "invalid permission id UUID in database")
		}

		perms = append(perms, &auth.GroupPermission{
			ID:         permID,
			GroupName:  groupName,
			Scope:      scope,
			Permission: permission,
			CreatedAt:  createdAt,
			UpdatedAt:  updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating group permissions")
	}

	return perms, nil
}

func (r *CRDBAuthRepo) CreateGroupPermission(
	ctx context.Context, l *logger.Logger, perm *auth.GroupPermission,
) error {
	query := `
		INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := r.db.ExecContext(ctx, query,
		perm.ID.String(),
		perm.GroupName,
		perm.Scope,
		perm.Permission,
		perm.CreatedAt,
		perm.UpdatedAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create group permission")
	}
	return nil
}

func (r *CRDBAuthRepo) UpdateGroupPermission(
	ctx context.Context, l *logger.Logger, perm *auth.GroupPermission,
) error {
	query := `
		UPDATE group_permissions
		SET group_name = $2, scope = $3, permission = $4, updated_at = $5
		WHERE id = $1
	`
	result, err := r.db.ExecContext(ctx, query,
		perm.ID.String(),
		perm.GroupName,
		perm.Scope,
		perm.Permission,
		perm.UpdatedAt,
	)
	if err != nil {
		return errors.Wrap(err, "failed to update group permission")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to check rows affected")
	}
	if rowsAffected == 0 {
		return rauth.ErrNotFound
	}

	return nil
}

func (r *CRDBAuthRepo) DeleteGroupPermission(
	ctx context.Context, l *logger.Logger, id uuid.UUID,
) error {
	query := `DELETE FROM group_permissions WHERE id = $1`
	result, err := r.db.ExecContext(ctx, query, id.String())
	if err != nil {
		return errors.Wrap(err, "failed to delete group permission")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to check rows affected")
	}
	if rowsAffected == 0 {
		return rauth.ErrNotFound
	}

	return nil
}

func (r *CRDBAuthRepo) ReplaceGroupPermissions(
	ctx context.Context, l *logger.Logger, perms []*auth.GroupPermission,
) (retErr error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				if retErr != nil {
					retErr = errors.CombineErrors(retErr, errors.Wrap(rollbackErr, "rollback error"))
				} else {
					retErr = errors.Wrap(rollbackErr, "rollback error")
				}
			}
		}
	}()

	// Delete all existing permissions
	_, err = tx.ExecContext(ctx, `DELETE FROM group_permissions`)
	if err != nil {
		return errors.Wrap(err, "failed to delete existing group permissions")
	}

	// Insert new permissions
	insertQuery := `
		INSERT INTO group_permissions (id, group_name, scope, permission, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	for _, perm := range perms {
		_, err = tx.ExecContext(ctx, insertQuery,
			perm.ID.String(),
			perm.GroupName,
			perm.Scope,
			perm.Permission,
			perm.CreatedAt,
			perm.UpdatedAt,
		)
		if err != nil {
			return errors.Wrap(err, "failed to insert group permission")
		}
	}

	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	committed = true
	return nil
}

// GetStatistics returns current counts for metrics gauges.
func (r *CRDBAuthRepo) GetStatistics(
	ctx context.Context, l *logger.Logger,
) (*rauth.Statistics, error) {
	stats := &rauth.Statistics{
		TokensByTypeAndStatus: make(map[string]map[string]int),
	}

	// Users by status
	userRows, err := r.db.QueryContext(ctx, `SELECT active, count(*) FROM users GROUP BY active`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get user statistics")
	}
	defer userRows.Close()
	for userRows.Next() {
		var active bool
		var count int
		if err := userRows.Scan(&active, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan user statistics")
		}
		if active {
			stats.UsersActive = count
		} else {
			stats.UsersInactive = count
		}
	}
	if err := userRows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating user statistics")
	}

	// Groups count
	if err := r.db.QueryRowContext(ctx, `SELECT count(*) FROM groups`).Scan(&stats.Groups); err != nil {
		return nil, errors.Wrap(err, "failed to get group count")
	}

	// Service accounts by enabled
	saRows, err := r.db.QueryContext(ctx, `SELECT enabled, count(*) FROM service_accounts GROUP BY enabled`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get service account statistics")
	}
	defer saRows.Close()
	for saRows.Next() {
		var enabled bool
		var count int
		if err := saRows.Scan(&enabled, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan service account statistics")
		}
		if enabled {
			stats.ServiceAccountsEnabled = count
		} else {
			stats.ServiceAccountsDisabled = count
		}
	}
	if err := saRows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating service account statistics")
	}

	// Tokens by type and status
	tokenRows, err := r.db.QueryContext(ctx, `SELECT token_type, status, count(*) FROM api_tokens GROUP BY token_type, status`)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get token statistics")
	}
	defer tokenRows.Close()
	for tokenRows.Next() {
		var tokenType, status string
		var count int
		if err := tokenRows.Scan(&tokenType, &status, &count); err != nil {
			return nil, errors.Wrap(err, "failed to scan token statistics")
		}
		if stats.TokensByTypeAndStatus[tokenType] == nil {
			stats.TokensByTypeAndStatus[tokenType] = make(map[string]int)
		}
		stats.TokensByTypeAndStatus[tokenType][status] = count
	}
	if err := tokenRows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating token statistics")
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
