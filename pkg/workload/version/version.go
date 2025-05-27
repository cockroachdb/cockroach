// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package version

import (
	"context"
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/jackc/pgx/v5"
)

type VersionQuery interface {
	IsClusterVersionLessThan(ctx context.Context, targetVersion roachpb.Version) (bool, error)
	Compare(ctx context.Context, compareFn func(roachpb.Version) bool) (bool, error)
}

type versionQuery struct {
	getVersion func(ctx context.Context) (string, error)
}

func WithPGX(tx pgx.Tx) VersionQuery {
	return &versionQuery{
		getVersion: func(ctx context.Context) (string, error) {
			var clusterVersionStr string
			row := tx.QueryRow(ctx, `SHOW CLUSTER SETTING version`)
			if err := row.Scan(&clusterVersionStr); err != nil {
				return "", err
			}
			return clusterVersionStr, nil
		},
	}
}

func WithGoSQL(db *gosql.DB) VersionQuery {
	return &versionQuery{
		getVersion: func(ctx context.Context) (string, error) {
			var clusterVersionStr string
			row := db.QueryRowContext(ctx, `SHOW CLUSTER SETTING version`)
			if err := row.Scan(&clusterVersionStr); err != nil {
				return "", err
			}
			return clusterVersionStr, nil
		},
	}
}

// IsClusterVersionLessThan compares the the cluster version against the given
// target version, returns true if the cluster version is less than the target
// version. This can be used in mixed version environments to prevent using
// features that are not supported by the cluster version.
func (q *versionQuery) IsClusterVersionLessThan(
	ctx context.Context, targetVersion roachpb.Version,
) (bool, error) {
	return q.Compare(ctx, func(v roachpb.Version) bool {
		return v.Less(targetVersion)
	})
}

// Compare compares the cluster version against the given function.
func (q *versionQuery) Compare(
	ctx context.Context, compareFn func(roachpb.Version) bool,
) (bool, error) {
	clusterVersionStr, err := q.getVersion(ctx)
	if err != nil {
		return false, err
	}
	clusterVersion, err := roachpb.ParseVersion(clusterVersionStr)
	if err != nil {
		return false, err
	}
	return compareFn(clusterVersion), nil
}
