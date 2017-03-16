// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package sql

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// GetUserHashedPassword returns the hashedPassword for the given username if
// found in system.users.
func GetUserHashedPassword(
	ctx context.Context, executor *Executor, metrics *MemoryMetrics, username string,
) ([]byte, error) {
	normalizedUsername := parser.Name(username).Normalize()
	// The root user is not in system.users.
	if normalizedUsername == security.RootUser {
		return nil, nil
	}

	var hashedPassword []byte
	if err := executor.cfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		p := makeInternalPlanner("get-pwd", txn, security.RootUser, metrics)
		defer finishInternalPlanner(p)
		const getHashedPassword = `SELECT hashedPassword FROM system.users ` +
			`WHERE username=$1`
		values, err := p.QueryRow(ctx, getHashedPassword, normalizedUsername)
		if err != nil {
			return errors.Errorf("error looking up user %s", normalizedUsername)
		}
		if len(values) == 0 {
			return errors.Errorf("user %s does not exist", normalizedUsername)
		}
		hashedPassword = []byte(*(values[0].(*parser.DBytes)))
		return nil
	}); err != nil {
		return nil, err
	}

	return hashedPassword, nil
}
