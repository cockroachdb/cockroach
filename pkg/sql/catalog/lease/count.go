// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lease

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// CountLeases returns the number of unexpired leases for a number of descriptors
// each at a particular version at a particular time.
func CountLeases(
	ctx context.Context,
	activeVersion clusterversion.Handle,
	executor isql.Executor,
	versions []IDVersion,
	at hlc.Timestamp,
) (int, error) {
	// Depending on if we have a sessionID or expiry only query, either
	// one or both tables.
	hasSessionID := activeVersion.IsActive(ctx, clusterversion.V23_2_LeaseToSessionCreation)
	usesExpiry := !activeVersion.IsActive(ctx, clusterversion.V23_2_LeaseWillOnlyHaveSessions)
	leaseDescs := make([]catalog.Descriptor, 0, 2)
	stmts := make([]string, 0, 2)
	if hasSessionID {
		leaseDescs = append(leaseDescs, systemschema.LeaseTable())
	}
	if usesExpiry {
		if activeVersion.IsActive(ctx, clusterversion.V23_1_SystemRbrReadNew) {
			leaseDescs = append(leaseDescs, systemschema.V23_1_LeaseTable())
		} else {
			leaseDescs = append(leaseDescs, systemschema.V22_2_LeaseTable())
		}
	}
	var whereClauses [2][]string
	for _, t := range versions {
		if hasSessionID {
			clause := fmt.Sprintf(`("descID" = %d AND version = %d AND (crdb_internal.sql_liveness_is_alive("sessionID")))`,
				t.ID, t.Version)
			whereClauses[0] = append(whereClauses[0],
				clause,
			)
		}
		if usesExpiry {
			clause := fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.ID, t.Version)
			whereClauses[1] = append(whereClauses[1],
				clause)
		}
	}
	if hasSessionID {
		stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
			at.AsOfSystemTime()) +
			strings.Join(whereClauses[0], " OR ")
		stmts = append(stmts, stmt)
	}
	if usesExpiry {
		stmt := fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
			at.AsOfSystemTime()) +
			strings.Join(whereClauses[1], " OR ")
		stmts = append(stmts, stmt)
	}

	for idx, stmt := range stmts {
		var values tree.Datums
		if err := executor.WithSyntheticDescriptors(leaseDescs[idx:idx+1], func() error {
			var err error
			values, err = executor.QueryRowEx(
				ctx, "count-leases", nil, /* txn */
				sessiondata.RootUserSessionDataOverride,
				stmt, at.GoTime(),
			)
			return err
		}); err != nil {
			return 0, err
		}
		if values == nil {
			return 0, errors.New("failed to count leases")
		}
		count := int(tree.MustBeDInt(values[0]))
		if count > 0 {
			return count, nil
		}
	}
	return 0, nil
}
