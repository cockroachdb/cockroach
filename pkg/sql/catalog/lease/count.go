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

	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
	executor isql.Executor,
	settings *clustersettings.Settings,
	versions []IDVersion,
	at hlc.Timestamp,
) (int, error) {
	leasingMode := SessionBasedLeasingMode(LeaseEnableSessionBasedLeasing.Get(&settings.SV))

	whereClauses := make([][]string, 2)
	for _, t := range versions {
		whereClauses[0] = append(whereClauses[0],
			fmt.Sprintf(`("descID" = %d AND version = %d AND expiration > $1)`,
				t.ID, t.Version),
		)
		whereClauses[1] = append(whereClauses[1],
			fmt.Sprintf(`("descID" = %d AND version = %d AND (crdb_internal.sql_liveness_is_alive("sessionID")))`,
				t.ID, t.Version),
		)
	}

	stmts := make([]string, 0, 2)
	syntheticDescriptors := make(catalog.Descriptors, 0, 2)
	if leasingMode != SessionBasedOnly {
		stmts = append(stmts, fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
			at.AsOfSystemTime())+
			strings.Join(whereClauses[0], " OR "))
		syntheticDescriptors = append(syntheticDescriptors, nil)
	}
	if leasingMode >= SessionBasedDrain {
		stmts = append(stmts, fmt.Sprintf(`SELECT count(1) FROM system.public.lease AS OF SYSTEM TIME '%s' WHERE `,
			at.AsOfSystemTime())+
			strings.Join(whereClauses[1], " OR "))
		syntheticDescriptors = append(syntheticDescriptors, systemschema.LeaseTable_V24_1())
	}

	for i := range stmts {
		var values tree.Datums
		descsToInject := syntheticDescriptors[i : i+1]
		if descsToInject[0] == nil {
			descsToInject = nil
		}
		err := executor.WithSyntheticDescriptors(descsToInject, func() error {
			var err error
			values, err = executor.QueryRowEx(
				ctx, "count-leases", nil, /* txn */
				sessiondata.RootUserSessionDataOverride,
				stmts[i], at.GoTime(),
			)
			return err
		})
		if err != nil {
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
