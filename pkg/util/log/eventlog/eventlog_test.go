// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eventlog_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventlog"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type ServerId struct {
	instanceId int
}

func (s *ServerId) ServerIdentityString(key serverident.ServerIdentificationKey) string {
	return strconv.Itoa(s.instanceId)
}

var _ serverident.ServerIdentificationPayload = &ServerId{}

func TestWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	testCases := []struct {
		instanceId int
		writeAsync bool
	}{
		{
			instanceId: 9999,
			writeAsync: false,
		},
		{
			instanceId: 9998,
			writeAsync: true,
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("writeAsync=%t", tc.writeAsync), func(t *testing.T) {
			ambientCtx := log.MakeServerAmbientContext(nil, &ServerId{instanceId: tc.instanceId})
			writer := eventlog.NewWriter(s.InternalDB().(isql.DB), tc.writeAsync, s.Stopper(), ambientCtx, s.ClusterSettings())
			// This can be any event type
			event := logtestutils.TestEvent{Timestamp: timeutil.Now().UnixNano()}

			writer.Write(ambientCtx.AnnotateCtx(ctx), event, false)
			runner := sqlutils.MakeSQLRunner(conn)
			var rows int
			testutils.SucceedsSoon(t, func() error {
				runner.QueryRow(t,
					fmt.Sprintf(`SELECT count(*) from system.eventlog where "eventType" = '%s' and "reportingID"=%d`,
						logtestutils.TestEventType, tc.instanceId),
				).Scan(&rows)

				if rows != 1 {
					return errors.New("Event not persisted")
				}
				return nil
			})
		})
	}
}
