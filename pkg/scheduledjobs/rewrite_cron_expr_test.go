// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduledjobs

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestCronRewrite(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cron_rewrites"), func(
		t *testing.T, d *datadriven.TestData,
	) string {
		switch d.Cmd {
		case "test":
			sc := bufio.NewScanner(strings.NewReader(d.Input))
			var ids []uuid.UUID
			for i := 0; sc.Scan(); i++ {
				id, err := uuid.FromString(sc.Text())
				if err != nil {
					d.Fatalf(t, "invalid uuid %s at line offset %d", sc.Text(), i)
				}
				ids = append(ids, id)
			}
			var output strings.Builder
			w := tabwriter.NewWriter(&output, 0, 1, 2, ' ', 0)
			_, _ = fmt.Fprintf(w, "\t%s\t%s\t%s\n", cronHourly, cronDaily, cronWeekly)

			for _, id := range ids {
				_, _ = fmt.Fprintf(w, "%s", id)
				for _, cr := range []string{cronHourly, cronDaily, cronWeekly} {
					cronExpr := MaybeRewriteCronExpr(id, cr)
					_, err := cron.ParseStandard(cronExpr)
					if err != nil {
						d.Fatalf(t, "failed to parse cron for %s+%s = %s: %v", id, cr, cronExpr, err)
					}
					_, _ = fmt.Fprintf(w, "\t%s", cronExpr)
				}
				_, _ = w.Write([]byte("\n"))
			}
			require.NoError(t, w.Flush())
			return output.String()
		default:
			d.Fatalf(t, "unknown command %s", d.Cmd)
			return ""
		}
	})
}
