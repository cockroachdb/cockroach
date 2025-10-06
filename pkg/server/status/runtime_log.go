// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"context"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/redact"
	humanize "github.com/dustin/go-humanize"
)

// statsTemplate formats an event of type eventpb.RuntimeStats into a
// user-facing string.
// TODO(knz): It may be beneficial to make this configurable at run-time.
var statsTemplate = template.Must(template.New("runtime stats").Funcs(template.FuncMap{
	"iBytes": humanize.IBytes,
}).Parse(`{{iBytes .MemRSSBytes}} RSS, {{.GoroutineCount}} goroutines (stacks: {{iBytes .MemStackSysBytes}}), ` +
	`{{iBytes .GoAllocBytes}}/{{iBytes .GoTotalBytes}} Go alloc/total{{if .GoStatsStaleness}}(stale){{end}} ` +
	`(heap fragmentation: {{iBytes .HeapFragmentBytes}}, heap reserved: {{iBytes .HeapReservedBytes}}, heap released: {{iBytes .HeapReleasedBytes}}), ` +
	`{{iBytes .CGoAllocBytes}}/{{iBytes .CGoTotalBytes}} CGO alloc/total ({{printf "%.1f" .CGoCallRate}} CGO/sec), ` +
	`{{printf "%.1f" .CPUUserPercent}}/{{printf "%.1f" .CPUSysPercent}} %(u/s)time, {{printf "%.1f" .GCPausePercent}} %gc ({{.GCRunCount}}x), ` +
	`{{iBytes .NetHostRecvBytes}}/{{iBytes .NetHostSendBytes}} (r/w)net`))

func logStats(ctx context.Context, stats *eventpb.RuntimeStats) {
	// In any case, log the structured event to its native channel (HEALTH).
	log.StructuredEvent(ctx, severity.INFO, stats)

	// Also, log a formatted version of the structured event on the HEALTH channel,
	// for use by humans while troubleshooting from log files.
	var buf strings.Builder
	if err := statsTemplate.Execute(&buf, stats); err != nil {
		log.Warningf(ctx, "failed to render runtime stats: %v", err)
		return
	}
	log.Health.Infof(ctx, "runtime stats: %s", redact.SafeString(buf.String()))
}
