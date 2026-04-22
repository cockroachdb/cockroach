// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import "github.com/cockroachdb/cockroach/pkg/settings"

// ProducerForwardThreshold gates the producer's forward-vs-PUT
// decision on each tick flush: a per-tick buffer at or below this
// many bytes is forwarded to the coordinator as a Coalesce (where
// it can be merged with other producers' contributions and either
// PUT as one combined data file or stuffed into the manifest's
// inline_tail), and one above the threshold is PUT as its own
// data file under log/data/<tick-end>/. Setting the threshold to
// 0 disables forwarding entirely — every non-empty tick buffer
// becomes its own file. Used to A/B test the coalesce path.
var ProducerForwardThreshold = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"bulkio.revlog.coalesce.producer_forward_threshold",
	"per-tick buffer size at or below which a revlog producer "+
		"forwards events to the coordinator for inline coalescing "+
		"instead of PUTing them as a standalone data file (0 disables)",
	1<<20, // 1 MiB
)
