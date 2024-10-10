// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowinspectpb

import (
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/redact"
)

func (s Stream) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s Stream) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("s%v/t%v eval_regular=%v eval_elastic=%v send_regular=%v send_elastic=%v",
		s.StoreID,
		s.TenantID,
		humanizeutil.IBytes(s.AvailableEvalRegularTokens),
		humanizeutil.IBytes(s.AvailableEvalElasticTokens),
		humanizeutil.IBytes(s.AvailableSendRegularTokens),
		humanizeutil.IBytes(s.AvailableSendElasticTokens),
	)
}
