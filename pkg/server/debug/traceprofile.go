// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"net/http"
	"net/http/pprof"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

func TraceProfileDo(st *cluster.Settings, do func() error) error {
	if err := st.SetTraceProfiling(1); err != nil {
		return err
	}
	defer func() { _ = st.SetTraceProfiling(0) }()
	return do()
}

func TraceProfileHandler(st *cluster.Settings, w http.ResponseWriter, r *http.Request) {
	if err := TraceProfileDo(st, func() error {
		pprof.Trace(w, r)
		return nil
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
