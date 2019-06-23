// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/spf13/cobra"
)

var csvServerCmd = SetCmdDefaults(&cobra.Command{
	Use:   `csv-server`,
	Short: `serves csv table data through an HTTP interface`,
	Args:  cobra.NoArgs,
	RunE:  runCSVServer,
})

var port *int

func init() {
	port = csvServerCmd.Flags().Int(`port`, 8081, `The port to bind to`)
	AddSubCmd(func(_ bool) *cobra.Command { return csvServerCmd })
}

func runCSVServer(_ *cobra.Command, _ []string) error {
	mux := workload.CSVMux(workload.Registered())

	// Cribbed straight from pprof's `init()` method. See:
	// https://golang.org/src/net/http/pprof/pprof.go
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	s := &http.Server{
		Addr:    fmt.Sprintf(`:%d`, *port),
		Handler: mux,
	}
	fmt.Printf("Listening on %s\n", s.Addr)
	return s.ListenAndServe()
}
