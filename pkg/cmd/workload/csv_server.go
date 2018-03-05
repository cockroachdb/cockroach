// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/workload"
)

var csvServerCmd = &cobra.Command{
	Use:   `csv-server`,
	Short: `Serves csv table data through an HTTP interface`,
	Args:  cobra.NoArgs,
	RunE:  runCSVServer,
}

var port *int

func init() {
	port = csvServerCmd.Flags().Int(`port`, 8081, `The port to bind to`)
	rootCmd.AddCommand(csvServerCmd)
}

func runCSVServer(_ *cobra.Command, _ []string) error {
	mux := http.NewServeMux()
	for _, meta := range workload.Registered() {
		meta := meta
		prefix := fmt.Sprintf(`/csv/%s/`, meta.Name)
		mux.HandleFunc(prefix, func(w http.ResponseWriter, req *http.Request) {
			if err := workload.HandleCSV(w, req, prefix, meta); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})
	}
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s := &http.Server{
		Addr:    fmt.Sprintf(`:%d`, *port),
		Handler: mux,
	}
	fmt.Printf("Listening on %s\n", s.Addr)
	return s.ListenAndServe()
}
