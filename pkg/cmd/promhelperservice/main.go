// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Prometheus Helper Service exposes an HTTP endpoint that helps in serving requests to make
// modifications in the prometheus host

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/handlers"
	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/logging"
	"github.com/cockroachdb/cockroach/pkg/cmd/promhelperservice/registryhandler"
)

const (
	// listen port for the prometheus helper service
	promHelperServicePort = 25780
)

func main() {
	ctx := context.Background()
	log := logging.MakeLogger(ctx, "main")

	port := flag.Int("prom-helper-port", promHelperServicePort, "prometheus helper service port")
	// handlers are registered
	if err := registryhandler.RegisterHandlers(ctx, &handlers.HandlerRegistry{}); err != nil {
		log.Fatalf("Failed to register: %v\n", err)
		return
	}
	flag.Parse()
	flag.PrintDefaults()
	flag.VisitAll(func(f *flag.Flag) {
		log.Infof("Flag %s is set to %s", f.Name, f.Value.String())
	})
	log.Infof("Starting the server on port %d", *port)
	s := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", *port)}
	defer func() {
		log.Errorf("Error shutting down server: %v", s.Shutdown(ctx))
	}()
	// server is started
	if err := s.ListenAndServe(); err != nil {
		log.Fatalf("Failed to listen and serve: %v\n", err)
		return
	}
}
