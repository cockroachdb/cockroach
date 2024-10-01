// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"gopkg.in/yaml.v2"
)

type httpHandler struct {
	ctx    context.Context
	w      *workloadRunner
	o      *opsRunner
	eventL *eventLogger
}

func (h *httpHandler) startHTTPServer(httpPort int, bindTo string) error {
	http.HandleFunc("/", h.serve)
	http.HandleFunc("/workload/", h.serveWorkload)
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindTo, httpPort))
	if err != nil {
		return err
	}
	go func() {
		if err := http.Serve(listener, nil /* handler */); err != nil {
			panic(err)
		}
	}()
	bindToDesc := "all network interfaces"
	if bindTo != "" {
		bindToDesc = bindTo
	}
	fmt.Printf("HTTP server listening on port %d on %s: http://%s:%d/\n", httpPort, bindToDesc, bindTo, httpPort)
	return nil
}

func (h *httpHandler) writeRunningOperations(rw http.ResponseWriter) {
	opEvents := h.eventL.getOperationEvents()
	runningOps := h.o.getRunningOperations()

	fmt.Fprintf(rw, "<p>Operations:</p>\n<ul>")

	fmt.Fprintf(rw, "<table style=\"border: 1px solid black;\">\n<tr><th>Worker</th><th>Operation</th><th>Log entries</th></tr>\n")
	for i := range runningOps {
		fmt.Fprintf(rw, "<tr>\n")
		fmt.Fprintf(rw, "<td>%d</td>", i)
		if runningOps[i] == "" {
			fmt.Fprintf(rw, "<td>idle</td><td></td></tr>")
			continue
		}
		fmt.Fprintf(rw, "<td>%s</td>", runningOps[i])
		fmt.Fprintf(rw, "<td><div style=\"font-family: monospace;\">")
		for j := range opEvents {
			if opEvents[j].SourceName == runningOps[i] {
				fmt.Fprintf(rw, "%s<br>", opEvents[j].String())
			}
		}
		fmt.Fprintf(rw, "</div></td></tr>")
	}
	fmt.Fprintf(rw, "</table>")
}

func (h *httpHandler) serve(rw http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(rw, "<!DOCTYPE html>\n")
	fmt.Fprintf(rw, "<html lang=\"en\">\n")
	fmt.Fprintf(rw, "<head><title>DRT Run</title><meta charset=\"utf-8\"></head>\n")
	fmt.Fprintf(rw, "<body>\n")

	fmt.Fprintf(rw, "<h3>Workloads</h3><hr>\n")

	fmt.Fprintf(rw, "<p>Workloads:</p>\n<ul>")
	for i, w := range h.w.config.Workloads {
		fmt.Fprintf(rw, "<li><a href=\"/workload/?id=%d\">%s (kind %s)</a></li>", i, w.Name, w.Kind)
	}
	fmt.Fprintf(rw, "</ul>\n")

	fmt.Fprintf(rw, "<h3>Operations</h3><hr>\n")
	h.writeRunningOperations(rw)

	fmt.Fprintf(rw, "<h3>Operations log history</h3><hr>\n")
	fmt.Fprintf(rw, "<div style=\"font-family: monospace;\">")
	for _, ev := range h.eventL.getOperationEvents() {
		fmt.Fprintf(rw, "%s<br>", ev.String())
	}
	fmt.Fprintf(rw, "</div>")

	fmt.Fprintf(rw, "<h3>Configuration</h3><hr>\n")
	fmt.Fprintf(rw, "<pre>")
	encoder := yaml.NewEncoder(rw)
	_ = encoder.Encode(h.w.config)
	fmt.Fprintf(rw, "</pre>")

	fmt.Fprintf(rw, "</body>\n</html>")

}

func (h *httpHandler) serveWorkload(rw http.ResponseWriter, req *http.Request) {
	workloadIdx := req.URL.Query().Get("id")
	fmt.Fprintf(rw, "<!DOCTYPE html>\n")
	fmt.Fprintf(rw, "<html lang=\"en\">\n")
	fmt.Fprintf(rw, "<head><title>DRT Run - Workload %s</title><meta charset=\"utf-8\"></head>\n", workloadIdx)
	fmt.Fprintf(rw, "<body>\n")

	fmt.Fprintf(rw, "<h3>Workload %s</h3><hr>\n", workloadIdx)

	idx, err := strconv.Atoi(workloadIdx)
	if err != nil || idx < 0 || idx >= len(h.w.config.Workloads) {
		fmt.Fprintf(rw, "error when parsing workload id: %s", err)
		fmt.Fprintf(rw, "</body></html>")
		return
	}

	events := h.eventL.getWorkloadEvents(idx)
	fmt.Fprintf(rw, "<p>Workload %s (kind: %s) events:</p>\n<ul>", h.w.config.Workloads[idx].Name, h.w.config.Workloads[idx].Kind)

	fmt.Fprintf(rw, "<div style=\"font-family: monospace;\">")
	for i := range events {
		fmt.Fprintf(rw, "%s<br>", events[i].String())
	}
	fmt.Fprintf(rw, "</div>")

	fmt.Fprintf(rw, "</body>\n</html>")
}
