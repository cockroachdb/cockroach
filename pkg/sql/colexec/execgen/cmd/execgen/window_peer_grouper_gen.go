// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"
	"strings"
	"text/template"
)

type windowPeerGrouperTmplInfo struct {
	AllPeers     bool
	HasPartition bool
	String       string
}

const windowPeerGrouperOpsTmpl = "pkg/sql/colexec/colexecwindow/window_peer_grouper_tmpl.go"

func genWindowPeerGrouperOps(inputFileContents string, wr io.Writer) error {
	s := strings.ReplaceAll(inputFileContents, "_PEER_GROUPER_STRING", "{{.String}}")

	// Now, generate the op, from the template.
	tmpl, err := template.New("peer_grouper_op").Parse(s)
	if err != nil {
		return err
	}

	windowPeerGrouperTmplInfos := []windowPeerGrouperTmplInfo{
		{AllPeers: false, HasPartition: false, String: "windowPeerGrouperNoPartition"},
		{AllPeers: false, HasPartition: true, String: "windowPeerGrouperWithPartition"},
		{AllPeers: true, HasPartition: false, String: "windowPeerGrouperAllPeersNoPartition"},
		{AllPeers: true, HasPartition: true, String: "windowPeerGrouperAllPeersWithPartition"},
	}
	return tmpl.Execute(wr, windowPeerGrouperTmplInfos)
}

func init() {
	registerGenerator(genWindowPeerGrouperOps, "window_peer_grouper.eg.go", windowPeerGrouperOpsTmpl)
}
