// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"
)

type windowPeerGrouperTmplInfo struct {
	AllPeers     bool
	HasPartition bool
	String       string
}

func genWindowPeerGrouperOps(wr io.Writer) error {
	d, err := ioutil.ReadFile("pkg/sql/colexec/window_peer_grouper_tmpl.go")
	if err != nil {
		return err
	}

	s := string(d)
	s = strings.Replace(s, "_PEER_GROUPER_STRING", "{{.String}}", -1)

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
	registerGenerator(genWindowPeerGrouperOps, "window_peer_grouper.eg.go")
}
