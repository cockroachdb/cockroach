// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logconfig

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// Export exports the configuration in the PlantUML synax.
func (c *Config) Export() (string, string) {
	var buf bytes.Buffer
	buf.WriteString("@startuml\nleft to right direction\n")

	// Export the channels.
	buf.WriteString("component sources {\n")
	for _, ch := range channelValues {
		fmt.Fprintf(&buf, "() %s\n", ch)
	}
	buf.WriteString("cloud stray as \"stray\\nerrors\"\n}\n")

	// The process stderr stream.
	buf.WriteString("queue stderr\n")

	// links collects the relationships. We need to collect them and
	// print them at the end because plantUML does not support
	// interleaving box and arrow declarations.
	links := []string{}

	// When a channel receives additional processing inside the logging
	// package, e.g. filtering or removing redaction markers, we add
	// additional boxes in a "process" component to indicate this.
	processing := []string{}
	pNum := 1
	process := func(target string, filter logpb.Severity, redact, redactable bool) string {
		if !redactable {
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"strip\"", pkey))
			links = append(links, fmt.Sprintf("%s --> %s", pkey, target))
			target = pkey
		}
		if redact {
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"redact\"", pkey))
			links = append(links, fmt.Sprintf("%s --> %s", pkey, target))
			target = pkey
		}
		if filter != logpb.Severity_INFO {
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"filter:%s\"", pkey, filter.String()[:1]))
			links = append(links, fmt.Sprintf("%s --> %s", pkey, target))
			target = pkey
		}
		return target
	}

	// Export the files. We are grouping per directory.
	//
	// folderNames collects the directory names.
	folderNames := []string{}
	// folders map each directory to a list of files within.
	folders := map[string][]string{}
	fileNum := 1
	for _, fc := range c.Sinks.FileGroups {
		if fc.Filter == logpb.Severity_NONE {
			// This file is not collecting anything. Skip it.
			continue
		}
		fileKey := fmt.Sprintf("f%d", fileNum)
		fileNum++
		fileName := "cockroach.log"
		if fc.prefix != "default" {
			fileName = fmt.Sprintf("cockroach-%s.log", fc.prefix)
		}
		if _, ok := folders[*fc.Dir]; !ok {
			folderNames = append(folderNames, *fc.Dir)
		}
		folders[*fc.Dir] = append(folders[*fc.Dir],
			fmt.Sprintf("file %s as \"%s\"", fileKey, fileName))

		target := process(fileKey, fc.Filter, *fc.Redact, *fc.Redactable)

		for _, ch := range fc.Channels.Channels {
			links = append(links, fmt.Sprintf("%s --> %s", ch, target))
		}
	}

	// If the fd2 capture is enabled, represent it.
	if c.CaptureFd2.Enable {
		dir := *c.CaptureFd2.Dir
		if _, ok := folders[dir]; !ok {
			folderNames = append(folderNames, dir)
		}
		fileName := "cockroach-stderr.log"
		folders[dir] = append(folders[dir],
			fmt.Sprintf("file stderrfile as \"%s\"", fileName))
		links = append(links, "stray --> stderrfile")
	} else {
		links = append(links, "stray --> stderr")
	}

	// Collect the network servers.
	//
	// serverNames contains the list of server names.
	serverNames := []string{}
	// servers maps each server to its box declaration.
	servers := map[string]string{}
	for _, fc := range c.Sinks.FluentServers {
		if fc.Filter == logpb.Severity_NONE {
			continue
		}
		serverNames = append(serverNames, fc.serverName)
		servers[fc.serverName] = fmt.Sprintf("queue s__%s as \"fluent: %s:%s\"",
			fc.serverName, fc.Net, fc.Address)
		for _, ch := range fc.Channels.Channels {
			links = append(links, fmt.Sprintf("%s --> s__%s", ch, fc.serverName))
		}
	}

	// Export the stderr redirects.
	if c.Sinks.Stderr.Filter != logpb.Severity_NONE {
		target := process("stderr", c.Sinks.Stderr.Filter, c.Sinks.Stderr.Redact, c.Sinks.Stderr.Redactable)

		for _, ch := range c.Sinks.Stderr.Channels.Channels {
			links = append(links, fmt.Sprintf("%s --> %s", ch, target))
		}
	}

	// Represent the processing stages, if any.
	if len(processing) > 0 {
		buf.WriteString("component process {\n")
		for _, p := range processing {
			fmt.Fprintf(&buf, " %s\n", p)
		}
		buf.WriteString("}\n")
	}

	// Represent the files, if any.
	sort.Strings(folderNames)
	if len(folderNames) > 0 {
		buf.WriteString("artifact files {\n")
		for _, fd := range folderNames {
			fmt.Fprintf(&buf, " folder \"%s\" {\n", fd)
			for _, fdecl := range folders[fd] {
				fmt.Fprintf(&buf, "  %s\n", fdecl)
			}
			buf.WriteString(" }\n")
		}
		buf.WriteString("}\n")
	}

	// Represent the network servers, if any.
	sort.Strings(serverNames)
	if len(serverNames) > 0 {
		buf.WriteString("cloud network {\n")
		for _, s := range serverNames {
			fmt.Fprintf(&buf, " %s\n", servers[s])
		}
		buf.WriteString("}\n")
	}

	// Export the relationships.
	for _, l := range links {
		fmt.Fprintf(&buf, "%s\n", l)
	}

	// We are done!
	buf.WriteString("@enduml\n")
	uml := buf.String()
	return uml, exportKey(uml)
}

// exportKey returns the PlantUML encoded key for this diagram.
func exportKey(uml string) string {
	// DEFLATE
	var b bytes.Buffer
	w, _ := flate.NewWriter(&b, flate.BestCompression)
	io.WriteString(w, uml)
	w.Close()
	compressed := b.Bytes()

	// BASE64
	inputLength := len(compressed)
	for i := 0; i < 3-inputLength%3; i++ {
		compressed = append(compressed, byte(0))
	}

	b = bytes.Buffer{}
	for i := 0; i < inputLength; i += 3 {
		b1, b2, b3, b4 := compressed[i], compressed[i+1], compressed[i+2], byte(0)

		b4 = b3 & 0x3f
		b3 = ((b2 & 0xf) << 2) | (b3 >> 6)
		b2 = ((b1 & 0x3) << 4) | (b2 >> 4)
		b1 = b1 >> 2

		for _, n := range []byte{b1, b2, b3, b4} {
			// PlantUML uses a special base64 dictionary.
			const base64 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
			b.WriteByte(byte(base64[n]))
		}
	}
	return b.String()
}
