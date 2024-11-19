// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
// onlyChans, if non-empty, restricts the output to only
// those rules that target the specified channels.
func (c *Config) Export(onlyChans ChannelList) (string, string) {
	chanSel := ChannelList{Channels: channelValues}
	if len(onlyChans.Channels) > 0 {
		chanSel = onlyChans
	}

	// links collects the relationships. We need to collect them and
	// print them at the end because plantUML does not support
	// interleaving box and arrow declarations.
	links := []string{}

	// When a channel receives additional processing inside the logging
	// package, e.g. filtering or removing redaction markers, we add
	// additional boxes in a "process" component to indicate this.
	pNum := 1
	process := func(target string, cc CommonSinkConfig) (res string, processing, links []string) {
		const defaultArrow = "-->"
		// The arrow is customized for the last step before the final
		// destination of messages, to indicate the criticality
		// ("exit-on-error"). A dotted arrow indicates that errors are
		// tolerated.
		arrow := defaultArrow
		if !*cc.Criticality {
			arrow = "..>"
		}
		// Introduce a "strip" box if the redactable flag is set.
		if !*cc.Redactable {
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"strip\"", pkey))
			links = append(links, fmt.Sprintf("%s %s %s", pkey, arrow, target))
			target = pkey
		}
		// Introduce a "redact" box if the redat flag is set.
		if *cc.Redact {
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"redact\"", pkey))
			links = append(links, fmt.Sprintf("%s --> %s", pkey, target))
			target = pkey
		}
		// Introduce the format box.
		{
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"format:%s\"", pkey, *cc.Format))
			links = append(links, fmt.Sprintf("%s --> %s", pkey, target))
			target = pkey
		}
		return target, processing, links
	}

	addFilter := func(target string, processing, links []string, filter logpb.Severity) (string, []string, []string) {
		if filter != logpb.Severity_INFO {
			pkey := fmt.Sprintf("p__%d", pNum)
			pNum++
			processing = append(processing, fmt.Sprintf("card %s as \"filter:%s\"", pkey, filter.String()[:1]))
			links = append(links, fmt.Sprintf("%s --> %s", pkey, target))
			target = pkey
		}
		return target, processing, links
	}

	// processing retains the processing rules.
	processing := []string{}

	// Export the files. We are grouping per directory.
	//
	// folderNames collects the directory names.
	folderNames := []string{}
	// folders map each directory to a list of files within.
	folders := map[string][]string{}
	fileNum := 1

	// Process the file sinks in sorted order,
	// so the output order is deteministic.
	var sortedNames []string
	for prefix := range c.Sinks.FileGroups {
		sortedNames = append(sortedNames, prefix)
	}
	sort.Strings(sortedNames)

	for _, fn := range sortedNames {
		fc := c.Sinks.FileGroups[fn]
		if fc.Filter == logpb.Severity_NONE {
			// This file is not collecting anything. Skip it.
			continue
		}
		fileKey := fmt.Sprintf("f%d", fileNum)
		fileNum++
		target := fileKey

		var syncproc, synclink []string
		if *fc.BufferedWrites {
			skey := fmt.Sprintf("buffer%d", fileNum)
			fileNum++
			syncproc = append(syncproc, fmt.Sprintf("card %s as \"buffer\"", skey))
			synclink = append(synclink, fmt.Sprintf("%s --> %s", skey, target))
			fileNum++
			target = skey
		}

		target, thisprocs, thislinks := process(target, fc.CommonSinkConfig)
		hasLink := false
		origTarget := target
		for _, ch := range fc.Channels.AllChannels.Channels {
			if !chanSel.HasChannel(ch) {
				continue
			}
			hasLink = true
			sev := fc.Channels.ChannelFilters[ch]
			target, thisprocs, thislinks = addFilter(origTarget, thisprocs, thislinks, sev)
			links = append(links, fmt.Sprintf("%s --> %s", ch, target))
		}

		if hasLink {
			processing = append(processing, syncproc...)
			processing = append(processing, thisprocs...)
			links = append(links, thislinks...)
			links = append(links, synclink...)
			fileName := "cockroach.log"
			if fc.prefix != "default" {
				fileName = fmt.Sprintf("cockroach-%s.log", fc.prefix)
			}
			if _, ok := folders[*fc.Dir]; !ok {
				folderNames = append(folderNames, *fc.Dir)
			}
			folders[*fc.Dir] = append(folders[*fc.Dir],
				fmt.Sprintf("file %s as \"%s\"", fileKey, fileName))
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
	// servers maps each server to its box declaration.
	servers := map[string]string{}

	sortedNames = nil
	for serverName := range c.Sinks.FluentServers {
		sortedNames = append(sortedNames, serverName)
	}
	sort.Strings(sortedNames)

	for _, fn := range sortedNames {
		fc := c.Sinks.FluentServers[fn]
		if fc.Filter == logpb.Severity_NONE {
			continue
		}
		skey := fmt.Sprintf("s__%s", fn)
		target, thisprocs, thislinks := process(skey, fc.CommonSinkConfig)
		origTarget := target
		hasLink := false
		for _, ch := range fc.Channels.AllChannels.Channels {
			if !chanSel.HasChannel(ch) {
				continue
			}
			sev := fc.Channels.ChannelFilters[ch]
			if sev == logpb.Severity_NONE {
				continue
			}
			hasLink = true
			target, thisprocs, thislinks = addFilter(origTarget, thisprocs, thislinks, sev)
			links = append(links, fmt.Sprintf("%s --> %s", ch, target))
		}
		if hasLink {
			processing = append(processing, thisprocs...)
			links = append(links, thislinks...)
			servers[fn] = fmt.Sprintf("queue %s as \"fluent: %s:%s\"",
				skey, fc.Net, fc.Address)
		}
	}

	// Collect HTTP sinks
	// Add the destinations into the same map, for display in the "network server"
	// section of the diagram
	sortedNames = nil
	for sinkName := range c.Sinks.HTTPServers {
		sortedNames = append(sortedNames, sinkName)
	}
	sort.Strings(sortedNames)

	for _, name := range sortedNames {
		cfg := c.Sinks.HTTPServers[name]
		if cfg.Filter == logpb.Severity_NONE {
			continue
		}
		key := fmt.Sprintf("h__%s", name)
		target, thisprocs, thislinks := process(key, cfg.CommonSinkConfig)
		origTarget := target
		hasLink := false
		for _, ch := range cfg.Channels.AllChannels.Channels {
			if !chanSel.HasChannel(ch) {
				continue
			}
			sev := cfg.Channels.ChannelFilters[ch]
			if sev == logpb.Severity_NONE {
				continue
			}
			hasLink = true
			target, thisprocs, thislinks = addFilter(origTarget, thisprocs, thislinks, sev)
			links = append(links, fmt.Sprintf("%s --> %s", ch, target))
		}
		if hasLink {
			processing = append(processing, thisprocs...)
			links = append(links, thislinks...)
			servers[name] = fmt.Sprintf("queue %s as \"http: %s\"",
				key, *cfg.Address)
		}
	}

	// Export the stderr redirects.
	if c.Sinks.Stderr.Filter != logpb.Severity_NONE {
		target, thisprocs, thislinks := process("stderr", c.Sinks.Stderr.CommonSinkConfig)
		origTarget := target
		hasLink := false
		for _, ch := range c.Sinks.Stderr.Channels.AllChannels.Channels {
			if !chanSel.HasChannel(ch) {
				continue
			}
			sev := c.Sinks.Stderr.Channels.ChannelFilters[ch]
			if sev == logpb.Severity_NONE {
				continue
			}
			hasLink = true
			target, thisprocs, thislinks = addFilter(origTarget, thisprocs, thislinks, sev)
			links = append(links, fmt.Sprintf("%s --> %s", ch, target))
		}
		if hasLink {
			links = append(links, thislinks...)
			processing = append(processing, thisprocs...)
		}
	}

	var buf bytes.Buffer
	buf.WriteString("@startuml\nleft to right direction\n")

	// Export the channels.
	buf.WriteString("component sources {\n")
	for _, ch := range chanSel.Channels {
		fmt.Fprintf(&buf, "() %s\n", ch)
	}
	buf.WriteString("cloud stray as \"stray\\nerrors\"\n}\n")

	// The process stderr stream.
	buf.WriteString("queue stderr\n")

	// Represent the processing stages, if any.
	for _, p := range processing {
		fmt.Fprintf(&buf, "%s\n", p)
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
	if len(servers) > 0 {
		buf.WriteString("cloud network {\n")
		for _, s := range servers {
			fmt.Fprintf(&buf, " %s\n", s)
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
//
// Documentation: https://plantuml.com/text-encoding
func exportKey(uml string) string {
	// DEFLATE
	var b bytes.Buffer
	w, _ := flate.NewWriter(&b, flate.BestCompression)
	_, _ = io.WriteString(w, uml)
	_ = w.Close()
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
			b.WriteByte(base64[n])
		}
	}
	return b.String()
}
