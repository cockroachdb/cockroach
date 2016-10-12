// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package iptables

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
)

const chain = "partition_nemesis"

// An IP is an IP address.
type IP string

// Bidirectional takes groups of nodes and creates rules which isolate them
// from one another. For example, with arguments ([ip1], [ip2, ip3]), ip1 would
// not be able to talk to ip2 and ip3, and vice versa.
func Bidirectional(partitions ...[]IP) map[IP][]IP {
	blacklist := make(map[IP][]IP)
	for i, srcPart := range partitions {
		for _, srcAddr := range srcPart {
			var blockedDests []IP
			for j, destPart := range partitions {
				if j == i {
					continue
				}
				blockedDests = append(blockedDests, destPart...)
			}
			blacklist[srcAddr] = blockedDests
		}
	}
	return blacklist
}

// Cmd is a naive command without proper support for whitespace.
type Cmd []string

// String formats the Cmd for shell copy&paste.
func (c Cmd) String() string {
	return strings.Join(c, " ")
}

// Cmds is a slice of commands.
type Cmds []Cmd

// String formats the Cmds for shell copy&paste.
func (c Cmds) String() string {
	s := make([]string, len(c))
	for i := range c {
		s[i] = c[i].String()
	}
	return strings.Join(s, " && \\\n")
}

func cmd(s string, args ...interface{}) Cmd {
	return append([]string{"iptables"}, strings.Split(fmt.Sprintf(s, args...), " ")...)
}

// Rules translates a blacklist into a map of invocations of `iptables`, keyed
// by the node on which they need to be run. A blacklist is keyed by origin,
// the values being the nodes which will be blocked from receiving inbound
// connections from the origin. For example, {ip1: [ip2, ip3]} means that rules
// will be created at ip2 and ip3 which drop incoming connections from ip1. In
// particular, asymmetry is supported: ip2 and ip3 would continue to be able to
// connect to ip1.
// The commands don't stack; before applying new rules, run Reset() to clear up
// a previous partition.
func Rules(blacklist map[IP][]IP) map[IP]Cmds {
	cmds := make(map[IP]Cmds)
	for src, dests := range blacklist {
		r := Cmds{
			cmd("-N %s", chain),          // create chain
			cmd("-I INPUT -j %s", chain), // jump to chain on incoming traffic
		}
		for _, dest := range dests {
			// Drop all incoming connection attempts which originate at the
			// blacklisted destination. Blocking outgoing packets isn't silent;
			// the client will get a permission error - but we want silent.
			r = append(r, cmd("-I %s -s %s -p tcp --dport %s -j DROP", chain, dest, base.DefaultPort))
		}
		cmds[src] = r
	}
	return cmds
}

// Reset creates commands which, when executed, undo the effects of a previous
// execution of Rules().
func Reset() Cmds {
	return Cmds{
		cmd("-D INPUT -j %s", chain), // remove from active rules
		cmd("-F %s", chain),          // remove all contained rules
		cmd("-X %s", chain),          // remove chain
	}
}
