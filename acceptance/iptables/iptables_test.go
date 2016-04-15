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
	"reflect"
	"testing"
)

func TestRules(t *testing.T) {
	blacklist := map[IP][]IP{
		"192.168.0.1": {"192.168.0.2"},
		"192.168.0.2": {"192.168.0.3", "192.168.0.1"},
	}
	exp := map[IP]string{
		"192.168.0.1": `iptables -N partition_nemesis && \` + "\n" +
			`iptables -I INPUT -j partition_nemesis && \` + "\n" +
			`iptables -I partition_nemesis -s 192.168.0.2 -p tcp --dport 26257 -j DROP`,
		"192.168.0.2": `iptables -N partition_nemesis && \` + "\n" +
			`iptables -I INPUT -j partition_nemesis && \` + "\n" +
			`iptables -I partition_nemesis -s 192.168.0.3 -p tcp --dport 26257 -j DROP && \` + "\n" +
			`iptables -I partition_nemesis -s 192.168.0.1 -p tcp --dport 26257 -j DROP`,
	}
	act := make(map[IP]string)
	for addr, rules := range Rules(blacklist) {
		act[addr] = rules.String()
	}

	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("expected:\n%v\ngot:\n%v", exp, act)
	}
}

func TestBidirectional(t *testing.T) {
	partition1 := []IP{"192.168.0.1"}
	partition2 := []IP{"192.168.0.2", "192.168.0.3"}
	partition3 := []IP{"192.168.0.4", "192.168.0.5"}

	exp := map[IP][]IP{
		"192.168.0.1": {"192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5"},
		"192.168.0.2": {"192.168.0.1", "192.168.0.4", "192.168.0.5"},
		"192.168.0.3": {"192.168.0.1", "192.168.0.4", "192.168.0.5"},
		"192.168.0.4": {"192.168.0.1", "192.168.0.2", "192.168.0.3"},
		"192.168.0.5": {"192.168.0.1", "192.168.0.2", "192.168.0.3"},
	}
	act := Bidirectional(partition1, partition2, partition3)
	if !reflect.DeepEqual(act, exp) {
		t.Fatalf("expected:\n%v\ngot:\n%v", exp, act)
	}
}
