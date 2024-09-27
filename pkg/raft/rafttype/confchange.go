// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafttype

import (
	"fmt"
	"strconv"
	"strings"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

// ConfChangeI abstracts over ConfChangeV2 and (legacy) ConfChange to allow
// treating them in a unified manner.
type ConfChangeI interface {
	AsV2() ConfChangeV2
	AsV1() (ConfChange, bool)
}

// MarshalConfChange calls Marshal on the underlying ConfChange or ConfChangeV2
// and returns the result along with the corresponding EntryType.
func MarshalConfChange(c ConfChangeI) (EntryType, []byte, error) {
	var typ EntryType
	var ccdata []byte
	var err error
	if c == nil {
		// A nil data unmarshals into an empty ConfChangeV2 and has the benefit
		// that appendEntry can never refuse it based on its size (which
		// registers as zero).
		typ = EntryConfChangeV2
		ccdata = nil
	} else if ccv1, ok := c.AsV1(); ok {
		typ = EntryConfChange
		ccdata, err = ccv1.Marshal()
	} else {
		ccv2 := c.AsV2()
		typ = EntryConfChangeV2
		ccdata, err = ccv2.Marshal()
	}
	return typ, ccdata, err
}

// AsV2 returns a V2 configuration change carrying out the same operation.
func (c ConfChange) AsV2() ConfChangeV2 {
	return ConfChangeV2{
		Changes: []ConfChangeSingle{{
			Type:   c.Type,
			NodeID: c.NodeID,
		}},
		Context: c.Context,
	}
}

func (c ConfChange) Marshal() ([]byte, error) {
	return (&pb.ConfChange{
		Type:    pb.ConfChangeType(c.Type),
		NodeID:  pb.PeerID(c.NodeID),
		Context: c.Context,
	}).Marshal()
}

func (c *ConfChange) Unmarshal(data []byte) error {
	var pbcc pb.ConfChange
	if err := pbcc.Unmarshal(data); err != nil {
		return err
	}
	c.Type = ConfChangeType(pbcc.Type)
	c.NodeID = PeerID(pbcc.NodeID)
	c.Context = pbcc.Context
	return nil
}

// AsV1 returns the ConfChange and true.
func (c ConfChange) AsV1() (ConfChange, bool) {
	return c, true
}

// AsV2 is the identity.
func (c ConfChangeV2) AsV2() ConfChangeV2 { return c }

// AsV1 returns ConfChange{} and false.
func (c ConfChangeV2) AsV1() (ConfChange, bool) { return ConfChange{}, false }

// EnterJoint returns two bools. The second bool is true if and only if this
// config change will use Joint Consensus, which is the case if it contains more
// than one change or if the use of Joint Consensus was requested explicitly.
// The first bool can only be true if second one is, and indicates whether the
// Joint State will be left automatically.
func (c ConfChangeV2) EnterJoint() (autoLeave bool, ok bool) {
	// NB: in theory, more config changes could qualify for the "simple"
	// protocol but it depends on the config on top of which the changes apply.
	// For example, adding two learners is not OK if both nodes are part of the
	// base config (i.e. two voters are turned into learners in the process of
	// applying the conf change). In practice, these distinctions should not
	// matter, so we keep it simple and use Joint Consensus liberally.
	if c.Transition != ConfChangeTransitionAuto || len(c.Changes) > 1 {
		// Use Joint Consensus.
		var autoLeave bool
		switch c.Transition {
		case ConfChangeTransitionAuto:
			autoLeave = true
		case ConfChangeTransitionJointImplicit:
			autoLeave = true
		case ConfChangeTransitionJointExplicit:
		default:
			panic(fmt.Sprintf("unknown transition: %+v", c))
		}
		return autoLeave, true
	}
	return false, false
}

// LeaveJoint is true if the configuration change leaves a joint configuration.
// This is the case if the ConfChangeV2 is zero, with the possible exception of
// the Context field.
func (c ConfChangeV2) LeaveJoint() bool {
	return len(c.Changes) == 0
}

func (c ConfChangeV2) Marshal() ([]byte, error) {
	ccv2 := &pb.ConfChangeV2{
		Transition: pb.ConfChangeTransition(c.Transition),
		Changes:    make([]pb.ConfChangeSingle, len(c.Changes)),
		Context:    c.Context,
	}
	for i, cc := range c.Changes {
		ccv2.Changes[i] = pb.ConfChangeSingle{
			Type:   pb.ConfChangeType(cc.Type),
			NodeID: pb.PeerID(cc.NodeID),
		}
	}
	return ccv2.Marshal()
}

func (c *ConfChangeV2) Unmarshal(data []byte) error {
	var ccv2 pb.ConfChangeV2
	if err := ccv2.Unmarshal(data); err != nil {
		return err
	}
	c.Transition = ConfChangeTransition(ccv2.Transition)
	c.Changes = make([]ConfChangeSingle, len(ccv2.Changes))
	for i, cc := range ccv2.Changes {
		c.Changes[i] = ConfChangeSingle{
			Type:   ConfChangeType(cc.Type),
			NodeID: PeerID(cc.NodeID),
		}
	}
	c.Context = ccv2.Context
	return nil
}

// ConfChangesFromString parses a Space-delimited sequence of operations into a
// slice of ConfChangeSingle. The supported operations are:
// - vn: make n a voter,
// - ln: make n a learner,
// - rn: remove n, and
// - un: update n.
func ConfChangesFromString(s string) ([]ConfChangeSingle, error) {
	var ccs []ConfChangeSingle
	toks := strings.Split(strings.TrimSpace(s), " ")
	if toks[0] == "" {
		toks = nil
	}
	for _, tok := range toks {
		if len(tok) < 2 {
			return nil, fmt.Errorf("unknown token %s", tok)
		}
		var cc ConfChangeSingle
		switch tok[0] {
		case 'v':
			cc.Type = ConfChangeAddNode
		case 'l':
			cc.Type = ConfChangeAddLearnerNode
		case 'r':
			cc.Type = ConfChangeRemoveNode
		case 'u':
			cc.Type = ConfChangeUpdateNode
		default:
			return nil, fmt.Errorf("unknown input: %s", tok)
		}
		id, err := strconv.ParseUint(tok[1:], 10, 64)
		if err != nil {
			return nil, err
		}
		cc.NodeID = PeerID(id)
		ccs = append(ccs, cc)
	}
	return ccs, nil
}

// ConfChangesToString is the inverse to ConfChangesFromString.
func ConfChangesToString(ccs []ConfChangeSingle) string {
	var buf strings.Builder
	for i, cc := range ccs {
		if i > 0 {
			buf.WriteByte(' ')
		}
		switch cc.Type {
		case ConfChangeAddNode:
			buf.WriteByte('v')
		case ConfChangeAddLearnerNode:
			buf.WriteByte('l')
		case ConfChangeRemoveNode:
			buf.WriteByte('r')
		case ConfChangeUpdateNode:
			buf.WriteByte('u')
		default:
			buf.WriteString("unknown")
		}
		fmt.Fprintf(&buf, "%d", cc.NodeID)
	}
	return buf.String()
}
