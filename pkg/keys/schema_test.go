// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/stretchr/testify/require"
)

// TestSchemaPrint prints and asserts on the CockroachDB schema. The output file
// serves as additional documentation.
func TestSchemaPrint(t *testing.T) {
	b := strings.Builder{}
	traverseSchema(schema(), 0, func(t *schemaNode, level int) {
		b.WriteString(strings.Repeat(" ", level*2))
		b.WriteString(fmt.Sprintf("%q", t.atom))
		if typ := t.atomType; typ != atomValueNone {
			b.WriteString(fmt.Sprintf(" [%v]", typ))
		}
		if typ := t.valType; typ != valueNone {
			b.WriteString(fmt.Sprintf(" -> [%s]", typ))
		}
		b.WriteString(fmt.Sprintf(" : %s\n", t.anno))
	})
	echotest.Require(t, b.String(), filepath.Join("testdata", t.Name()+".txt"))
}

// TestSchemaLookup tests that schema nodes can be found by annotation paths.
func TestSchemaLookup(t *testing.T) {
	lookup := buildSchemaLookup(schema())
	for _, path := range []string{
		"/",
		"/Local/RangeID/Replicated/AppliedState",
		"/Local/RangeID/Unreplicated/RaftHardState",
		"/Local/Range",
		"/Local/Range/RangeDescriptor",
		"/Local/Store/StoreIdent",
		"/Local/Store/LossOfQuorumRecovery/Status",
		"/Local/LockTable/SingleKey",
		"/Meta1",
		"/Meta2",
		"/System/NodeIDGenerator",
		"/System/SpanConfig/HostOnTenant",
		"/TableData",
		"/Tenant",
	} {
		_, found := lookup[path]
		require.True(t, found, path)
	}
}

// TestSchemaPrintKey tests that schema nodes can be used to parse keys, and
// return a hint for the value under a key.
//
// TODO(pav-kv): make this approach performant and reusable for pretty-printing
// in printer.go and various helpers like SprintMVCCKeyValue.
func TestSchemaPrintKey(t *testing.T) {
	s := schema()
	lookup := buildSchemaLookup(s)
	for _, tt := range []struct {
		key  roachpb.Key
		want string
		node string
	}{{
		key:  RaftLogKey(123, 200),
		want: "/Local/RangeID=123/Unreplicated/RaftLog=200",
		node: "/Local/RangeID/Unreplicated/RaftLog",
	}, {
		key:  RaftHardStateKey(321),
		want: "/Local/RangeID=321/Unreplicated/RaftHardState",
		node: "/Local/RangeID/Unreplicated/RaftHardState",
	}, {
		key:  RangeAppliedStateKey(678),
		want: "/Local/RangeID=678/Replicated/AppliedState",
		node: "/Local/RangeID/Replicated/AppliedState",
	}, {
		key:  MakeTenantPrefix(roachpb.TenantID{InternalValue: 10}),
		want: "/Tenant=10",
		node: "/Tenant",
	}} {
		t.Run("", func(t *testing.T) {
			wantNode, ok := lookup[tt.node]
			require.True(t, ok)
			str, node := printKey(t, s, tt.key)
			require.Equal(t, tt.want, str)
			if node != wantNode {
				t.Fatal("found wrong schema node")
			}
		})
	}
}

// printKey pretty-prints the given key, according to the schema. Returns the
// printed key, and the leaf schema node.
func printKey(t *testing.T, node *schemaNode, key roachpb.Key) (string, *schemaNode) {
	t.Helper()
	str := ""
	for len(key) > 0 {
		found := false
		// TODO(pav-kv): make this lookup O(1). In most cases, all children have the
		// same atom length.
		for i := range node.children {
			if bytes.HasPrefix(key, []byte(node.children[i].atom)) {
				key = key[len(node.children[i].atom):]
				node = &node.children[i]
				found = true
				break
			}
		}
		require.True(t, found)

		str += fmt.Sprintf("/%s", node.anno)
		switch node.atomType {
		case atomValueNone:
		case atomValueUint64:
			next, val, err := encoding.DecodeUint64Ascending(key)
			require.NoError(t, err)
			str += fmt.Sprintf("=%d", val)
			key = next
		case atomValueVarInt:
			next, val, err := encoding.DecodeVarintAscending(key)
			require.NoError(t, err)
			str += fmt.Sprintf("=%d", val)
			key = next
		case atomValueVarUInt:
			next, val, err := encoding.DecodeUvarintAscending(key)
			require.NoError(t, err)
			str += fmt.Sprintf("=%d", val)
			key = next
		default:
			t.Fatalf("unsupported atom type %d", node.atomType)
		}
	}
	return str, node
}

// traverseSchema visits all nodes of the given schema. The traversal is
// pre-order, i.e. it first visits a node, and then, recursively, all its
// descendants.
func traverseSchema(root *schemaNode, level int, visit func(t *schemaNode, level int)) {
	visit(root, level)
	level++
	for i := range root.children {
		traverseSchema(&root.children[i], level, visit)
	}
}

// buildSchemaLookup returns a lookup table mapping annotation paths into the
// corresponding schema nodes.
func buildSchemaLookup(root *schemaNode) map[string]*schemaNode {
	lookup := map[string]*schemaNode{}
	var anno []string
	traverseSchema(root, 0, func(t *schemaNode, level int) {
		anno = append(anno[:level], t.anno)
		lookup[filepath.Join(anno...)] = t
	})
	return lookup
}
