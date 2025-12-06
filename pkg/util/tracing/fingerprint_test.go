// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// TestStatementFingerprintPropagation tests that statement fingerprints
// are correctly propagated from parent to child spans, both locally
// and across "RPC" boundaries.
func TestStatementFingerprintPropagation(t *testing.T) {
	tr := NewTracer()

	// Test 1: Local parent-child propagation
	t.Run("local_propagation", func(t *testing.T) {
		// Create a root span and set a fingerprint
		root := tr.StartSpan("root-operation", WithRecording(tracingpb.RecordingVerbose))
		defer root.Finish()
		
		var testFingerprint int64 = 1234567890
		root.SetStatementFingerprint(testFingerprint)
		
		// Verify the fingerprint was set on the root
		require.Equal(t, testFingerprint, root.GetStatementFingerprint())
		
		// Create a child span
		child := tr.StartSpan("child-operation", WithParent(root))
		defer child.Finish()
		
		// Verify the child inherited the fingerprint
		require.Equal(t, testFingerprint, child.GetStatementFingerprint(),
			"Child span should inherit parent's statement fingerprint")
		
		// Create a grandchild
		grandchild := tr.StartSpan("grandchild-operation", WithParent(child))
		defer grandchild.Finish()
		
		// Verify the grandchild also has the fingerprint
		require.Equal(t, testFingerprint, grandchild.GetStatementFingerprint(),
			"Grandchild span should inherit fingerprint transitively")
	})

	// Test 2: Remote parent-child propagation (simulating RPC)
	t.Run("remote_propagation", func(t *testing.T) {
		// Create a span that will act as remote parent
		parent := tr.StartSpan("remote-parent", WithRecording(tracingpb.RecordingVerbose))
		var testFingerprint int64 = 9876543210
		parent.SetStatementFingerprint(testFingerprint)
		
		// Get the span metadata (this would be sent over RPC)
		parentMeta := parent.Meta()
		
		// Verify the fingerprint is in the metadata
		require.Equal(t, testFingerprint, parentMeta.statementFingerprint,
			"SpanMeta should contain the statement fingerprint")
		
		// Simulate RPC by injecting into carrier
		carrier := MetadataCarrier{MD: metadata.MD{}}
		tr.InjectMetaInto(parentMeta, carrier)
		
		// On the "remote" side, extract the metadata
		extractedMeta, err := tr.ExtractMetaFrom(carrier)
		require.NoError(t, err)
		require.Equal(t, testFingerprint, extractedMeta.statementFingerprint,
			"Extracted SpanMeta should contain the statement fingerprint")
		
		// Create a child span with the remote parent
		remoteChild := tr.StartSpan("remote-child", WithRemoteParentFromSpanMeta(extractedMeta))
		defer remoteChild.Finish()
		
		// Verify the remote child inherited the fingerprint
		require.Equal(t, testFingerprint, remoteChild.GetStatementFingerprint(),
			"Remote child span should inherit parent's statement fingerprint")
		
		parent.Finish()
	})

	// Test 3: No fingerprint propagation when not set
	t.Run("no_fingerprint", func(t *testing.T) {
		// Create a span without setting fingerprint
		parent := tr.StartSpan("parent-no-fingerprint")
		defer parent.Finish()
		
		// Verify no fingerprint
		require.Equal(t, int64(0), parent.GetStatementFingerprint())
		
		// Create child
		child := tr.StartSpan("child-no-fingerprint", WithParent(parent))
		defer child.Finish()
		
		// Verify child also has no fingerprint
		require.Equal(t, int64(0), child.GetStatementFingerprint())
	})

	// Test 4: Setting fingerprint on child doesn't affect parent
	t.Run("child_fingerprint_isolation", func(t *testing.T) {
		parent := tr.StartSpan("parent-isolation")
		defer parent.Finish()
		
		var parentFingerprint int64 = 111222333
		parent.SetStatementFingerprint(parentFingerprint)
		
		child := tr.StartSpan("child-isolation", WithParent(parent))
		defer child.Finish()
		
		// Child initially has parent's fingerprint
		require.Equal(t, parentFingerprint, child.GetStatementFingerprint())
		
		// Child sets its own fingerprint
		var childFingerprint int64 = 444555666
		child.SetStatementFingerprint(childFingerprint)
		
		// Verify child has new fingerprint
		require.Equal(t, childFingerprint, child.GetStatementFingerprint())
		
		// Verify parent still has original fingerprint
		require.Equal(t, parentFingerprint, parent.GetStatementFingerprint(),
			"Parent fingerprint should not be affected by child's change")
	})

	// Test 5: SpanMeta.String() includes fingerprint
	t.Run("string_representation", func(t *testing.T) {
		span := tr.StartSpan("test-string", WithRecording(tracingpb.RecordingVerbose))
		defer span.Finish()
		
		var testFingerprint int64 = 7777777777
		span.SetStatementFingerprint(testFingerprint)
		
		meta := span.Meta()
		metaString := meta.String()
		
		require.Contains(t, metaString, "fingerprint: 7777777777",
			"SpanMeta.String() should include the fingerprint")
	})
}

// TestStatementFingerprintInContext tests accessing fingerprints through context.
func TestStatementFingerprintInContext(t *testing.T) {
	tr := NewTracer()
	
	// Create a span with fingerprint
	ctx, span := tr.StartSpanCtx(context.Background(), "sql-operation",
		WithRecording(tracingpb.RecordingVerbose))
	defer span.Finish()
	
	var testFingerprint int64 = 5555555555
	span.SetStatementFingerprint(testFingerprint)
	
	// Simulate passing context to KV layer
	kvLayerFunc := func(ctx context.Context) int64 {
		// This is how KV layer would access the fingerprint
		if sp := SpanFromContext(ctx); sp != nil {
			return sp.GetStatementFingerprint()
		}
		return 0
	}
	
	// Verify KV layer can access the fingerprint
	retrievedFingerprint := kvLayerFunc(ctx)
	require.Equal(t, testFingerprint, retrievedFingerprint,
		"KV layer should be able to retrieve fingerprint from context")
	
	// Create child context/span (simulating deeper call stack)
	childCtx, childSpan := tr.StartSpanCtx(ctx, "kv-operation")
	defer childSpan.Finish()
	
	// Verify fingerprint is still accessible in child context
	childFingerprint := kvLayerFunc(childCtx)
	require.Equal(t, testFingerprint, childFingerprint,
		"Fingerprint should be accessible through child context")
}