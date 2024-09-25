package raftpb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

const MUST_TRACE_ALL = false

// A RaftMessage with an attached context.
type ContextMessage struct {
	Message
	Context context.Context
}

func NewContextMessage(ctx context.Context, m Message) ContextMessage {
	if ctx == nil {
		log.Fatalf(context.Background(), "nil context")
	}
	if MUST_TRACE_ALL && tracing.SpanFromContext(ctx) == nil {
		log.Fatalf(ctx, "expected span in context: %v", ctx)
	}
	return ContextMessage{Context: ctx, Message: m}
}

func NewContextMessages(ctx context.Context, messages []Message) []ContextMessage {
	responses := make([]ContextMessage, len(messages))
	for i, m := range messages {
		responses[i] = NewContextMessage(ctx, m)
	}
	return responses
}
