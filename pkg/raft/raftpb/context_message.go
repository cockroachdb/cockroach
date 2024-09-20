package raftpb

import "context"

// A RaftMessage with an attached context.
type ContextMessage struct {
	Message
	Context context.Context
}

func NewContextMessage(ctx context.Context, m Message) ContextMessage {
	return ContextMessage{Context: ctx, Message: m}
}

func NewContextMessages(ctx context.Context, messages []Message) []ContextMessage {
	responses := make([]ContextMessage, len(messages))
	for i, m := range messages {
		responses[i] = NewContextMessage(ctx, m)
	}
	return responses
}
