package slack

import (
	"fmt"
	"time"
)

/**
 * Internal events, created by this lib and not mapped to Slack APIs.
 */

// ConnectedEvent is used for when we connect to Slack
type ConnectedEvent struct {
	ConnectionCount int // 1 = first time, 2 = second time
	Info            *Info
}

// ConnectionErrorEvent contains information about a connection error
type ConnectionErrorEvent struct {
	Attempt  int
	Backoff  time.Duration // how long we'll wait before the next attempt
	ErrorObj error
}

func (c *ConnectionErrorEvent) Error() string {
	return c.ErrorObj.Error()
}

// ConnectingEvent contains information about our connection attempt
type ConnectingEvent struct {
	Attempt         int // 1 = first attempt, 2 = second attempt
	ConnectionCount int
}

// DisconnectedEvent contains information about how we disconnected
type DisconnectedEvent struct {
	Intentional bool
	Cause       error
}

// LatencyReport contains information about connection latency
type LatencyReport struct {
	Value time.Duration
}

// InvalidAuthEvent is used in case we can't even authenticate with the API
type InvalidAuthEvent struct{}

// UnmarshallingErrorEvent is used when there are issues deconstructing a response
type UnmarshallingErrorEvent struct {
	ErrorObj error
}

func (u UnmarshallingErrorEvent) Error() string {
	return u.ErrorObj.Error()
}

// MessageTooLongEvent is used when sending a message that is too long
type MessageTooLongEvent struct {
	Message   OutgoingMessage
	MaxLength int
}

func (m *MessageTooLongEvent) Error() string {
	return fmt.Sprintf("Message too long (max %d characters)", m.MaxLength)
}

// RateLimitEvent is used when Slack warns that rate-limits are being hit.
type RateLimitEvent struct{}

func (e *RateLimitEvent) Error() string {
	return "Messages are being sent too fast."
}

// OutgoingErrorEvent contains information in case there were errors sending messages
type OutgoingErrorEvent struct {
	Message  OutgoingMessage
	ErrorObj error
}

func (o OutgoingErrorEvent) Error() string {
	return o.ErrorObj.Error()
}

// IncomingEventError contains information about an unexpected error receiving a websocket event
type IncomingEventError struct {
	ErrorObj error
}

func (i *IncomingEventError) Error() string {
	return i.ErrorObj.Error()
}

// AckErrorEvent i
type AckErrorEvent struct {
	ErrorObj error
	ReplyTo  int
}

func (a *AckErrorEvent) Error() string {
	return a.ErrorObj.Error()
}
