// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package pubsub

import (
	"time"
)

// AckHandler implements ack/nack handling.
type AckHandler interface {
	// OnAck processes a message ack.
	OnAck()

	// OnNack processes a message nack.
	OnNack()
}

// Message represents a Pub/Sub message.
type Message struct {
	// ID identifies this message. This ID is assigned by the server and is
	// populated for Messages obtained from a subscription.
	//
	// This field is read-only.
	ID string

	// Data is the actual data in the message.
	Data []byte

	// Attributes represents the key-value pairs the current message is
	// labelled with.
	Attributes map[string]string

	// PublishTime is the time at which the message was published. This is
	// populated by the server for Messages obtained from a subscription.
	//
	// This field is read-only.
	PublishTime time.Time

	// DeliveryAttempt is the number of times a message has been delivered.
	// This is part of the dead lettering feature that forwards messages that
	// fail to be processed (from nack/ack deadline timeout) to a dead letter topic.
	// If dead lettering is enabled, this will be set on all attempts, starting
	// with value 1. Otherwise, the value will be nil.
	// This field is read-only.
	DeliveryAttempt *int

	// OrderingKey identifies related messages for which publish order should
	// be respected. If empty string is used, message will be sent unordered.
	OrderingKey string

	// ackh handles Ack() or Nack().
	ackh AckHandler
}

// Ack indicates successful processing of a Message passed to the Subscriber.Receive callback.
// It should not be called on any other Message value.
// If message acknowledgement fails, the Message will be redelivered.
// Client code must call Ack or Nack when finished for each received Message.
// Calls to Ack or Nack have no effect after the first call.
func (m *Message) Ack() {
	if m.ackh != nil {
		m.ackh.OnAck()
	}
}

// Nack indicates that the client will not or cannot process a Message passed to the Subscriber.Receive callback.
// It should not be called on any other Message value.
// Nack will result in the Message being redelivered more quickly than if it were allowed to expire.
// Client code must call Ack or Nack when finished for each received Message.
// Calls to Ack or Nack have no effect after the first call.
func (m *Message) Nack() {
	if m.ackh != nil {
		m.ackh.OnNack()
	}
}

// NewMessage creates a message with an AckHandler implementation, which should
// not be nil.
func NewMessage(ackh AckHandler) *Message {
	return &Message{ackh: ackh}
}

// MessageAckHandler provides access to the internal field Message.ackh.
func MessageAckHandler(m *Message) AckHandler {
	return m.ackh
}
