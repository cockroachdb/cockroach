package slack

import (
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// MaxMessageTextLength is the current maximum message length in number of characters as defined here
	// https://api.slack.com/rtm#limits
	MaxMessageTextLength = 4000
)

// RTM represents a managed websocket connection. It also supports
// all the methods of the `Client` type.
//
// Create this element with Client's NewRTM() or NewRTMWithOptions(*RTMOptions)
type RTM struct {
	// Client is the main API, embedded
	Client

	idGen        IDGenerator
	pingInterval time.Duration
	pingDeadman  *time.Timer

	// Connection life-cycle
	conn             *websocket.Conn
	IncomingEvents   chan RTMEvent
	outgoingMessages chan OutgoingMessage
	killChannel      chan bool
	disconnected     chan struct{}
	disconnectedm    *sync.Once
	forcePing        chan bool

	// UserDetails upon connection
	info *Info

	// useRTMStart should be set to true if you want to use
	// rtm.start to connect to Slack, otherwise it will use
	// rtm.connect
	useRTMStart bool

	// dialer is a gorilla/websocket Dialer. If nil, use the default
	// Dialer.
	dialer *websocket.Dialer

	// mu is mutex used to prevent RTM connection race conditions
	mu *sync.Mutex

	// connParams is a map of flags for connection parameters.
	connParams url.Values
}

// signal that we are disconnected by closing the channel.
// protect it with a mutex to ensure it only happens once.
func (rtm *RTM) disconnect() {
	rtm.disconnectedm.Do(func() {
		close(rtm.disconnected)
	})
}

// Disconnect and wait, blocking until a successful disconnection.
func (rtm *RTM) Disconnect() error {
	// always push into the kill channel when invoked,
	// this lets the ManagedConnection() function properly clean up.
	// if the buffer is full then just continue on.
	select {
	case rtm.killChannel <- true:
		return nil
	case <-rtm.disconnected:
		return ErrAlreadyDisconnected
	}
}

// GetInfo returns the info structure received when calling
// "startrtm", holding metadata needed to implement a full
// chat client. It will be non-nil after a call to StartRTM().
func (rtm *RTM) GetInfo() *Info {
	return rtm.info
}

// SendMessage submits a simple message through the websocket.  For
// more complicated messages, use `rtm.PostMessage` with a complete
// struct describing your attachments and all.
func (rtm *RTM) SendMessage(msg *OutgoingMessage) {
	if msg == nil {
		rtm.Debugln("Error: Attempted to SendMessage(nil)")
		return
	}

	rtm.outgoingMessages <- *msg
}

func (rtm *RTM) resetDeadman() {
	rtm.pingDeadman.Reset(deadmanDuration(rtm.pingInterval))
}

func deadmanDuration(d time.Duration) time.Duration {
	return d * 4
}
