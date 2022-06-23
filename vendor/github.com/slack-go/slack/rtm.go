package slack

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	websocketDefaultTimeout = 10 * time.Second
	defaultPingInterval     = 30 * time.Second
)

const (
	rtmEventTypeAck                 = ""
	rtmEventTypeHello               = "hello"
	rtmEventTypeGoodbye             = "goodbye"
	rtmEventTypePong                = "pong"
	rtmEventTypeDesktopNotification = "desktop_notification"
)

// StartRTM calls the "rtm.start" endpoint and returns the provided URL and the full Info block.
//
// To have a fully managed Websocket connection, use `NewRTM`, and call `ManageConnection()` on it.
func (api *Client) StartRTM() (info *Info, websocketURL string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), websocketDefaultTimeout)
	defer cancel()

	return api.StartRTMContext(ctx)
}

// StartRTMContext calls the "rtm.start" endpoint and returns the provided URL and the full Info block with a custom context.
//
// To have a fully managed Websocket connection, use `NewRTM`, and call `ManageConnection()` on it.
func (api *Client) StartRTMContext(ctx context.Context) (info *Info, websocketURL string, err error) {
	response := &infoResponseFull{}
	err = api.postMethod(ctx, "rtm.start", url.Values{"token": {api.token}}, response)
	if err != nil {
		return nil, "", err
	}

	api.Debugln("Using URL:", response.Info.URL)
	return &response.Info, response.Info.URL, response.Err()
}

// ConnectRTM calls the "rtm.connect" endpoint and returns the provided URL and the compact Info block.
//
// To have a fully managed Websocket connection, use `NewRTM`, and call `ManageConnection()` on it.
func (api *Client) ConnectRTM() (info *Info, websocketURL string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), websocketDefaultTimeout)
	defer cancel()

	return api.ConnectRTMContext(ctx)
}

// ConnectRTMContext calls the "rtm.connect" endpoint and returns the
// provided URL and the compact Info block with a custom context.
//
// To have a fully managed Websocket connection, use `NewRTM`, and call `ManageConnection()` on it.
func (api *Client) ConnectRTMContext(ctx context.Context) (info *Info, websocketURL string, err error) {
	response := &infoResponseFull{}
	err = api.postMethod(ctx, "rtm.connect", url.Values{"token": {api.token}}, response)
	if err != nil {
		api.Debugf("Failed to connect to RTM: %s", err)
		return nil, "", err
	}

	api.Debugln("Using URL:", response.Info.URL)
	return &response.Info, response.Info.URL, response.Err()
}

// RTMOption options for the managed RTM.
type RTMOption func(*RTM)

// RTMOptionUseStart as of 11th July 2017 you should prefer setting this to false, see:
// https://api.slack.com/changelog/2017-04-start-using-rtm-connect-and-stop-using-rtm-start
func RTMOptionUseStart(b bool) RTMOption {
	return func(rtm *RTM) {
		rtm.useRTMStart = b
	}
}

// RTMOptionDialer takes a gorilla websocket Dialer and uses it as the
// Dialer when opening the websocket for the RTM connection.
func RTMOptionDialer(d *websocket.Dialer) RTMOption {
	return func(rtm *RTM) {
		rtm.dialer = d
	}
}

// RTMOptionPingInterval determines how often to deliver a ping message to slack.
func RTMOptionPingInterval(d time.Duration) RTMOption {
	return func(rtm *RTM) {
		rtm.pingInterval = d
		rtm.resetDeadman()
	}
}

// RTMOptionConnParams installs parameters to embed into the connection URL.
func RTMOptionConnParams(connParams url.Values) RTMOption {
	return func(rtm *RTM) {
		rtm.connParams = connParams
	}
}

// NewRTM returns a RTM, which provides a fully managed connection to
// Slack's websocket-based Real-Time Messaging protocol.
func (api *Client) NewRTM(options ...RTMOption) *RTM {
	result := &RTM{
		Client:           *api,
		IncomingEvents:   make(chan RTMEvent, 50),
		outgoingMessages: make(chan OutgoingMessage, 20),
		pingInterval:     defaultPingInterval,
		pingDeadman:      time.NewTimer(deadmanDuration(defaultPingInterval)),
		killChannel:      make(chan bool),
		disconnected:     make(chan struct{}),
		disconnectedm:    &sync.Once{},
		forcePing:        make(chan bool),
		idGen:            NewSafeID(1),
		mu:               &sync.Mutex{},
	}

	for _, opt := range options {
		opt(result)
	}

	return result
}
