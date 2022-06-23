package slack

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	stdurl "net/url"
	"reflect"
	"time"

	"github.com/slack-go/slack/internal/backoff"
	"github.com/slack-go/slack/internal/misc"

	"github.com/gorilla/websocket"
	"github.com/slack-go/slack/internal/errorsx"
	"github.com/slack-go/slack/internal/timex"
)

// ManageConnection can be called on a Slack RTM instance returned by the
// NewRTM method. It will connect to the slack RTM API and handle all incoming
// and outgoing events. If a connection fails then it will attempt to reconnect
// and will notify any listeners through an error event on the IncomingEvents
// channel.
//
// If the connection ends and the disconnect was unintentional then this will
// attempt to reconnect.
//
// This should only be called once per slack API! Otherwise expect undefined
// behavior.
//
// The defined error events are located in websocket_internals.go.
func (rtm *RTM) ManageConnection() {
	var (
		err  error
		info *Info
		conn *websocket.Conn
	)

	for connectionCount := 0; ; connectionCount++ {
		// start trying to connect
		// the returned err is already passed onto the IncomingEvents channel
		if info, conn, err = rtm.connect(connectionCount, rtm.useRTMStart); err != nil {
			// when the connection is unsuccessful its fatal, and we need to bail out.
			rtm.Debugf("Failed to connect with RTM on try %d: %s", connectionCount, err)
			rtm.disconnect()
			return
		}

		// lock to prevent data races with Disconnect particularly around isConnected
		// and conn.
		rtm.mu.Lock()
		rtm.conn = conn
		rtm.info = info
		rtm.mu.Unlock()

		rtm.IncomingEvents <- RTMEvent{"connected", &ConnectedEvent{
			ConnectionCount: connectionCount,
			Info:            info,
		}}

		rtm.Debugf("RTM connection succeeded on try %d", connectionCount)

		rawEvents := make(chan json.RawMessage)
		// we're now connected so we can set up listeners
		go rtm.handleIncomingEvents(rawEvents)
		// this should be a blocking call until the connection has ended
		rtm.handleEvents(rawEvents)

		select {
		case <-rtm.disconnected:
			// after handle events returns we need to check if we're disconnected
			// when this happens we need to cleanup the newly created connection.
			if err = conn.Close(); err != nil {
				rtm.Debugln("failed to close conn on disconnected RTM", err)
			}
			return
		default:
			// otherwise continue and run the loop again to reconnect
		}
	}
}

// connect attempts to connect to the slack websocket API. It handles any
// errors that occur while connecting and will return once a connection
// has been successfully opened.
// If useRTMStart is false then it uses rtm.connect to create the connection,
// otherwise it uses rtm.start.
func (rtm *RTM) connect(connectionCount int, useRTMStart bool) (*Info, *websocket.Conn, error) {
	const (
		errInvalidAuth      = "invalid_auth"
		errInactiveAccount  = "account_inactive"
		errMissingAuthToken = "not_authed"
		errTokenRevoked     = "token_revoked"
	)

	// used to provide exponential backoff wait time with jitter before trying
	// to connect to slack again
	boff := &backoff.Backoff{
		Max: 5 * time.Minute,
	}

	for {
		var (
			backoff time.Duration
		)

		// send connecting event
		rtm.IncomingEvents <- RTMEvent{"connecting", &ConnectingEvent{
			Attempt:         boff.Attempts() + 1,
			ConnectionCount: connectionCount,
		}}

		// attempt to start the connection
		info, conn, err := rtm.startRTMAndDial(useRTMStart)
		if err == nil {
			return info, conn, nil
		}

		// check for fatal errors
		switch err.Error() {
		case errInvalidAuth, errInactiveAccount, errMissingAuthToken, errTokenRevoked:
			rtm.Debugf("invalid auth when connecting with RTM: %s", err)
			rtm.IncomingEvents <- RTMEvent{"invalid_auth", &InvalidAuthEvent{}}
			return nil, nil, err
		default:
		}

		switch actual := err.(type) {
		case misc.StatusCodeError:
			if actual.Code == http.StatusNotFound {
				rtm.Debugf("invalid auth when connecting with RTM: %s", err)
				rtm.IncomingEvents <- RTMEvent{"invalid_auth", &InvalidAuthEvent{}}
				return nil, nil, err
			}
		case *RateLimitedError:
			backoff = actual.RetryAfter
		default:
		}

		backoff = timex.Max(backoff, boff.Duration())
		// any other errors are treated as recoverable and we try again after
		// sending the event along the IncomingEvents channel
		rtm.IncomingEvents <- RTMEvent{"connection_error", &ConnectionErrorEvent{
			Attempt:  boff.Attempts(),
			Backoff:  backoff,
			ErrorObj: err,
		}}

		// get time we should wait before attempting to connect again
		rtm.Debugf("reconnection %d failed: %s reconnecting in %v\n", boff.Attempts(), err, backoff)

		// wait for one of the following to occur,
		// backoff duration has elapsed, killChannel is signalled, or
		// the rtm finishes disconnecting.
		select {
		case <-time.After(backoff): // retry after the backoff.
		case intentional := <-rtm.killChannel:
			if intentional {
				rtm.killConnection(intentional, ErrRTMDisconnected)
				return nil, nil, ErrRTMDisconnected
			}
		case <-rtm.disconnected:
			return nil, nil, ErrRTMDisconnected
		}
	}
}

// startRTMAndDial attempts to connect to the slack websocket. If useRTMStart is true,
// then it returns the  full information returned by the "rtm.start" method on the
// slack API. Else it uses the "rtm.connect" method to connect
func (rtm *RTM) startRTMAndDial(useRTMStart bool) (info *Info, _ *websocket.Conn, err error) {
	var (
		url string
	)

	if useRTMStart {
		rtm.Debugf("Starting RTM")
		info, url, err = rtm.StartRTM()
	} else {
		rtm.Debugf("Connecting to RTM")
		info, url, err = rtm.ConnectRTM()
	}
	if err != nil {
		rtm.Debugf("Failed to start or connect to RTM: %s", err)
		return nil, nil, err
	}

	// install connection parameters
	u, err := stdurl.Parse(url)
	if err != nil {
		return nil, nil, err
	}
	u.RawQuery = rtm.connParams.Encode()
	url = u.String()

	rtm.Debugf("Dialing to websocket on url %s", url)
	// Only use HTTPS for connections to prevent MITM attacks on the connection.
	upgradeHeader := http.Header{}
	upgradeHeader.Add("Origin", "https://api.slack.com")
	dialer := websocket.DefaultDialer
	if rtm.dialer != nil {
		dialer = rtm.dialer
	}
	conn, _, err := dialer.Dial(url, upgradeHeader)
	if err != nil {
		rtm.Debugf("Failed to dial to the websocket: %s", err)
		return nil, nil, err
	}
	return info, conn, err
}

// killConnection stops the websocket connection and signals to all goroutines
// that they should cease listening to the connection for events.
//
// This should not be called directly! Instead a boolean value (true for
// intentional, false otherwise) should be sent to the killChannel on the RTM.
func (rtm *RTM) killConnection(intentional bool, cause error) (err error) {
	rtm.Debugln("killing connection", cause)

	if rtm.conn != nil {
		err = rtm.conn.Close()
	}

	rtm.IncomingEvents <- RTMEvent{"disconnected", &DisconnectedEvent{Intentional: intentional, Cause: cause}}

	if intentional {
		rtm.disconnect()
	}

	return err
}

// handleEvents is a blocking function that handles all events. This sends
// pings when asked to (on rtm.forcePing) and upon every given elapsed
// interval. This also sends outgoing messages that are received from the RTM's
// outgoingMessages channel. This also handles incoming raw events from the RTM
// rawEvents channel.
func (rtm *RTM) handleEvents(events chan json.RawMessage) {
	ticker := time.NewTicker(rtm.pingInterval)
	defer ticker.Stop()
	for {
		select {
		// catch "stop" signal on channel close
		case intentional := <-rtm.killChannel:
			_ = rtm.killConnection(intentional, errorsx.String("signaled"))
			return
		// detect when the connection is dead.
		case <-rtm.pingDeadman.C:
			_ = rtm.killConnection(false, ErrRTMDeadman)
			return
		// send pings on ticker interval
		case <-ticker.C:
			if err := rtm.ping(); err != nil {
				_ = rtm.killConnection(false, err)
				return
			}
		case <-rtm.forcePing:
			if err := rtm.ping(); err != nil {
				_ = rtm.killConnection(false, err)
				return
			}
		// listen for messages that need to be sent
		case msg := <-rtm.outgoingMessages:
			rtm.sendOutgoingMessage(msg)
			// listen for incoming messages that need to be parsed
		case rawEvent := <-events:
			switch rtm.handleRawEvent(rawEvent) {
			case rtmEventTypeGoodbye:
				// kill the connection, but DO NOT RETURN, a follow up kill signal will
				// be sent that still needs to be processed. this duplication is because
				// the event reader restarts once it emits the goodbye event.
				// unlike the other cases in this function a final read will be triggered
				// against the connection which will emit a kill signal. if we return early
				// this kill signal will be processed by the next connection.
				_ = rtm.killConnection(false, ErrRTMGoodbye)
			default:
			}
		}
	}
}

// handleIncomingEvents monitors the RTM's opened websocket for any incoming
// events. It pushes the raw events into the channel.
//
// This will stop executing once the RTM's when a fatal error is detected, or
// a disconnect occurs.
func (rtm *RTM) handleIncomingEvents(events chan json.RawMessage) {
	for {
		if err := rtm.receiveIncomingEvent(events); err != nil {
			select {
			case rtm.killChannel <- false:
			case <-rtm.disconnected:
			}
			return
		}
	}
}

func (rtm *RTM) sendWithDeadline(msg interface{}) error {
	// set a write deadline on the connection
	if err := rtm.conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	if err := rtm.conn.WriteJSON(msg); err != nil {
		return err
	}
	// remove write deadline
	return rtm.conn.SetWriteDeadline(time.Time{})
}

// sendOutgoingMessage sends the given OutgoingMessage to the slack websocket.
//
// It does not currently detect if a outgoing message fails due to a disconnect
// and instead lets a future failed 'PING' detect the failed connection.
func (rtm *RTM) sendOutgoingMessage(msg OutgoingMessage) {
	rtm.Debugln("Sending message:", msg)
	if len([]rune(msg.Text)) > MaxMessageTextLength {
		rtm.IncomingEvents <- RTMEvent{"outgoing_error", &MessageTooLongEvent{
			Message:   msg,
			MaxLength: MaxMessageTextLength,
		}}
		return
	}

	if err := rtm.sendWithDeadline(msg); err != nil {
		rtm.IncomingEvents <- RTMEvent{"outgoing_error", &OutgoingErrorEvent{
			Message:  msg,
			ErrorObj: err,
		}}
	}
}

// ping sends a 'PING' message to the RTM's websocket. If the 'PING' message
// fails to send then this returns an error signifying that the connection
// should be considered disconnected.
//
// This does not handle incoming 'PONG' responses but does store the time of
// each successful 'PING' send so latency can be detected upon a 'PONG'
// response.
func (rtm *RTM) ping() error {
	id := rtm.idGen.Next()
	rtm.Debugln("Sending PING ", id)
	msg := &Ping{ID: id, Type: "ping", Timestamp: time.Now().Unix()}

	if err := rtm.sendWithDeadline(msg); err != nil {
		rtm.Debugf("RTM Error sending 'PING %d': %s", id, err.Error())
		return err
	}
	return nil
}

// receiveIncomingEvent attempts to receive an event from the RTM's websocket.
// This will block until a frame is available from the websocket.
// If the read from the websocket results in a fatal error, this function will return non-nil.
func (rtm *RTM) receiveIncomingEvent(events chan json.RawMessage) error {
	event := json.RawMessage{}
	err := rtm.conn.ReadJSON(&event)

	// check if the connection was closed.
	if websocket.IsUnexpectedCloseError(err) {
		return err
	}

	switch {
	case err == io.ErrUnexpectedEOF:
		// EOF's don't seem to signify a failed connection so instead we ignore
		// them here and detect a failed connection upon attempting to send a
		// 'PING' message

		// trigger a 'PING' to detect potential websocket disconnect
		select {
		case rtm.forcePing <- true:
		case <-rtm.disconnected:
		}
	case err != nil:
		// All other errors from ReadJSON come from NextReader, and should
		// kill the read loop and force a reconnect.
		rtm.IncomingEvents <- RTMEvent{"incoming_error", &IncomingEventError{
			ErrorObj: err,
		}}

		return err
	case len(event) == 0:
		rtm.Debugln("Received empty event")
	default:
		rtm.Debugln("Incoming Event:", string(event))
		select {
		case events <- event:
		case <-rtm.disconnected:
			rtm.Debugln("disonnected while attempting to send raw event")
		}
	}

	return nil
}

// handleRawEvent takes a raw JSON message received from the slack websocket
// and handles the encoded event.
// returns the event type of the message.
func (rtm *RTM) handleRawEvent(rawEvent json.RawMessage) string {
	event := &Event{}
	err := json.Unmarshal(rawEvent, event)
	if err != nil {
		rtm.IncomingEvents <- RTMEvent{"unmarshalling_error", &UnmarshallingErrorEvent{err}}
		return ""
	}

	switch event.Type {
	case rtmEventTypeAck:
		rtm.handleAck(rawEvent)
	case rtmEventTypeHello:
		rtm.IncomingEvents <- RTMEvent{"hello", &HelloEvent{}}
	case rtmEventTypePong:
		rtm.handlePong(rawEvent)
	case rtmEventTypeGoodbye:
		// just return the event type up for goodbye, will be handled by caller.
	default:
		rtm.handleEvent(event.Type, rawEvent)
	}

	return event.Type
}

// handleAck handles an incoming 'ACK' message.
func (rtm *RTM) handleAck(event json.RawMessage) {
	ack := &AckMessage{}
	if err := json.Unmarshal(event, ack); err != nil {
		rtm.Debugln("RTM Error unmarshalling 'ack' event:", err)
		rtm.Debugln(" -> Erroneous 'ack' event:", string(event))
		return
	}

	if ack.Ok {
		rtm.IncomingEvents <- RTMEvent{"ack", ack}
	} else if ack.RTMResponse.Error != nil {
		// As there is no documentation for RTM error-codes, this
		// identification of a rate-limit warning is very brittle.
		if ack.RTMResponse.Error.Code == -1 && ack.RTMResponse.Error.Msg == "slow down, too many messages..." {
			rtm.IncomingEvents <- RTMEvent{"ack_error", &RateLimitEvent{}}
		} else {
			rtm.IncomingEvents <- RTMEvent{"ack_error", &AckErrorEvent{ack.Error, ack.ReplyTo}}
		}
	} else {
		rtm.IncomingEvents <- RTMEvent{"ack_error", &AckErrorEvent{ErrorObj: fmt.Errorf("ack decode failure")}}
	}
}

// handlePong handles an incoming 'PONG' message which should be in response to
// a previously sent 'PING' message. This is then used to compute the
// connection's latency.
func (rtm *RTM) handlePong(event json.RawMessage) {
	var (
		p Pong
	)

	rtm.resetDeadman()

	if err := json.Unmarshal(event, &p); err != nil {
		rtm.Client.log.Println("RTM Error unmarshalling 'pong' event:", err)
		return
	}

	latency := time.Since(time.Unix(p.Timestamp, 0))
	rtm.IncomingEvents <- RTMEvent{"latency_report", &LatencyReport{Value: latency}}
}

// handleEvent is the "default" response to an event that does not have a
// special case. It matches the command's name to a mapping of defined events
// and then sends the corresponding event struct to the IncomingEvents channel.
// If the event type is not found or the event cannot be unmarshalled into the
// correct struct then this sends an UnmarshallingErrorEvent to the
// IncomingEvents channel.
func (rtm *RTM) handleEvent(typeStr string, event json.RawMessage) {
	v, exists := EventMapping[typeStr]
	if !exists {
		rtm.Debugf("RTM Error - received unmapped event %q: %s\n", typeStr, string(event))
		err := fmt.Errorf("RTM Error: Received unmapped event %q: %s", typeStr, string(event))
		rtm.IncomingEvents <- RTMEvent{"unmarshalling_error", &UnmarshallingErrorEvent{err}}
		return
	}
	t := reflect.TypeOf(v)
	recvEvent := reflect.New(t).Interface()
	err := json.Unmarshal(event, recvEvent)
	if err != nil {
		rtm.Debugf("RTM Error, could not unmarshall event %q: %s\n", typeStr, string(event))
		err := fmt.Errorf("RTM Error: Could not unmarshall event %q: %s", typeStr, string(event))
		rtm.IncomingEvents <- RTMEvent{"unmarshalling_error", &UnmarshallingErrorEvent{err}}
		return
	}
	rtm.IncomingEvents <- RTMEvent{typeStr, recvEvent}
}

// EventMapping holds a mapping of event names to their corresponding struct
// implementations. The structs should be instances of the unmarshalling
// target for the matching event type.
var EventMapping = map[string]interface{}{
	"message":         MessageEvent{},
	"presence_change": PresenceChangeEvent{},
	"user_typing":     UserTypingEvent{},

	"channel_marked":          ChannelMarkedEvent{},
	"channel_created":         ChannelCreatedEvent{},
	"channel_joined":          ChannelJoinedEvent{},
	"channel_left":            ChannelLeftEvent{},
	"channel_deleted":         ChannelDeletedEvent{},
	"channel_rename":          ChannelRenameEvent{},
	"channel_archive":         ChannelArchiveEvent{},
	"channel_unarchive":       ChannelUnarchiveEvent{},
	"channel_history_changed": ChannelHistoryChangedEvent{},

	"dnd_updated":      DNDUpdatedEvent{},
	"dnd_updated_user": DNDUpdatedEvent{},

	"im_created":         IMCreatedEvent{},
	"im_open":            IMOpenEvent{},
	"im_close":           IMCloseEvent{},
	"im_marked":          IMMarkedEvent{},
	"im_history_changed": IMHistoryChangedEvent{},

	"group_marked":          GroupMarkedEvent{},
	"group_open":            GroupOpenEvent{},
	"group_joined":          GroupJoinedEvent{},
	"group_left":            GroupLeftEvent{},
	"group_close":           GroupCloseEvent{},
	"group_rename":          GroupRenameEvent{},
	"group_archive":         GroupArchiveEvent{},
	"group_unarchive":       GroupUnarchiveEvent{},
	"group_history_changed": GroupHistoryChangedEvent{},

	"file_created":         FileCreatedEvent{},
	"file_shared":          FileSharedEvent{},
	"file_unshared":        FileUnsharedEvent{},
	"file_public":          FilePublicEvent{},
	"file_private":         FilePrivateEvent{},
	"file_change":          FileChangeEvent{},
	"file_deleted":         FileDeletedEvent{},
	"file_comment_added":   FileCommentAddedEvent{},
	"file_comment_edited":  FileCommentEditedEvent{},
	"file_comment_deleted": FileCommentDeletedEvent{},

	"pin_added":   PinAddedEvent{},
	"pin_removed": PinRemovedEvent{},

	"star_added":   StarAddedEvent{},
	"star_removed": StarRemovedEvent{},

	"reaction_added":   ReactionAddedEvent{},
	"reaction_removed": ReactionRemovedEvent{},

	"pref_change": PrefChangeEvent{},

	"team_join":              TeamJoinEvent{},
	"team_rename":            TeamRenameEvent{},
	"team_pref_change":       TeamPrefChangeEvent{},
	"team_domain_change":     TeamDomainChangeEvent{},
	"team_migration_started": TeamMigrationStartedEvent{},

	"manual_presence_change": ManualPresenceChangeEvent{},

	"user_change": UserChangeEvent{},

	"emoji_changed": EmojiChangedEvent{},

	"commands_changed": CommandsChangedEvent{},

	"email_domain_changed": EmailDomainChangedEvent{},

	"bot_added":   BotAddedEvent{},
	"bot_changed": BotChangedEvent{},

	"accounts_changed": AccountsChangedEvent{},

	"reconnect_url": ReconnectUrlEvent{},

	"member_joined_channel": MemberJoinedChannelEvent{},
	"member_left_channel":   MemberLeftChannelEvent{},

	"subteam_created":         SubteamCreatedEvent{},
	"subteam_members_changed": SubteamMembersChangedEvent{},
	"subteam_self_added":      SubteamSelfAddedEvent{},
	"subteam_self_removed":    SubteamSelfRemovedEvent{},
	"subteam_updated":         SubteamUpdatedEvent{},

	"desktop_notification":       DesktopNotificationEvent{},
	"mobile_in_app_notification": MobileInAppNotificationEvent{},
}
