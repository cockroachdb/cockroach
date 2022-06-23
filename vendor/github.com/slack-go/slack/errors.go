package slack

import "github.com/slack-go/slack/internal/errorsx"

// Errors returned by various methods.
const (
	ErrAlreadyDisconnected  = errorsx.String("Invalid call to Disconnect - Slack API is already disconnected")
	ErrRTMDisconnected      = errorsx.String("disconnect received while trying to connect")
	ErrRTMGoodbye           = errorsx.String("goodbye detected")
	ErrRTMDeadman           = errorsx.String("deadman switch triggered")
	ErrParametersMissing    = errorsx.String("received empty parameters")
	ErrBlockIDNotUnique     = errorsx.String("Block ID needs to be unique")
	ErrInvalidConfiguration = errorsx.String("invalid configuration")
	ErrMissingHeaders       = errorsx.String("missing headers")
	ErrExpiredTimestamp     = errorsx.String("timestamp is too old")
)

// internal errors
const (
	errPaginationComplete = errorsx.String("pagination complete")
)
