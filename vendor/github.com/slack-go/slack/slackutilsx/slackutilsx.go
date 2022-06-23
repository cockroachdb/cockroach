// Package slackutilsx is a utility package that doesn't promise API stability.
// its for experimental functionality and utilities.
package slackutilsx

import (
	"strings"
	"unicode/utf8"
)

// ChannelType the type of channel based on the channelID
type ChannelType int

func (t ChannelType) String() string {
	switch t {
	case CTypeDM:
		return "Direct"
	case CTypeGroup:
		return "Group"
	case CTypeChannel:
		return "Channel"
	default:
		return "Unknown"
	}
}

const (
	// CTypeUnknown represents channels we cannot properly detect.
	CTypeUnknown ChannelType = iota
	// CTypeDM is a private channel between two slack users.
	CTypeDM
	// CTypeGroup is a group channel.
	CTypeGroup
	// CTypeChannel is a public channel.
	CTypeChannel
)

// DetectChannelType converts a channelID to a ChannelType.
// channelID must not be empty. However, if it is empty, the channel type will default to Unknown.
func DetectChannelType(channelID string) ChannelType {
	// intentionally ignore the error and just default to CTypeUnknown
	switch r, _ := utf8.DecodeRuneInString(channelID); r {
	case 'C':
		return CTypeChannel
	case 'G':
		return CTypeGroup
	case 'D':
		return CTypeDM
	default:
		return CTypeUnknown
	}
}

// EscapeMessage text
func EscapeMessage(message string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return replacer.Replace(message)
}

// Retryable errors return true.
type Retryable interface {
	Retryable() bool
}
