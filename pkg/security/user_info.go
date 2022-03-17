package security

import (
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// SQLUserInfo s
type SQLUserInfo struct {
	Username SQLUsername
	UserID   uuid.UUID
}
