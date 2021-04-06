// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// File-based authentication is an “emergency authentication”
// mechanism for the HTTP interface, which is guaranteed to bypass KV
// operations to ascertain the validity of a login session.
//
// (For context, regular HTTP sessions are authenticated using a
// session cookie where the session ID is validated against the system
// table system.web_sessions.)
//
// As an emergency authentication mechanism, it is therefore highly
// sensitive wrt implementation. Any change to the mechanisms
// presented here must be careful to ensure that emergency access
// cannot be granted *by* users that don't already have admin-level
// privilege over the cluster.
//
// As of this writing, this guarantee is obtained by storing the
// emergency credentials in a file pointed to by a command-line flag
// (only admins can change command-line flag); and if that flag is not
// present in a file stored in the first store data directory (only
// admins can write to the store directory).
//
// Note that this design assumes there is only one authentication
// realm for the entire HTTP server. This is obviously true in a
// single-tenant cluster.
//
// In a multi-tenant cluster, however, we need to be careful about
// this. If a single HTTP server was able to accept connections on
// behalf of multiple tenants, this assumption would be
// broken. Today, we preserve this assumption as follows:
//
// - On KV nodes, the HTTP server only accepts connections on behalf
//   of the system tenant, which is incidentally the same authentication
//   realm as all the KV operations.
// - On SQL pods, there is currently no HTTP server accepting authenticated
//   requests.
// - The plan currently envisioned for SQL pod processes is that they
//   only serve requests for 1 tenant (specified in start-up flags) and
//   if we were to add a HTTP server to them, it would also be restricted
//   to that tenant.

// maxEmergencyTokenDuration is the maximum validity duration for an
// emergency token.
const maxEmergencyTokenDuration = 7 * 24 * time.Hour

// specialSession represents one emergency session.
type specialSession struct {
	// Shared secret with the client.
	secret []byte
	// User that the session logs in as when the session token is used.
	username security.SQLUsername
	// Whether the user is an admin. This is stored in this emergency
	// session metadata to avoid a KV lookup to determine the admin bit.
	isAdmin bool
	// When the special session expires.
	expiresAt time.Time
}

type specialSessions struct {
	syncutil.RWMutex
	sessions map[int64]*specialSession
}

func newSpecialSessions() *specialSessions {
	return &specialSessions{
		sessions: make(map[int64]*specialSession),
	}
}

// isValidCachedSession determine whether there is a known emergency
// session for the given input session cookie.
func (s *authenticationServer) isValidCachedSession(
	ctx context.Context, cookie *serverpb.SessionCookie,
) (isValid bool, username security.SQLUsername, isAdmin bool, err error) {
	sess, ok := func() (*specialSession, bool) {
		s.specialSessions.RLock()
		defer s.specialSessions.RUnlock()
		s, ok := s.specialSessions.sessions[cookie.ID]
		return s, ok
	}()
	if !ok {
		return false, username, false, nil
	}
	if log.V(1) {
		log.Ops.Infof(ctx, "pre-authenticated emergency session found: %v", cookie.ID)
	}
	if !bytes.Equal(sess.secret, cookie.Secret) {
		if log.V(1) {
			log.Ops.Infof(ctx, "emergency cookie secret does not match")
		}
		return false, username, false, nil
	}
	if now := s.server.clock.PhysicalTime(); !now.Before(sess.expiresAt) {
		if log.V(1) {
			log.Ops.Infof(ctx, "emergency cookie has expired")
		}
		return false, username, false, nil
	}
	log.Ops.Infof(ctx, "valid HTTP authentication bypass for user %q", sess.username)
	return true, sess.username, sess.isAdmin, nil
}

// registerSignalHandler associates the reload function with the refresh signal
// (typically SIGHUP).
func (s *authenticationServer) registerSignalHandler(
	ctx context.Context, stopper *stop.Stopper,
) error {
	ctx = logtags.AddTag(ctx, "refresh-auth-file", nil)

	return stopper.RunAsyncTask(ctx, "refresh-auth-file", func(ctx context.Context) {
		ch := sysutil.RefreshSignaledChan()
		for {
			select {
			case sig := <-ch:
				log.Ops.Infof(ctx, "received signal %q, triggering auth file reload", sig)
				s.loadDataFromFile(ctx)
				continue
			case <-ctx.Done():
			case <-stopper.ShouldQuiesce():
			}
			break
		}
	})
}

// loadDataFromFile refreshes the emergency sessions from the input file.
func (s *authenticationServer) loadDataFromFile(ctx context.Context) (hasErrors bool) {
	ctx = logtags.AddTag(ctx, "read-auth-file", nil)

	newSessions := make(map[int64]*specialSession)
	defer func() {
		// We override the sessions regardless of the outcome of the reload below.
		// This ensures that the emergency sessions get disabled if the file
		// is removed or made empty.
		s.specialSessions.Lock()
		defer s.specialSessions.Unlock()
		s.specialSessions.sessions = newSessions
	}()

	// The input file. This may be disabled by configuration.
	filename := s.server.cfg.EmergencyAuthenticationSessions
	if filename == "" {
		log.Ops.Infof(ctx, "emergency access file disabled")
		return false
	}

	// Load the data from the filesystem.
	contentsB, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Ops.Warningf(ctx, "unable to read emergency access file: %v", err)
		return true
	}

	// Process the input lines.
	for _, line := range strings.Split(string(contentsB), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || line == "" {
			// Comment or empty line.
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 5 {
			log.Ops.Warningf(ctx, "invalid format: %q", line)
			hasErrors = true
			continue
		}
		user := parts[0]
		username, err := security.MakeSQLUsernameFromUserInput(user, security.UsernameValidation)
		if err != nil {
			log.Ops.Warningf(ctx, "invalid username in %q: %v", line, err)
			hasErrors = true
			continue
		}

		isAdmin, err := strconv.ParseBool(parts[1])
		if err != nil {
			log.Ops.Warningf(ctx, "invalid admin bit in %q: %v", line, err)
			hasErrors = true
			continue
		}

		sid, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			log.Ops.Warningf(ctx, "invalid session ID in %q: %v", line, err)
			hasErrors = true
			continue
		}
		uuid, err := uuid.FromString(parts[3])
		if err != nil {
			log.Ops.Warningf(ctx, "invalid secret in %q: %v", line, err)
			hasErrors = true
			continue
		}
		nanos, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil {
			log.Ops.Warningf(ctx, "invalid timestamp nanos in %q: %v", line, err)
			hasErrors = true
			continue
		}
		expiry := timeutil.Unix(0, nanos)
		if expiry.After(timeutil.Now().Add(maxEmergencyTokenDuration)) {
			log.Ops.Warningf(ctx, "token validity is too long: %s", timeutil.Now().Sub(expiry))
			hasErrors = true
			continue
		}

		session := specialSession{
			secret:    uuid[:],
			username:  username,
			isAdmin:   isAdmin,
			expiresAt: expiry,
		}

		// Ensure a structured event is logged. This is needed for security audits.
		//
		// Note that this call here causes a new event to be logged for a
		// user every time the file is re-loaded, even if the emergency
		// auth session does not change. At the time of this writing,
		// re-loads are only triggered manually, so for the sake of simplicity
		// we consider it acceptable to have repeated events.
		//
		// This choice must be revisited if the design evolves to call the
		// reloading function periodically, without operator intervention.
		log.StructuredEvent(ctx, &eventpb.EmergencyAuthRule{
			SessionID: sid,
			Username:  username.Normalized(),
			IsAdmin:   isAdmin,
			ExpiresAt: nanos,
		})
		newSessions[sid] = &session
	}

	return hasErrors
}

// GenerateEmergencyCredentials generates an authentication line and cookie suitable
// for the emergency session authentication mechanism.
func GenerateEmergencyCredentials(
	user security.SQLUsername, isAdmin bool, duration time.Duration,
) (expiresAt time.Time, authFileEntry string, cookie *http.Cookie, err error) {
	username := user.Normalized()

	now := timeutil.Now()
	expiryTime := now.Add(duration)
	if expiryTime.After(now.Add(maxEmergencyTokenDuration)) {
		return expiryTime, "", nil, errors.Newf("expiry too far in the future (max %s)", maxEmergencyTokenDuration)
	}

	id := int64(rand.Uint64())
	uuid, err := uuid.NewV4()
	if err != nil {
		return expiryTime, "", nil, err
	}
	sCookie := &serverpb.SessionCookie{ID: id, Secret: uuid[:]}
	httpCookie, err := EncodeSessionCookie(sCookie, false /* forHTTPSOnly */)
	if err != nil {
		return expiryTime, "", nil, err
	}

	authLine := fmt.Sprintf("%s %t %d %s %d",
		username, isAdmin, id, uuid, expiryTime.UnixNano())
	return expiryTime, authLine, httpCookie, nil
}
