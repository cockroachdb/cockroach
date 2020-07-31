// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	crypto_rand "crypto/rand"
	"encoding/base64"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// stateValidator will be embedded in the OIDC server and concurrent access will be managed by the
// mutex in there.
type stateValidator struct {
	states *cache.UnorderedCache
}

// Hold elements in state cache with max TTL of an hour or 5000 elements. This helps ensure that
// old state variables get cleaned out if OAuth never succeeds, and that the cache doesn't grow
// past a certain size and cause storage problems on a node.
// Successfully "used" state variables are cleared out as soon as the OAuth callback is triggered
// so the storage would only grow with "bad" login attempts.
const size = 5000
const maxTTLSeconds = 60 * 60

func newStateValidator() *stateValidator {
	return &stateValidator{
		states: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(s int, key, value interface{}) bool {
				return timeutil.Now().Unix()-value.(int64) > maxTTLSeconds || s > size
			},
		}),
	}
}

func (s *stateValidator) newState() (string, error) {
	size := 16
	state := make([]byte, size)
	if _, err := crypto_rand.Read(state); err != nil {
		return "", err
	}
	s.states.Add(string(state), timeutil.Now().UnixNano())
	return string(state), nil
}

func (s *stateValidator) validate(state string) error {
	if _, ok := s.states.Get(state); !ok {
		return errors.New("state validator: unknown state")
	}
	s.states.Del(state)
	return nil
}

func encodeOIDCState(statePb serverpb.OIDCState) (string, error) {
	stateBytes, err := protoutil.Marshal(&statePb)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(stateBytes), nil
}

func decodeOIDCState(encodedState string) (*serverpb.OIDCState, error) {
	// Cookie value should be a base64 encoded protobuf.
	stateBytes, err := base64.StdEncoding.DecodeString(encodedState)
	if err != nil {
		return nil, errors.Wrap(err, "state could not be decoded")
	}
	var stateValue serverpb.OIDCState
	if err := protoutil.Unmarshal(stateBytes, &stateValue); err != nil {
		return nil, errors.Wrap(err, "state could not be unmarshaled")
	}
	return &stateValue, nil
}
