// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqutil

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/keysutil"
	"github.com/cockroachdb/errors"
)

// RecoveryKey is an alias for RKey that is used to make it
// yaml serializable. Caution must be taken to use produced
// representation outside of tests.
type RecoveryKey roachpb.RKey

// MarshalYAML implements Marshaler interface.
func (r RecoveryKey) MarshalYAML() (interface{}, error) {
	return roachpb.RKey(r).String(), nil
}

// UnmarshalYAML implements Unmarshaler interface.
func (r *RecoveryKey) UnmarshalYAML(fn func(interface{}) error) error {
	var pretty string
	if err := fn(&pretty); err != nil {
		return err
	}
	scanner := keysutil.MakePrettyScanner(nil /* tableParser */)
	key, err := scanner.Scan(pretty)
	if err != nil {
		return errors.Wrapf(err, "failed to parse key %s", pretty)
	}
	*r = RecoveryKey(key)
	return nil
}

// AsRKey returns key as a cast to RKey.
func (r RecoveryKey) AsRKey() roachpb.RKey {
	return roachpb.RKey(r)
}
