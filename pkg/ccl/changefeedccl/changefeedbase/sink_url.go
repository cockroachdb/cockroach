// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"encoding/base64"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// SinkURL is a helper struct which for "consuming" URL query
// parameters from the underlying URL.
type SinkURL struct {
	_ util.NoCopy
	*url.URL
	q url.Values
}

func (u *SinkURL) PeekParam(p string) string {
	if u.q == nil {
		u.q = u.Query()
	}
	v := u.q.Get(p)
	return v
}

func (u *SinkURL) ConsumeParam(p string) string {
	v := u.PeekParam(p)
	u.q.Del(p)
	return v
}

func (u *SinkURL) ConsumeParams(p string) []string {
	if u.q == nil {
		u.q = u.Query()
	}
	v := u.q[p]
	u.q.Del(p)
	return v
}

func (u *SinkURL) AddParam(p string, value string) {
	if u.q == nil {
		u.q = u.Query()
	}
	u.q.Add(p, value)
}

func (u *SinkURL) SetParam(p string, value string) {
	if u.q == nil {
		u.q = u.Query()
	}
	u.q.Set(p, value)
}

func (u *SinkURL) ConsumeBool(param string, dest *bool) (wasSet bool, err error) {
	if paramVal := u.ConsumeParam(param); paramVal != "" {
		wasSet, err := strToBool(paramVal, dest)
		if err != nil {
			return false, errors.Wrapf(err, "param %s must be a bool", param)
		}
		return wasSet, err
	}
	return false, nil
}

func (u *SinkURL) ConsumeBoolParam(param string) (bool, error) {
	var b bool
	_, err := u.ConsumeBool(param, &b)
	return b, err
}

func (u *SinkURL) DecodeBase64(param string, dest *[]byte) error {
	// TODO(dan): There's a straightforward and unambiguous transformation
	//  between the base 64 encoding defined in RFC 4648 and the URL variant
	//  defined in the same RFC: simply replace all `+` with `-` and `/` with
	//  `_`. Consider always doing this for the user and accepting either
	//  variant.
	val := u.ConsumeParam(param)
	err := DecodeBase64FromString(val, dest)
	if err != nil {
		return errors.Wrapf(err, `param %s must be base 64 encoded`, param)
	}
	return nil
}

func (u *SinkURL) RemainingQueryParams() (res []string) {
	for p := range u.q {
		res = append(res, p)
	}
	return
}

func (u *SinkURL) String() string {
	if u.q != nil {
		// If we changed query params, re-encode them.
		u.URL.RawQuery = u.q.Encode()
		u.q = nil
	}
	return u.URL.String()
}

func strToBool(src string, dest *bool) (wasSet bool, err error) {
	b, err := strconv.ParseBool(src)
	if err != nil {
		return false, err
	}
	*dest = b
	return true, nil
}

func DecodeBase64FromString(src string, dest *[]byte) error {
	if src == `` {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(src)
	if err != nil {
		return err
	}
	*dest = decoded
	return nil
}
