// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prototime

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewTimestamp creates a new proto WKT timestamp from a time.Time.
// It runs CheckValid() on the timestamp before returning.
func NewTimestamp(t time.Time) (*timestamppb.Timestamp, error) {
	timestamp := timestamppb.New(t)
	if err := timestamp.CheckValid(); err != nil {
		return nil, err
	}
	return timestamp, nil
}
