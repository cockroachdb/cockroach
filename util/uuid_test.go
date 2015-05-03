// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"bytes"
	"testing"
)

func TestUUID(t *testing.T) {
	uuid1 := NewUUID4()
	uuid2 := NewUUID4()
	if bytes.Equal(uuid1, uuid2) {
		t.Errorf("consecutive uuids equal %s", uuid1)
	}
}

func TestUUIDString(t *testing.T) {
	uuid := UUID([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	expStr := "d7aa0f5e-e42d-46d8-bdf7-16e4f9be5ebe"
	if str := uuid.String(); str != expStr {
		t.Errorf("expected txn %s; got %s", expStr, str)
	}
}
