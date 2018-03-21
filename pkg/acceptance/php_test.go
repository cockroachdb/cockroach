// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.

package acceptance

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDockerPHP(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	testDockerSuccess(ctx, t, "php", []string{"sh", "-c", "cd /mnt/data/php && php test.php 3"})
	testDockerFail(ctx, t, "php", []string{"sh", "-c", "cd /mnt/data/php && php test.php 1"})
}
