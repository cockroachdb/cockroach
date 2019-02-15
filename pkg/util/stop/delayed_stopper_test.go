// Copyright 2019 The Cockroach Authors.
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

package stop_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/assert"
)

func TestDelayedTask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ds := stop.NewDelayedStopper(stop.NewStopper())
	ctx := context.Background()
	dt1, err := ds.RunDelayedAsyncTask(ctx, "foo", func(context.Context) {}, time.Minute, nil)
	assert.Nil(t, err)
	var dt stop.DelayedTask
	dt.Cancel() // ensure that Cancel on a zero value is safe.
	dt2, err := ds.RunDelayedAsyncTask(ctx, "foo", func(context.Context) {}, 2*time.Minute, &dt)
	assert.Nil(t, err)
	assert.Equal(t, dt2, &dt)
	dt3, err := ds.RunDelayedAsyncTask(ctx, "foo", func(context.Context) {}, 2*time.Minute, nil)
	assert.Nil(t, err)
	c := make(chan struct{})
	_, err = ds.RunDelayedAsyncTask(ctx, "foo", func(context.Context) {
		close(c)
	}, time.Microsecond, nil)
	assert.Nil(t, err)
	<-c
	dt1.Cancel()
	dt2.Cancel()
	dt3.Cancel()
	ds.Stop(ctx)
	dt4, err := ds.RunDelayedAsyncTask(ctx, "foo", func(context.Context) {}, 2*time.Minute, nil)
	assert.Equal(t, err, stop.ErrUnavailable)
	assert.Nil(t, dt4)
}
