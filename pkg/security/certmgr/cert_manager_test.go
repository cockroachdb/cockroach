// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package certmgr

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCertManager_ManageCert_RemoveCert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm := NewCertManager(ctx)
	require.Nil(t, cm.monitorCancel)
	cert1 := NewFileCert("abc", "def")
	require.Nil(t, cm.Cert("UI"))
	cm.ManageCert("UI", cert1)
	require.NotNil(t, cm.monitorCancel)
	require.Equal(t, cert1, cm.Cert("UI"))
	cert2 := NewFileCert("xyz", "123")
	cm.ManageCert("UI", cert2)
	require.NotNil(t, cm.monitorCancel)
	require.Equal(t, cert2, cm.Cert("UI"))
	cm.RemoveCert("UI")
	require.Nil(t, cm.Cert("UI"))
	require.Nil(t, cm.monitorCancel)
}

var cmLogRe = regexp.MustCompile(`event_log\.go`)

// Check that the structured event was logged.
func checkLogStructEntry(t *testing.T, expectSuccess bool, beforeReload time.Time) error {
	log.Flush()
	entries, err := log.FetchEntriesFromFiles(beforeReload.UnixNano(),
		math.MaxInt64, 10000, cmLogRe, log.WithMarkedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	foundEntry := false
	for _, e := range entries {
		if !strings.Contains(e.Message, "certs_reload") {
			continue
		}
		foundEntry = true
		// TODO(knz): Remove this when crdb-v2 becomes the new format.
		e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
		// crdb-v2 starts json with an equal sign.
		e.Message = strings.TrimPrefix(e.Message, "=")
		jsonPayload := []byte(e.Message)
		var ev eventpb.CertsReload
		if err := json.Unmarshal(jsonPayload, &ev); err != nil {
			t.Errorf("unmarshalling %q: %v", e.Message, err)
		}
		if expectSuccess {
			if ev.Success != true || ev.ErrorMessage != "" {
				t.Errorf("incorrect event: expected success with no error, got %+v", ev)
			}
		} else if ev.Success == true || ev.ErrorMessage == "" {
			t.Errorf("incorrect event: expected failure with an error, got %+v", ev)
		}
	}
	if !foundEntry {
		return errors.New("structured entry for certs_reload not found in log")
	}
	return nil
}

func TestCertManager_Reload_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm := NewCertManager(ctx)

	cert1 := NewMockCert(ctrl)
	cert1.EXPECT().Err().AnyTimes()
	cert1.EXPECT().Reload(gomock.Any()).Times(1).Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert1")
		})
	cm.ManageCert("cert1", cert1)

	cert2 := NewMockCert(ctrl)
	cert2.EXPECT().Err().AnyTimes()
	cert2.EXPECT().Reload(gomock.Any()).Times(1).Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert2")
		})
	cm.ManageCert("cert2", cert2)

	beforeReload := timeutil.Now()
	t.Log("issuing SIGHUP")
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
		t.Fatal(err)
	}

	// We use SucceedsSoon here because there may be a delay between
	// the moment SIGHUP is processed and certs are reloaded, and
	// the moment the structured logging event is actually
	// written to the log file.
	testutils.SucceedsSoon(t, func() error {
		return checkLogStructEntry(t, true, beforeReload)
	})
}

func TestCertManager_Reload_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm := NewCertManager(ctx)

	cert1 := NewMockCert(ctrl)
	cert1.EXPECT().Err().AnyTimes()
	cert1.EXPECT().Reload(gomock.Any()).Times(1).Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert1")
		})
	cm.ManageCert("cert1", cert1)

	cert2 := NewMockCert(ctrl)
	cert2.EXPECT().Err().AnyTimes().Return(errors.New("reload failed"))
	cert2.EXPECT().Reload(gomock.Any()).Times(1).Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert2")
		})
	cm.ManageCert("cert2", cert2)

	beforeReload := timeutil.Now()
	t.Log("issuing SIGHUP")
	if err := unix.Kill(unix.Getpid(), unix.SIGHUP); err != nil {
		t.Fatal(err)
	}

	// We use SucceedsSoon here because there may be a delay between
	// the moment SIGHUP is processed and certs are reloaded, and
	// the moment the structured logging event is actually
	// written to the log file.
	testutils.SucceedsSoon(t, func() error {
		return checkLogStructEntry(t, false, beforeReload)
	})
}

// Test that canceling the context while Reload is in progress, will stop
// immediately and not call Reload on the remaining certs.
// The order in which the certs are being called is random so either of the two
// certs can be reloaded. We verify that if one of the certs is reloaded, the
// other won't be and we will terminate gracefully.
func TestCertManager_Reload_CancelWhenInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.AfterTest(t)()
	defer log.ScopeWithoutShowLogs(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cm := NewCertManager(ctx)

	cert1 := NewMockCert(ctrl)
	cert1.EXPECT().Err().AnyTimes()
	var cancelIssued, firstReloadDone, allReloadsDone sync.WaitGroup
	cancelIssued.Add(1)
	firstReloadDone.Add(1)
	allReloadsDone.Add(1)
	var reloadCount int32
	cert1.EXPECT().Reload(gomock.Any()).AnyTimes().Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert1")
			newCount := atomic.AddInt32(&reloadCount, 1)
			require.EqualValues(t, 1, newCount)
			firstReloadDone.Done()
			cancelIssued.Wait()
		})
	cm.ManageCert("cert1", cert1)

	cert2 := NewMockCert(ctrl)
	cert2.EXPECT().Err().AnyTimes()
	cert2.EXPECT().Reload(gomock.Any()).AnyTimes().Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert2")
			newCount := atomic.AddInt32(&reloadCount, 1)
			require.EqualValues(t, 1, newCount)
			firstReloadDone.Done()
			cancelIssued.Wait()
		})
	cm.ManageCert("cert2", cert2)

	go func() {
		cm.Reload(ctx)
		allReloadsDone.Done()
	}()

	firstReloadDone.Wait() // Wait for the reload of the first cert to occur
	cancel()
	cancelIssued.Done()   // Cancel has been issues
	allReloadsDone.Wait() // Wait for the reload to finish
}

func TestNewCertManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	require.NotNil(t, NewCertManager(context.Background()))
}
