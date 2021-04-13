// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package security

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"
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
	cm := NewCertManager(context.Background())
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

	cm := NewCertManager(context.Background())

	cert1 := NewMockCert(ctrl)
	cert1.EXPECT().Err()
	cert1.EXPECT().Reload(gomock.Any()).Times(1).Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert1")
		})
	cm.ManageCert("cert1", cert1)

	cert2 := NewMockCert(ctrl)
	cert2.EXPECT().Err()
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

	cm := NewCertManager(context.Background())

	cert1 := NewMockCert(ctrl)
	cert1.EXPECT().Err()
	cert1.EXPECT().Reload(gomock.Any()).Times(1).Do(
		func(_ context.Context) {
			t.Log("called Reload() on cert1")
		})
	cm.ManageCert("cert1", cert1)

	cert2 := NewMockCert(ctrl)
	cert2.EXPECT().Err().Return(errors.New("reload failed"))
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

func TestNewCertManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	require.NotNil(t, NewCertManager(context.Background()))
}
