package ui

import (
	"context"
	"errors"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type noOIDCConfigured struct{}

func (c *noOIDCConfigured) GetOIDCConf() OIDCUIConf {
	return OIDCUIConf{
		Enabled: false,
	}
}

type testFs struct{}

func (t testFs) Open(name string) (fs.File, error) {
	if name == "test.json" {
		return ioutil.TempFile("", "test.json")
	}
	return nil, errors.New("wrong filename")
}

func TestUIHandlerDevelopment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer func() func() {
		hold := Assets
		Assets = &testFs{}
		return func() {
			Assets = hold
		}
	}()()

	cfg := Config{
		ExperimentalUseLogin: false,
		LoginEnabled:         false,
		NodeID:               &base.NodeIDContainer{},
		GetUser: func(ctx context.Context) *string {
			z := ""
			return &z
		},
		OIDC: &noOIDCConfigured{},
	}
	server := httptest.NewServer(Handler(cfg))
	defer server.Close()

	tcs := []struct {
		haveUI         bool
		devHeader      bool
		expectRootBody string
	}{
		{
			haveUI:         false,
			devHeader:      false,
			expectRootBody: string(bareIndexHTML),
		},
		{
			haveUI:         false,
			devHeader:      true,
			expectRootBody: string(indexHTML),
		},
		{
			haveUI:         true,
			devHeader:      false,
			expectRootBody: string(indexHTML),
		},
		{
			haveUI:         true,
			devHeader:      true,
			expectRootBody: string(indexHTML),
		},
	}

	for i, tc := range tcs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			HaveUI = tc.haveUI
			req, err := http.NewRequest("GET", server.URL, nil)
			require.NoError(t, err)
			if tc.devHeader {
				req.Header.Add("crdb-development", "true")
			}

			resp, err := server.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			s := &strings.Builder{}
			_, err = io.Copy(s, resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expectRootBody, s.String())

			req2, err := http.NewRequest("GET", server.URL+"/test.json", nil)
			require.NoError(t, err)
			if tc.devHeader {
				req2.Header.Add("crdb-development", "true")
			}
			resp2, err := server.Client().Do(req2)
			require.NoError(t, err)
			require.Equal(t, 200, resp2.StatusCode)
		})
	}

}

func TestUIHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cfg := Config{
		ExperimentalUseLogin: false,
		LoginEnabled:         false,
		NodeID:               &base.NodeIDContainer{},
		GetUser: func(ctx context.Context) *string {
			z := ""
			return &z
		},
		OIDC: &noOIDCConfigured{},
	}

	server := httptest.NewServer(Handler(cfg))
	defer server.Close()

	resp, err := server.Client().Get(server.URL)
	defer resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	resp, err = server.Client().Get(server.URL + "/uiconfig")
	defer resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
}
