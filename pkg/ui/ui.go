// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ui embeds the assets for the web UI into the Cockroach binary.
//
// By default, it serves a stub web UI. Linking with distoss or distccl will
// replace the stubs with the OSS UI or the CCL UI, respectively. The exported
// symbols in this package are thus function pointers instead of functions so
// that they can be mutated by init hooks.
package ui

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	assetfs "github.com/elazarl/go-bindata-assetfs"
)

// Asset loads and returns the asset for the given name. It returns an error if
// the asset could not be found or could not be loaded.
var Asset func(name string) ([]byte, error)

// AssetDir returns the file names below a certain directory in the embedded
// filesystem.
//
// For example, if the embedded filesystem contains the following hierarchy:
//
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
//
// AssetDir("") returns []string{"data"}
// AssetDir("data") returns []string{"foo.txt", "img"}
// AssetDir("data/img") returns []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") return errors
var AssetDir func(name string) ([]string, error)

// AssetInfo loads and returns metadata for the asset with the given name. It
// returns an error if the asset could not be found or could not be loaded.
var AssetInfo func(name string) (os.FileInfo, error)

// haveUI returns whether the admin UI has been linked into the binary.
func haveUI() bool {
	return Asset != nil && AssetDir != nil && AssetInfo != nil
}

// indexTemplate takes arguments about the current session and returns HTML
// which includes the UI JavaScript bundles, plus a script tag which sets the
// currently logged in user so that the UI JavaScript can decide whether to show
// a login page.
var indexHTMLTemplate = template.Must(template.New("index").Parse(`<!DOCTYPE html>
<html>
	<head>
		<title>Cockroach Console</title>
		<meta charset="UTF-8">
		<link href="favicon.ico" rel="shortcut icon">
	</head>
	<body>
		<div id="react-layout"></div>

		<script>
			window.dataFromServer = {{.}};
		</script>

		<script src="protos.dll.js" type="text/javascript"></script>
		<script src="vendor.dll.js" type="text/javascript"></script>
		<script src="bundle.js" type="text/javascript"></script>
	</body>
</html>
`))

type indexHTMLArgs struct {
	ExperimentalUseLogin bool
	LoginEnabled         bool
	LoggedInUser         *string
	Tag                  string
	Version              string
	NodeID               string
	OIDCAutoLogin        bool
	OIDCLoginEnabled     bool
	OIDCButtonText       string
}

// OIDCUIConf is a variable that stores data required by the
// Admin UI to display and manage the OIDC login flow. It is
// provided by the `oidcAuthenticationServer` at runtime
// since that's where all the OIDC configuration is centralized.
type OIDCUIConf struct {
	ButtonText string
	AutoLogin  bool
	Enabled    bool
}

// OIDCUI is an interface that our OIDC configuration must implement in order to be able
// to pass relevant configuration info to the ui module. This is to pass through variables that
// are necessary to render an appropriate user interface for OIDC support and to set the state
// cookie that OIDC requires for securing auth requests.
type OIDCUI interface {
	GetOIDCConf() OIDCUIConf
}

// bareIndexHTML is used in place of indexHTMLTemplate when the binary is built
// without the web UI.
var bareIndexHTML = []byte(fmt.Sprintf(`<!DOCTYPE html>
<title>CockroachDB</title>
Binary built without web UI.
<hr>
<em>%s</em>`, build.GetInfo().Short()))

// Config contains the configuration parameters for Handler.
type Config struct {
	ExperimentalUseLogin bool
	LoginEnabled         bool
	NodeID               *base.NodeIDContainer
	GetUser              func(ctx context.Context) *string
	OIDC                 OIDCUI
}

// Handler returns an http.Handler that serves the UI,
// including index.html, which has some login-related variables
// templated into it, as well as static assets.
func Handler(cfg Config) http.Handler {
	fileServer := http.FileServer(&assetfs.AssetFS{
		Asset:     Asset,
		AssetDir:  AssetDir,
		AssetInfo: AssetInfo,
	})
	buildInfo := build.GetInfo()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !haveUI() {
			http.ServeContent(w, r, "index.html", buildInfo.GoTime(), bytes.NewReader(bareIndexHTML))
			return
		}

		if r.URL.Path != "/" {
			fileServer.ServeHTTP(w, r)
			return
		}

		oidcConf := cfg.OIDC.GetOIDCConf()

		if err := indexHTMLTemplate.Execute(w, indexHTMLArgs{
			ExperimentalUseLogin: cfg.ExperimentalUseLogin,
			LoginEnabled:         cfg.LoginEnabled,
			LoggedInUser:         cfg.GetUser(r.Context()),
			Tag:                  buildInfo.Tag,
			Version:              build.VersionPrefix(),
			NodeID:               cfg.NodeID.String(),
			OIDCAutoLogin:        oidcConf.AutoLogin,
			OIDCLoginEnabled:     oidcConf.Enabled,
			OIDCButtonText:       oidcConf.ButtonText,
		}); err != nil {
			err = errors.Wrap(err, "templating index.html")
			http.Error(w, err.Error(), 500)
			log.Errorf(r.Context(), "%v", err)
		}
	})
}
