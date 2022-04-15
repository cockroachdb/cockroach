package server

import (
	"bytes"
	"context"
	"crypto/tls"
	gojson "encoding/json"
	htemplate "html/template"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/open2b/scriggo"
	"github.com/open2b/scriggo/native"
)

type serviceMgr struct {
	cfg     BaseConfig
	authn   *authenticationV2Server
	stopper *stop.Stopper
	ie      *sql.InternalExecutor
	mu      struct {
		syncutil.RWMutex
		mux *http.ServeMux
	}
}

func newServiceMgr(
	ctx context.Context,
	stopper *stop.Stopper,
	cfg BaseConfig,
	ie *sql.InternalExecutor,
	sqlServer *SQLServer,
) *serviceMgr {
	s := &serviceMgr{
		cfg:     cfg,
		stopper: stopper,
		ie:      ie,
		authn: &authenticationV2Server{
			sqlServer:  sqlServer,
			authServer: newAuthenticationServer(cfg.Config, sqlServer),
			ctx:        ctx,
		},
	}

	return s
}

func (s *serviceMgr) start(
	ctx context.Context, workersCtx context.Context, uiTLSConfig *tls.Config,
) error {
	if !s.cfg.EnableServices {
		// Everything disabled. Do nothing.
		return nil
	}

	// TODO(knz): Fix the address configuration.
	httpLn, err := ListenAndUpdateAddrs(ctx, &s.cfg.ServicesAddr, &s.cfg.ServicesAdvertiseAddr, "svc")
	if err != nil {
		return err
	}
	// The HTTP listener shutdown worker, which closes everything under
	// the HTTP port when the stopper indicates we are shutting down.
	waitQuiesce := func(ctx context.Context) {
		// NB: we can't do this as a Closer because (*Server).ServeWith is
		// running in a worker and usually sits on accept() which unblocks
		// only when the listener closes. In other words, the listener needs
		// to close when quiescing starts to allow that worker to shut down.
		<-s.stopper.ShouldQuiesce()
		if err := httpLn.Close(); err != nil {
			log.Ops.Fatalf(ctx, "%v", err)
		}
	}
	if err := s.stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
		waitQuiesce(workersCtx)
		return err
	}

	if uiTLSConfig != nil {
		httpMux := cmux.New(httpLn)
		clearL := httpMux.Match(cmux.HTTP1())
		tlsL := httpMux.Match(cmux.Any())
		// Dispatch incoming requests to either clearL or tlsL.
		if err := s.stopper.RunAsyncTask(workersCtx, "serve-services", func(context.Context) {
			netutil.FatalIfUnexpected(httpMux.Serve())
		}); err != nil {
			return err
		}

		if err := s.stopper.RunAsyncTask(workersCtx, "serve-services-https-redirect", func(context.Context) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				if HSTSEnabled.Get(&s.cfg.Settings.SV) {
					w.Header().Set(hstsHeaderKey, hstsHeaderValue)
				}
				http.Redirect(w, r, "https://"+r.Host+r.RequestURI, http.StatusTemporaryRedirect)
			})

			plainRedirectServer := netutil.MakeServer(workersCtx, s.stopper, uiTLSConfig, mux)

			netutil.FatalIfUnexpected(plainRedirectServer.Serve(clearL))
		}); err != nil {
			return err
		}

		httpLn = tls.NewListener(tlsL, uiTLSConfig)
	}

	// Periodically load the routes from SQL.
	if err := s.stopper.RunAsyncTask(workersCtx, "services-reload", func(ctx context.Context) {
		for {
			select {
			case <-s.stopper.ShouldQuiesce():
				return
			case <-workersCtx.Done():
				return
			case <-time.After(5 * time.Second):
				if err := s.reloadRoutes(workersCtx); err != nil {
					log.Warningf(workersCtx, "unable to load service routes: %v", err)
				}
			}
		}
	}); err != nil {
		return err
	}

	// Serve the HTTP endpoint.
	connManager := netutil.MakeServer(workersCtx, s.stopper, uiTLSConfig, http.HandlerFunc(s.baseHandler))
	return s.stopper.RunAsyncTask(workersCtx, "server-http-services", func(context.Context) {
		netutil.FatalIfUnexpected(connManager.Serve(httpLn))
	})
}

func (s *serviceMgr) baseHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	mux := s.mu.mux
	s.mu.RUnlock()
	if mux == nil {
		w.Header().Add("Content-Type", "text/plain")
		w.WriteHeader(500)
		w.Write([]byte("services not ready\n"))
		return
	}

	// Disable caching of responses.
	w.Header().Set("Cache-control", "no-cache")

	if HSTSEnabled.Get(&s.cfg.Settings.SV) {
		w.Header().Set("Strict-Transport-Security", hstsHeaderValue)
	}
	mux.ServeHTTP(w, r)
}

func (s *serviceMgr) reloadRoutes(ctx context.Context) (retErr error) {
	mux := http.NewServeMux()
	it, err := s.ie.QueryIteratorEx(ctx, "get-routes", nil,
		sessiondata.InternalExecutorOverride{
			User: username.RootUserName(),
		},
		// TODO: combine with default options, get owner, etc
		`SELECT route, options FROM system.service_rules`)
	if err != nil {
		return err
	}
	defer func(it sqlutil.InternalRows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)
	ok, err := it.Next(ctx)
	if err != nil {
		return err
	}
	var routeErrs error
	var decodeBuf bytes.Buffer
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		r := serviceRoute{
			s: s,
		}
		input := struct {
			HTTPMethod  string `json:"http_method"`
			ExecMethod  string `json:"exec_method"`
			Function    string
			ContentType string `json:"content_type"`
			AuthnMethod string `json:"authn_method"`
			Database    string `json:"current_database"`
			VHost       string `json:"virtual_host"`
		}{}
		path := string(tree.MustBeDString(row[0]))
		j, err := tree.AsJSON(row[1], sessiondatapb.DataConversionConfig{}, time.UTC)
		if err != nil {
			thisErr := errors.WithDetail(err, row[0].String())
			log.Errorf(ctx, "%v", thisErr)
			routeErrs = errors.CombineErrors(routeErrs, thisErr)
			continue
		}
		decodeBuf.Reset()
		j.Format(&decodeBuf)
		if err := gojson.Unmarshal(decodeBuf.Bytes(), &input); err != nil {
			thisErr := errors.WithDetail(err, row[0].String())
			log.Errorf(ctx, "%v", thisErr)
			routeErrs = errors.CombineErrors(routeErrs, thisErr)
			continue
		}
		log.Infof(ctx, "decoded service parameters: %+v", input)

		r.curDb = input.Database
		r.path = input.VHost + path
		r.execMethod = strings.ToLower(input.ExecMethod)
		r.contentType = strings.ToLower(input.ContentType)
		switch r.execMethod {
		case "login-header", "logout-header", "login-cookie", "logout-cookie":
			// Pre-defined handlers.
		case "scriggo":
			if r.contentType == "" {
				r.contentType = "text/html"
			}
		case "text":
			if r.contentType == "" {
				r.contentType = "text/plain"
			}
		default:
			thisErr := errors.Newf(
				"invalid exec method for service route %q: %s", r.path, r.execMethod)
			log.Errorf(ctx, "%v", thisErr)
			routeErrs = errors.CombineErrors(routeErrs, thisErr)
			continue
		}

		switch r.execMethod {
		case "scriggo", "text":
			if input.Function == "" {
				thisErr := errors.Newf(
					"missing function for service route %q", r.path)
				log.Errorf(ctx, "%v", thisErr)
				routeErrs = errors.CombineErrors(routeErrs, thisErr)
			}
			r.fn = input.Function
		}

		r.authnMethod = strings.ToLower(input.AuthnMethod)
		if r.authnMethod == "" {
			// Services must opt out of authentication explicitly.
			r.authnMethod = "header"
		}
		switch r.authnMethod {
		case "cookie", "header", "anonymous":
			// OK
		default:
			thisErr := errors.Newf(
				"invalid authentication method for service route %q: %s", r.path, r.authnMethod)
			log.Errorf(ctx, "%v", thisErr)
			routeErrs = errors.CombineErrors(routeErrs, thisErr)
			continue
		}

		switch r.execMethod {
		case "login-cookie":
			mux.HandleFunc(r.path, s.authn.authServer.demoLogin)
			log.Infof(ctx, "configured login-cookie route: %q", r.path)
		case "logout-cookie":
			mux.HandleFunc(r.path, s.logoutCookie)
			log.Infof(ctx, "configured logout-cookie route: %q", r.path)
		case "login-header":
			mux.HandleFunc(r.path, s.authn.login)
			log.Infof(ctx, "configured login-header route: %q", r.path)
		case "logout-header":
			mux.HandleFunc(r.path, s.authn.logout)
			log.Infof(ctx, "configured logout-header route: %q", r.path)
		default:
			if input.HTTPMethod != "" {
				allowedMethods := strings.ToLower(input.HTTPMethod)
				if strings.Contains(allowedMethods, "get") {
					r.allowedGet = true
				}
				if strings.Contains(allowedMethods, "post") {
					r.allowedPost = true
				}
			} else {
				r.allowedGet = true
			}

			// Wrap the request in an authentication mux if the authn
			// method specifies so.
			handler := r.serveHTTP
			switch r.authnMethod {
			case "header":
				hmux := &authenticationV2Mux{
					s:     s.authn,
					inner: http.HandlerFunc(handler),
				}
				handler = hmux.ServeHTTP
			case "cookie":
				hmux := &authenticationMux{
					server: s.authn.authServer,
					inner:  http.HandlerFunc(handler),
				}
				handler = hmux.ServeHTTP
			}

			mux.HandleFunc(r.path, handler)

			log.Infof(ctx, "configured route: %q -> %s (get=%v,post=%v,authn=%v)", r.path, r.execMethod, r.allowedGet, r.allowedPost, r.authnMethod)
		}
	}
	err = errors.CombineErrors(err, routeErrs)
	if err != nil {
		return err
	}

	// Atomic reconfigure all routes.
	s.mu.Lock()
	s.mu.mux = mux
	s.mu.Unlock()

	return nil
}

func (s *serviceMgr) logoutCookie(w http.ResponseWriter, r *http.Request) {
	_, session, err := s.authn.authServer.getSession(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Revoke the session.
	// TODO(knz): Share code with the Logout() method.
	if n, err := s.ie.ExecEx(
		r.Context(),
		"revoke-auth-session",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		`UPDATE system.web_sessions SET "revokedAt" = now() WHERE id = $1`,
		session.ID,
	); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else if n == 0 {
		http.Error(w, "no session found", http.StatusBadRequest)
		return
	}
	cookie := makeCookieWithValue("", false /* forHTTPSOnly */)
	cookie.MaxAge = -1
	w.Header()["Set-Cookie"] = []string{cookie.String()}
	w.Header()["Location"] = []string{"/"}
	w.WriteHeader(302)
	_, _ = w.Write([]byte("logged out"))
}

func (s *serviceMgr) runExec(
	ctx context.Context, db string, username username.SQLUsername, sql string, args ...interface{},
) (rowsAffected int, retErr error) {
	return s.ie.ExecEx(ctx, "run-route-exec", nil,
		sessiondata.InternalExecutorOverride{
			User:     username,
			Database: db,
		},
		sql, args...)
}

func (s *serviceMgr) runQuery(
	ctx context.Context, db string, username username.SQLUsername, sql string, args ...interface{},
) (rows [][]json.JSON, retErr error) {
	it, err := s.ie.QueryIteratorEx(ctx, "run-route-query", nil,
		sessiondata.InternalExecutorOverride{
			User:     username,
			Database: db,
		},
		sql, args...)
	if err != nil {
		return nil, err
	}
	defer func(it sqlutil.InternalRows) {
		retErr = errors.CombineErrors(retErr, it.Close())
	}(it)
	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		srow := make([]json.JSON, len(row))
		for cidx, d := range row {
			j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				return nil, err
			}
			srow[cidx] = j
		}
		rows = append(rows, srow)
	}
	return rows, err
}

type serviceRoute struct {
	s           *serviceMgr
	path        string
	curDb       string
	allowedGet  bool
	allowedPost bool
	authnMethod string
	execMethod  string
	contentType string
	fn          string
	ht          *htemplate.Template
}

func (s *serviceRoute) serveHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// FIXME(knz): use configurable username for anonymous requests.
	username := username.MakeSQLUsernameFromPreNormalizedString("anonymous")
	if s.authnMethod != "anonymous" {
		username = getSQLUsername(ctx)
	}

	resultCode := 200

	funcs := map[string]native.Declaration{
		"redirect": func(url string) string {
			resultCode = http.StatusTemporaryRedirect
			w.Header().Set("Location", url)
			return ""
		},
		"getPath": func() string {
			return r.URL.Path
		},
		"getInput": func(key string) string {
			return r.Form.Get(key)
		},
		"toText": func(j json.JSON) string {
			s, err := j.AsText()
			if err != nil {
				panic(err)
			}
			return *s
		},
		"getUser": func() string {
			return username.Normalized()
		},
		"query": func(sql string, args ...interface{}) [][]json.JSON {
			res, err := s.s.runQuery(ctx, s.curDb, username, sql, args...)
			if err != nil {
				panic(err) // handled by scriggo
			}
			return res
		},
		"exec": func(sql string, args ...interface{}) int {
			res, err := s.s.runExec(ctx, s.curDb, username, sql, args...)
			if err != nil {
				panic(err) // handled by scriggo
			}
			return res
		},
	}

	reportErr := func(err error) {
		w.Header().Add("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		w.Write([]byte("\n"))
	}

	switch {
	case r.Method == "POST" && s.allowedPost:
	case r.Method == "GET" && s.allowedGet:
	default:
		w.Header().Add("Content-Type", "text/plain")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		reportErr(err)
		return
	}

	var buf bytes.Buffer
	switch s.execMethod {
	case "text":
		buf.WriteString(s.fn)
	case "scriggo":
		fsys := scriggo.Files{"index.txt": []byte(s.fn)}
		opts := &scriggo.BuildOptions{
			Globals: native.Declarations(funcs),
		}
		template, err := scriggo.BuildTemplate(fsys, "index.txt", opts)
		if err != nil {
			reportErr(err)
			return
		}
		if err := template.Run(&buf, nil, nil); err != nil {
			reportErr(err)
			return
		}
	}

	log.Infof(ctx, "executed %q -> %q", s.fn, buf.String())

	w.Header().Add("Content-Type", s.contentType)
	w.WriteHeader(resultCode)
	_, err := buf.WriteTo(w)
	if err != nil {
		log.Warningf(ctx, "short write: %v", err)
	}
}
