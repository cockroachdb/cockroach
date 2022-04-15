package server

import (
	"bytes"
	"context"
	"crypto/tls"
	htemplate "html/template"
	"net/http"
	"strings"
	ttemplate "text/template"
	"time"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/security"
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
)

type serviceMgr struct {
	cfg     BaseConfig
	stopper *stop.Stopper
	ie      *sql.InternalExecutor
	mu      struct {
		syncutil.RWMutex
		mux *http.ServeMux
	}
}

func newServiceMgr(
	ctx context.Context, stopper *stop.Stopper, cfg BaseConfig, ie *sql.InternalExecutor,
) *serviceMgr {
	s := &serviceMgr{
		cfg:     cfg,
		stopper: stopper,
		ie:      ie,
	}

	return s
}

func (s *serviceMgr) start(
	ctx context.Context, workersCtx context.Context, uiTLSConfig *tls.Config,
) error {
	// TODO(knz): Fix the address configuration.
	addr := "127.0.0.1:8081"
	advaddr := addr
	httpLn, err := ListenAndUpdateAddrs(ctx, &addr, &advaddr, "http")
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

			plainRedirectServer := netutil.MakeServer(s.stopper, uiTLSConfig, mux)

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
	connManager := netutil.MakeServer(s.stopper, uiTLSConfig, http.HandlerFunc(s.baseHandler))
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
			User: security.RootUserName(),
		},
		`SELECT path, http_method, exec_method, content_type, function
-- TODO(knz): don't hardwire the table
FROM defaultdb.services`)
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
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		r := serviceRoute{
			s: s,
		}
		if row[0] != tree.DNull {
			r.path = string(tree.MustBeDString(row[0]))
		}
		if row[2] != tree.DNull {
			r.execMethod = strings.ToLower(string(tree.MustBeDString(row[2])))
		}
		if row[3] != tree.DNull {
			r.contentType = strings.ToLower(string(tree.MustBeDString(row[3])))
		}
		if row[4] != tree.DNull {
			r.fn = string(tree.MustBeDString(row[4]))
		}
		switch r.execMethod {
		case "thtml":
			if r.contentType == "" {
				r.contentType = "text/html"
			}
		case "ttext":
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

		if row[1] != tree.DNull {
			allowedMethods := strings.ToLower(string(tree.MustBeDString(row[1])))
			if strings.Contains(allowedMethods, "get") {
				r.allowedGet = true
			}
			if strings.Contains(allowedMethods, "post") {
				r.allowedPost = true
			}
		}
		mux.HandleFunc(r.path, r.serveHTTP)
		log.Infof(ctx, "configured route: %q -> %s (get=%v,post=%v)", r.path, r.execMethod, r.allowedGet, r.allowedPost)
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

func (s *serviceMgr) runExec(
	ctx context.Context, sql string, args ...interface{},
) (rowsAffected int, retErr error) {
	return s.ie.ExecEx(ctx, "run-route-exec", nil,
		// TODO(knz): Make username configurable.
		sessiondata.InternalExecutorOverride{
			User: security.RootUserName(),
		},
		sql, args...)
}

func (s *serviceMgr) runQuery(
	ctx context.Context, sql string, args ...interface{},
) (rows [][]json.JSON, retErr error) {
	it, err := s.ie.QueryIteratorEx(ctx, "run-route-query", nil,
		// TODO(knz): Make username configurable.
		sessiondata.InternalExecutorOverride{
			User: security.RootUserName(),
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
	allowedGet  bool
	allowedPost bool
	execMethod  string
	contentType string
	fn          string
	ht          *htemplate.Template
	tt          *ttemplate.Template
}

func (s *serviceRoute) serveHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	funcs := map[string]interface{}{
		"getInput": func(key string) string {
			return r.Form.Get(key)
		},
		"query": func(sql string, args ...interface{}) ([][]json.JSON, error) {
			return s.s.runQuery(ctx, sql, args...)
		},
		"exec": func(sql string, args ...interface{}) (int, error) {
			return s.s.runExec(ctx, sql, args...)
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
	case "thtml":
		ht, err := htemplate.New("render").Funcs(funcs).Parse(s.fn)
		if err != nil {
			reportErr(err)
			return
		}
		if err := ht.Execute(&buf, nil); err != nil {
			reportErr(err)
			return
		}
	case "ttext":
		tt, err := ttemplate.New("render").Funcs(funcs).Parse(s.fn)
		if err != nil {
			reportErr(err)
			return
		}
		if err := tt.Execute(&buf, nil); err != nil {
			reportErr(err)
			return
		}
	}

	log.Infof(ctx, "executed %q -> %q", s.fn, buf.String())

	w.Header().Add("Content-Type", s.contentType)
	w.WriteHeader(200)
	_, err := buf.WriteTo(w)
	if err != nil {
		log.Warningf(ctx, "short write: %v", err)
	}
}
