// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"embed"
	"html/template"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gorilla/mux"
)

type IndexHTMLArgs struct {
	// Insecure means disable auth entirely - anyone can use.
	Insecure         bool
	LoggedInUser     *string
	Tag              string
	Version          string
	NodeID           string
	ClusterID        string
	OIDCAutoLogin    bool
	OIDCLoginEnabled bool
	OIDCButtonText   string
	FeatureFlags     serverpb.FeatureFlags

	OIDCGenerateJWTAuthTokenEnabled bool

	LicenseType               string
	SecondsUntilLicenseExpiry int64
	IsManaged                 bool
	Admin                     serverpb.AdminServer
	Status                    serverpb.StatusServer
}

//go:embed assets/*
var assetsFS embed.FS

//go:embed templates/*
var templatesFS embed.FS

// Parsed templates stored at package level
var templates *template.Template

func init() {
	// Parse all templates at startup with custom functions
	var err error
	funcMap := template.FuncMap{
		"mul": func(a, b float64) float64 {
			return a * b
		},
		"div": func(a, b float64) float64 {
			if b == 0 {
				return 0
			}
			return a / b
		},
	}
	templates, err = template.New("").Funcs(funcMap).ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		panic("Failed to parse templates: " + err.Error())
	}
}

// Config contains the configuration parameters for the future handler.
type Config struct {
	Insecure bool
	NodeID   *base.NodeIDContainer
	Version  string
	// ClusterID can be added later when available
}

// TemplateData is the data passed to templates
type TemplateData struct {
	Insecure  bool
	NodeID    string
	Version   string
	ClusterID string
}

// PageData combines base IndexHTMLArgs with page-specific data
type PageData struct {
	IndexHTMLArgs
	Data interface{}
}

func MakeFutureHandler(cfg IndexHTMLArgs) http.HandlerFunc {
	// Create a new Gorilla Mux router
	router := mux.NewRouter()

	// Prefix all routes with /future
	futureRouter := router.PathPrefix("/future").Subrouter()

	// Redirect root to overview
	futureRouter.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/future/overview", http.StatusFound)
	}).Methods("GET")

	// Serve the overview page
	futureRouter.HandleFunc("/overview", func(w http.ResponseWriter, r *http.Request) {
		handleOverview(w, r, cfg)
	}).Methods("GET")

	// Serve the login page
	futureRouter.HandleFunc("/login", handleLogin).Methods("GET")

	// Serve the metrics page
	futureRouter.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handleMetrics(w, r, cfg)
	}).Methods("GET")

	futureRouter.HandleFunc("/sqlactivity/statements", func(w http.ResponseWriter, r *http.Request) {
		handleSqlActivityStatements(w, r, cfg)
	}).Methods("GET")

	// Serve static assets
	futureRouter.PathPrefix("/assets/").HandlerFunc(handleAssets)

	return router.ServeHTTP
}

func handleSqlActivityStatements(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	resp, err := cfg.Status.CombinedStatementStats(r.Context(), &serverpb.CombinedStatementsStatsRequest{
		Start: 0,
		End:   timeutil.Now().Unix(),
		FetchMode: &serverpb.CombinedStatementsStatsRequest_FetchMode{
			StatsType: serverpb.CombinedStatementsStatsRequest_StmtStatsOnly,
			Sort:      serverpb.StatsSortOptions_SERVICE_LAT,
		},
		Limit: 200,
	})
	if err != nil {
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "sql_activity.html", PageData{
		IndexHTMLArgs: cfg,
		Data:          resp,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleOverview serves the overview.html template
func handleOverview(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Execute the pre-parsed template
	err := templates.ExecuteTemplate(w, "overview.html", PageData{
		IndexHTMLArgs: cfg,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleLogin serves the login.html template
func handleLogin(w http.ResponseWriter, r *http.Request) {
	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Execute the pre-parsed template (login doesn't use layout, so no data needed)
	err := templates.ExecuteTemplate(w, "login.html", nil)
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleMetrics serves the metrics.html template
func handleMetrics(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Execute the pre-parsed template
	err := templates.ExecuteTemplate(w, "metrics.html", PageData{
		IndexHTMLArgs: cfg,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleAssets serves static assets from the embedded filesystem
func handleAssets(w http.ResponseWriter, r *http.Request) {
	// Get the requested path and strip the /future/assets/ prefix
	requestPath := r.URL.Path
	requestPath = strings.TrimPrefix(requestPath, "/future/assets/")

	// Construct the path within the embedded filesystem
	embedPath := "assets/" + requestPath

	// Try to read the file from the embedded filesystem
	data, err := assetsFS.ReadFile(embedPath)
	if err != nil {
		if strings.Contains(err.Error(), "file does not exist") {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Set no-cache headers for development hot reloading
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	// Detect and set content type based on file extension
	switch {
	case strings.HasSuffix(requestPath, ".js"):
		w.Header().Set("Content-Type", "application/javascript")
	case strings.HasSuffix(requestPath, ".css"):
		w.Header().Set("Content-Type", "text/css")
	case strings.HasSuffix(requestPath, ".html"):
		w.Header().Set("Content-Type", "text/html")
	case strings.HasSuffix(requestPath, ".json"):
		w.Header().Set("Content-Type", "application/json")
	case strings.HasSuffix(requestPath, ".svg"):
		w.Header().Set("Content-Type", "image/svg+xml")
	}

	// Write the file contents
	_, _ = w.Write(data)
}
