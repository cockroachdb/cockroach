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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gorilla/mux"
)

//go:embed assets/*
var assetsFS embed.FS

//go:embed templates/*
var templatesFS embed.FS

// Parsed templates stored at package level
var templates *template.Template

func init() {
	// Parse all templates at startup
	var err error
	templates, err = template.ParseFS(templatesFS, "templates/*.html")
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

func MakeFutureHandler(cfg Config) http.HandlerFunc {
	// Create a new Gorilla Mux router
	router := mux.NewRouter()

	// Prefix all routes with /future
	futureRouter := router.PathPrefix("/future").Subrouter()

	// Serve the overview page for root
	futureRouter.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleOverview(w, r, cfg)
	}).Methods("GET")

	// Serve the login page
	futureRouter.HandleFunc("/login", handleLogin).Methods("GET")

	// Serve the metrics page
	futureRouter.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handleMetrics(w, r, cfg)
	}).Methods("GET")

	// Serve static assets
	futureRouter.PathPrefix("/assets/").HandlerFunc(handleAssets)

	return router.ServeHTTP
}

// handleOverview serves the overview.html template
func handleOverview(w http.ResponseWriter, r *http.Request, cfg Config) {
	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Prepare template data
	data := makeTemplateData(cfg)

	// Execute the pre-parsed template
	err := templates.ExecuteTemplate(w, "overview.html", data)
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
func handleMetrics(w http.ResponseWriter, r *http.Request, cfg Config) {
	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Prepare template data
	data := makeTemplateData(cfg)

	// Execute the pre-parsed template
	err := templates.ExecuteTemplate(w, "metrics.html", data)
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// makeTemplateData creates template data from config
func makeTemplateData(cfg Config) TemplateData {
	data := TemplateData{
		Insecure:  cfg.Insecure,
		Version:   cfg.Version,
		ClusterID: "96db9afc-e23c-43fe-99b6-6ce891cd87f8", // TODO: Get from actual cluster
	}
	if cfg.NodeID != nil {
		data.NodeID = cfg.NodeID.String()
	}
	return data
}

// handleAssets serves static assets from the embedded filesystem
func handleAssets(w http.ResponseWriter, r *http.Request) {
	log.Dev.Warningf(r.Context(), "REQUESTING: %s", r.URL.Path)

	// Get the requested path and strip the /future/assets/ prefix
	requestPath := r.URL.Path
	requestPath = strings.TrimPrefix(requestPath, "/future/assets/")

	// Construct the path within the embedded filesystem
	embedPath := "assets/" + requestPath

	log.Dev.Warningf(r.Context(), "SERVING embedded: %s", embedPath)

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
