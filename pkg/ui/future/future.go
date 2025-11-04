// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

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
		"sub": func(a, b interface{}) float64 {
			return toFloat64(a) - toFloat64(b)
		},
		"add": func(a, b interface{}) float64 {
			return toFloat64(a) + toFloat64(b)
		},
		"mul": func(a, b interface{}) float64 {
			return toFloat64(a) * toFloat64(b)
		},
		"div": func(a, b interface{}) float64 {
			bVal := toFloat64(b)
			if bVal == 0 {
				return 0
			}
			return toFloat64(a) / bVal
		},
		"eq": func(a, b interface{}) bool {
			return a == b
		},
		"formatTime":    formatTimeWrapper,
		"formatBytes":   formatBytesWrapper,
		"formatNumber":  formatNumberWrapper,
		"formatPercent": formatPercentWrapper,
	}
	templates, err = template.New("").Funcs(funcMap).ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		panic("Failed to parse templates: " + err.Error())
	}
}

// toFloat64 converts various numeric types to float64
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case uint:
		return float64(v)
	case uint64:
		return float64(v)
	case uint32:
		return float64(v)
	default:
		return 0
	}
}

// Wrapper functions that accept interface{} and convert to float64
func formatTimeWrapper(val interface{}) string {
	return formatTime(toFloat64(val))
}

func formatBytesWrapper(val interface{}) string {
	return formatBytes(toFloat64(val))
}

func formatNumberWrapper(val interface{}) string {
	return formatNumber(toFloat64(val))
}

func formatPercentWrapper(val interface{}) string {
	return formatPercent(toFloat64(val))
}

// formatTime formats nanoseconds into human-readable time with appropriate units
func formatTime(nanos float64) string {
	if nanos == 0 {
		return "0 ns"
	}
	if nanos < 1000 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", nanos), "0"), ".") + " ns"
	}
	micros := nanos / 1000
	if micros < 1000 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", micros), "0"), ".") + " µs"
	}
	millis := micros / 1000
	if millis < 1000 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", millis), "0"), ".") + " ms"
	}
	secs := millis / 1000
	if secs < 60 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", secs), "0"), ".") + " s"
	}
	mins := secs / 60
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", mins), "0"), ".") + " min"
}

// formatBytes formats bytes into human-readable format with appropriate units
func formatBytes(bytes float64) string {
	if bytes == 0 {
		return "0 B"
	}
	if bytes < 1024 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", bytes), "0"), ".") + " B"
	}
	kb := bytes / 1024
	if kb < 1024 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", kb), "0"), ".") + " KB"
	}
	mb := kb / 1024
	if mb < 1024 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", mb), "0"), ".") + " MB"
	}
	gb := mb / 1024
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", gb), "0"), ".") + " GB"
}

// formatNumber formats large numbers with appropriate suffixes (K, M, B)
func formatNumber(num float64) string {
	if num == 0 {
		return "0"
	}
	if num < 1000 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", num), "0"), ".")
	}
	k := num / 1000
	if k < 1000 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", k), "0"), ".") + "K"
	}
	m := k / 1000
	if m < 1000 {
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", m), "0"), ".") + "M"
	}
	b := m / 1000
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", b), "0"), ".") + "B"
}

// formatPercent formats a decimal as a percentage
func formatPercent(val float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", val*100), "0"), ".") + "%"
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

// SQLActivityParams holds the form parameters for SQL activity page
type SQLActivityParams struct {
	Top           int
	By            string
	Interval      string // Selected interval (e.g., "1h", "Custom")
	Start         int64  // Unix timestamp in seconds
	End           int64  // Unix timestamp in seconds
	StartTime     string // Formatted time for display
	EndTime       string // Formatted time for display
	Timezone      string
	ResultCount   int
	SortByDisplay string
}

// SQLActivityData combines the API response with form parameters
type SQLActivityData struct {
	Statements    *serverpb.StatementsResponse
	Params        SQLActivityParams
	TotalWorkload float64 // Sum of (count * service_lat.mean) across all statements
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
	// Parse query parameters with defaults
	topStr := r.URL.Query().Get("top")
	byStr := r.URL.Query().Get("by")
	intervalStr := r.URL.Query().Get("interval")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	// Set defaults
	top := 100
	by := "% of All Runtime"
	interval := "1h" // default interval

	// Parse top parameter
	if topStr != "" {
		if val, err := fmt.Sscanf(topStr, "%d", &top); err == nil && val == 1 {
			// Successfully parsed
		} else {
			top = 100 // fallback to default
		}
	}

	// Parse by parameter
	if byStr != "" {
		by = byStr
	}

	// Parse interval parameter
	if intervalStr != "" {
		interval = intervalStr
	}

	// Map the "by" dropdown to the enum value
	sortOption := mapSortByToEnum(by)

	// Calculate time range - use query params if provided, otherwise default to last hour
	var startTime, endTime time.Time
	var startUnix, endUnix int64

	if startStr != "" && endStr != "" {
		// Parse Unix timestamps from query parameters (in seconds)
		if val, err := fmt.Sscanf(startStr, "%d", &startUnix); err == nil && val == 1 {
			startTime = time.Unix(startUnix, 0)
		}
		if val, err := fmt.Sscanf(endStr, "%d", &endUnix); err == nil && val == 1 {
			endTime = time.Unix(endUnix, 0)
		}
	}

	// If parsing failed or params not provided, use default (last hour)
	if startTime.IsZero() || endTime.IsZero() {
		now := timeutil.Now()
		startTime = now.Add(-1 * time.Hour)
		endTime = now
		startUnix = startTime.Unix()
		endUnix = endTime.Unix()
	}

	// Get timezone (default to America/New_York for now)
	timezone := "America/New_York"

	resp, err := cfg.Status.CombinedStatementStats(r.Context(), &serverpb.CombinedStatementsStatsRequest{
		Start: startUnix,
		End:   endUnix,
		FetchMode: &serverpb.CombinedStatementsStatsRequest_FetchMode{
			StatsType: serverpb.CombinedStatementsStatsRequest_StmtStatsOnly,
			Sort:      sortOption,
		},
		Limit: int64(top),
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to get statement stats: %v", err)
		http.Error(w, "Failed to get statement stats", http.StatusInternalServerError)
		return
	}

	// Format timestamps for display
	loc, _ := timeutil.LoadLocation(timezone)
	startTimeStr := startTime.In(loc).Format("2006-01-02 15:04:05")
	endTimeStr := endTime.In(loc).Format("2006-01-02 15:04:05")

	// Calculate total workload (sum of count * service_lat.mean across all statements)
	var totalWorkload float64
	for _, stmt := range resp.Statements {
		totalWorkload += float64(stmt.Stats.Count) * stmt.Stats.ServiceLat.Mean
	}

	// Build the data structure with params
	data := SQLActivityData{
		Statements:    resp,
		TotalWorkload: totalWorkload,
		Params: SQLActivityParams{
			Top:           top,
			By:            by,
			Interval:      interval,
			Start:         startUnix,
			End:           endUnix,
			StartTime:     startTimeStr,
			EndTime:       endTimeStr,
			Timezone:      timezone,
			ResultCount:   len(resp.Statements),
			SortByDisplay: by,
		},
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "sql_activity.html", PageData{
		IndexHTMLArgs: cfg,
		Data:          data,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// mapSortByToEnum maps the dropdown display text to the protobuf enum value
func mapSortByToEnum(sortBy string) serverpb.StatsSortOptions {
	switch sortBy {
	case "% of All Runtime":
		return serverpb.StatsSortOptions_PCT_RUNTIME
	case "Contention Time":
		return serverpb.StatsSortOptions_CONTENTION_TIME
	case "Execution Count":
		return serverpb.StatsSortOptions_EXECUTION_COUNT
	case "SQL CPU Time":
		return serverpb.StatsSortOptions_CPU_TIME
	case "Statement Time":
		return serverpb.StatsSortOptions_SERVICE_LAT
	case "Last Execution Time":
		return serverpb.StatsSortOptions_LAST_EXEC
	case "Max Latency":
		return serverpb.StatsSortOptions_LATENCY_INFO_MAX
	case "Max Memory":
		return serverpb.StatsSortOptions_MAX_MEMORY
	case "Min Latency":
		return serverpb.StatsSortOptions_LATENCY_INFO_MIN
	case "Network":
		return serverpb.StatsSortOptions_NETWORK
	case "Retries":
		return serverpb.StatsSortOptions_RETRIES
	case "Rows Processed":
		return serverpb.StatsSortOptions_ROWS_PROCESSED
	default:
		return serverpb.StatsSortOptions_PCT_RUNTIME
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
