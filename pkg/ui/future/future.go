// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
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
	Admin                     serverpb.AdminClient
	Status                    serverpb.StatusClient
	TS                        tspb.TimeSeriesClient
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
		"json": func(v interface{}) template.JS {
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return template.JS("{}")
			}
			return template.JS(jsonBytes)
		},
		"join": func(elements []string, separator string) string {
			return strings.Join(elements, separator)
		},
		"joinInt64": func(elements []int64, separator string) string {
			strs := make([]string, len(elements))
			for i, v := range elements {
				strs[i] = fmt.Sprintf("%d", v)
			}
			return strings.Join(strs, separator)
		},
		"formatTime":        formatTimeWrapper,
		"formatBytes":       formatBytesWrapper,
		"formatNumber":      formatNumberWrapper,
		"formatPercent":     formatPercentWrapper,
		"formatElapsedTime": formatElapsedTimeWrapper,
		"formatTimestamp":   formatTimestampWrapper,
		"timeNow":           func() time.Time { return timeutil.Now() },
		"timeSub": func(a, b time.Time) time.Duration {
			return a.Sub(b)
		},
		"dict": func(values ...interface{}) map[string]interface{} {
			dict := make(map[string]interface{})
			for i := 0; i < len(values); i += 2 {
				key := values[i].(string)
				dict[key] = values[i+1]
			}
			return dict
		},
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

func formatElapsedTimeWrapper(val interface{}) string {
	switch v := val.(type) {
	case time.Duration:
		return formatElapsedTime(v)
	default:
		return "unknown"
	}
}

func formatTimestampWrapper(val interface{}) string {
	// Convert nanoseconds to formatted timestamp
	nanos := toFloat64(val)
	t := time.Unix(0, int64(nanos))
	return t.UTC().Format("Jan 2, 2006 at 15:04 MST")
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

// CommonData contains data computed on each request that should be available to all pages
type CommonData struct {
	// License notification fields
	LicenseNotificationText    string
	LicenseNotificationTooltip string
	LicenseNotificationClass   string

	// Throttle warning fields
	ThrottleWarningText    string
	ThrottleWarningTooltip string
	ThrottleWarningClass   string
}

// PageData combines base IndexHTMLArgs with page-specific data
type PageData struct {
	IndexHTMLArgs
	Common CommonData
	Data   interface{}
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

// DiagnosticBundle represents a completed diagnostic bundle
type DiagnosticBundle struct {
	ID          int64
	CollectedAt time.Time
	DownloadURL string
}

// DiagnosticState represents the diagnostic state for a statement fingerprint
type DiagnosticState struct {
	// HasPendingRequest indicates if there's an active (not completed) diagnostic request
	HasPendingRequest bool
	// PendingRequestID is the ID of the pending request (for cancellation)
	PendingRequestID int64
	// CompletedBundles is the list of completed diagnostic bundles
	CompletedBundles []DiagnosticBundle
	// ID is the statement fingerprint ID of the fingerprint corresponding to this set of diagnostics.
	ID appstatspb.StmtFingerprintID
	// Query is the string fingerprint corresponding to this set of diagnostics.
	Query string
	// PlanGists is the list of plan gists for this statement fingerprint
	PlanGists []string
	TableMode bool
}

// SQLActivityData combines the API response with form parameters
type SQLActivityData struct {
	Statements    *serverpb.StatementsResponse
	Params        SQLActivityParams
	TotalWorkload float64 // Sum of (count * service_lat.mean) across all statements
	// DiagnosticStates maps statement fingerprint ID to its diagnostic state
	DiagnosticStates map[string]*DiagnosticState
}

// StatementFingerprintPageData contains data for a single statement fingerprint page
type StatementFingerprintPageData struct {
	Statement   serverpb.StatementDetailsResponse_CollectedStatementSummary
	StmtID      string // Statement fingerprint ID (may be string hash or uint64)
	Params      SQLActivityParams
	Diagnostics DiagnosticState
	PlanStats   []serverpb.StatementDetailsResponse_CollectedStatementGroupedByPlanHash
}

// OverviewData contains cluster overview information
type OverviewData struct {
	// Capacity information
	CapacityUsed      int64   // Total bytes used across all stores
	CapacityTotal     int64   // Total capacity across all stores
	CapacityPercent   float64 // Percentage of capacity used
	CapacityUsedStr   string  // Formatted capacity used (e.g., "1.7 TiB")
	CapacityTotalStr  string  // Formatted total capacity (e.g., "8.6 TiB")
	CapacityAvailable int64   // Total available capacity

	// Node status counts
	LiveNodes     int
	SuspectNodes  int
	DrainingNodes int
	DeadNodes     int

	// Range information
	TotalRanges           int
	UnderReplicatedRanges int
	UnavailableRanges     int

	// Node groups by region
	RegionGroups []RegionGroup
	TotalNodes   int
}

// RegionGroup represents nodes grouped by region locality
type RegionGroup struct {
	RegionName string
	NodeCount  int
	Nodes      []NodeInfo

	// Aggregated metrics for the region
	TotalReplicas   int
	CapacityPercent float64
	MemoryPercent   float64
	TotalVCPUs      int
	Status          string // "Live", "Degraded", etc.
	UptimeSeconds   int64  // Minimum uptime in the region
}

// NodeInfo contains information about a single node
type NodeInfo struct {
	NodeID          int32
	Address         string
	Region          string
	UptimeSeconds   int64
	UptimeFormatted string
	Replicas        int
	CapacityPercent float64
	MemoryPercent   float64
	VCPUs           int
	Version         string
	Status          string
	StatusClass     string // CSS class: "info", "warn", "bad"
}

// handlerWithCommonData wraps a handler to automatically compute and inject common data
func handlerWithCommonData(
	cfg IndexHTMLArgs,
	handler func(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData),
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		commonData := computeCommonData(cfg)
		handler(w, r, cfg, commonData)
	}
}

// computeCommonData generates all common data needed across pages
func computeCommonData(cfg IndexHTMLArgs) CommonData {
	data := CommonData{}

	// Compute license notification
	data.LicenseNotificationText, data.LicenseNotificationTooltip, data.LicenseNotificationClass =
		generateLicenseNotification(cfg.LicenseType, cfg.SecondsUntilLicenseExpiry, cfg.IsManaged)

	// TODO: Compute throttle warning when logic is implemented
	// For now, throttle warning fields remain empty

	return data
}

func MakeFutureHandler(cfg IndexHTMLArgs) http.HandlerFunc {
	// Create a new Gorilla Mux router
	router := mux.NewRouter()

	// Redirect root to /future/overview
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/future/overview", http.StatusFound)
	}).Methods("GET")

	// Prefix all routes with /future
	futureRouter := router.PathPrefix("/future").Subrouter()

	// Redirect /future to overview
	futureRouter.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/future/overview", http.StatusFound)
	}).Methods("GET")

	// Serve the overview page
	futureRouter.HandleFunc("/overview", handlerWithCommonData(cfg, handleOverview)).Methods("GET")

	// Serve the dual-cluster magic overview page
	futureRouter.HandleFunc("/overview/magic", handlerWithCommonData(cfg, handleOverviewMagic)).Methods("GET")

	// Serve the summary table partials for magic overview (cluster 1 or 2)
	futureRouter.HandleFunc("/overview/magic/summary", func(w http.ResponseWriter, r *http.Request) {
		handleOverviewMagicSummary(w, r, cfg)
	}).Methods("GET")

	// Serve the login page
	futureRouter.HandleFunc("/login", handleLogin).Methods("GET")

	// Serve specific dashboard
	futureRouter.HandleFunc("/metrics", handlerWithCommonData(cfg, handleMetricsDashboard)).Methods("GET")

	futureRouter.HandleFunc("/databases", handlerWithCommonData(cfg, handleDatabases)).Methods("GET")

	// Database metadata refresh endpoints
	futureRouter.HandleFunc("/databases/refresh", func(w http.ResponseWriter, r *http.Request) {
		handleDatabaseMetadataRefresh(w, r, cfg)
	}).Methods("POST")

	futureRouter.HandleFunc("/databases/{id}", handlerWithCommonData(cfg, handleDatabase)).Methods("GET")

	futureRouter.HandleFunc("/databases/{id}/refresh", func(w http.ResponseWriter, r *http.Request) {
		handleDatabaseMetadataRefresh(w, r, cfg)
	}).Methods("POST")

	futureRouter.HandleFunc("/tables/{id}", handlerWithCommonData(cfg, handleTable)).Methods("GET")

	futureRouter.HandleFunc("/tables/{id}/reset-index-stats", func(w http.ResponseWriter, r *http.Request) {
		handleResetIndexStats(w, r, cfg)
	}).Methods("POST")

	futureRouter.HandleFunc("/sqlactivity", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/future/sqlactivity/statements", http.StatusFound)
	}).Methods("GET")

	futureRouter.HandleFunc("/sqlactivity/statements", handlerWithCommonData(cfg, handleSqlActivityStatements)).Methods("GET")

	futureRouter.HandleFunc("/sqlactivity/statements/{stmtID}", handlerWithCommonData(cfg, handleSqlActivityStatementFingerprint)).Methods("GET")

	futureRouter.HandleFunc("/sqlactivity/statements/{stmtID}/diagnostics", handlerWithCommonData(cfg, handleGetDiagnosticsControls)).Methods("GET")

	futureRouter.HandleFunc("/sqlactivity/statements/{stmtID}/diagnostics", handlerWithCommonData(cfg, handleCreateDiagnostics)).Methods("POST")

	futureRouter.HandleFunc("/sqlactivity/statements/{stmtID}/diagnostics/{diagID}", handlerWithCommonData(cfg, handleCancelDiagnostics)).Methods("DELETE")

	// Redirect diagnostics download to the remote HTTP server
	futureRouter.HandleFunc("/sqlactivity/stmtdiagnostics/{diagID}/download", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		diagID := vars["diagID"]
		http.Redirect(w, r, fmt.Sprintf("%s/_admin/v1/stmtbundle/%s", apiBaseURL, diagID), http.StatusFound)
	}).Methods("GET")

	futureRouter.HandleFunc("/nodes/{nodeID}", handlerWithCommonData(cfg, handleNode))

	futureRouter.HandleFunc("/nodes/{nodeID}/logs", handlerWithCommonData(cfg, handleNodeLogs))

	// Timeseries query endpoint
	futureRouter.HandleFunc("/ts/query", func(w http.ResponseWriter, r *http.Request) {
		handleTSQuery(w, r, cfg)
	}).Methods("POST")

	// Metrics summary and events partials
	futureRouter.HandleFunc("/metrics/summary", handlerWithCommonData(cfg, handleMetricsSummary)).Methods("GET")
	futureRouter.HandleFunc("/metrics/events", handlerWithCommonData(cfg, handleMetricsEvents)).Methods("GET")

	// Serve static assets
	futureRouter.PathPrefix("/assets/").HandlerFunc(handleAssets)

	return router.ServeHTTP
}

// NodePageData contains data for the node details page
type NodePageData struct {
	NodeID       int32
	Address      string
	Health       string
	LastUpdate   time.Time
	BuildVersion string
	Stores       []StoreInfo
	NodeTotals   StoreTotals
}

// StoreInfo contains metrics for a single store
type StoreInfo struct {
	StoreID int32
	Totals  StoreTotals
}

// StoreTotals contains aggregated metrics
type StoreTotals struct {
	LiveBytes          int64
	KeyBytes           int64
	ValueBytes         int64
	RangeKeyBytes      int64
	RangeValueBytes    int64
	IntentBytes        int64
	SystemBytes        int64
	GCBytesAge         int64
	TotalReplicas      int64
	RaftLeaders        int64
	TotalRanges        int64
	UnavailablePercent float64
	UnderRepPercent    float64
	UsedCapacity       int64
	AvailableCapacity  int64
	MaximumCapacity    int64
}

// NodeLogsPageData contains data for the node logs page
type NodeLogsPageData struct {
	NodeID  int32
	Address string
	Entries []LogEntry
}

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp     int64  // Nanoseconds since epoch
	FormattedTime string // Human-readable timestamp
	Severity      int32
	SeverityName  string
	Message       string
	File          string
	Line          int64
	Tags          string
}

func handleNode(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData) {
	ctx := r.Context()

	// Extract node ID from URL path
	vars := mux.Vars(r)
	nodeIDStr := vars["nodeID"]
	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 32)
	if err != nil {
		http.Error(w, "Invalid node ID", http.StatusBadRequest)
		return
	}

	// Fetch all nodes to find the requested node
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Find the specific node
	var targetNode *serverpb.NodeResponse
	for i := range nodesResp.Nodes {
		if int32(nodesResp.Nodes[i].Desc.NodeID) == int32(nodeID) {
			targetNode = &nodesResp.Nodes[i]
			break
		}
	}

	if targetNode == nil {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	// Build page data
	pageData := NodePageData{
		NodeID:       int32(targetNode.Desc.NodeID),
		Address:      targetNode.Desc.Address.AddressField,
		Health:       "Healthy", // Simplified - could check liveness
		LastUpdate:   time.Unix(0, targetNode.UpdatedAt),
		BuildVersion: targetNode.BuildInfo.Tag,
		Stores:       make([]StoreInfo, 0, len(targetNode.StoreStatuses)),
	}

	// Initialize node totals
	var nodeTotals StoreTotals

	// Process each store
	for _, store := range targetNode.StoreStatuses {
		// Get MVCC stats from metrics
		storeStats := StoreTotals{
			LiveBytes:          int64(store.Metrics["livebytes"]),
			KeyBytes:           int64(store.Metrics["keybytes"]),
			ValueBytes:         int64(store.Metrics["valbytes"]),
			RangeKeyBytes:      int64(store.Metrics["rangekeybytes"]),
			RangeValueBytes:    int64(store.Metrics["rangevalbytes"]),
			IntentBytes:        int64(store.Metrics["intentbytes"]),
			SystemBytes:        int64(store.Metrics["sysbytes"]),
			GCBytesAge:         int64(store.Metrics["gcbytesage"]),
			TotalReplicas:      int64(store.Desc.Capacity.RangeCount),
			RaftLeaders:        int64(store.Desc.Capacity.LeaseCount),
			TotalRanges:        int64(store.Metrics["ranges"]),
			UnavailablePercent: 0, // TODO: Calculate from problem ranges
			UnderRepPercent:    0, // TODO: Calculate from problem ranges
			UsedCapacity:       store.Desc.Capacity.Used,
			AvailableCapacity:  store.Desc.Capacity.Available,
			MaximumCapacity:    store.Desc.Capacity.Capacity,
		}

		pageData.Stores = append(pageData.Stores, StoreInfo{
			StoreID: int32(store.Desc.StoreID),
			Totals:  storeStats,
		})

		// Aggregate to node totals
		nodeTotals.LiveBytes += storeStats.LiveBytes
		nodeTotals.KeyBytes += storeStats.KeyBytes
		nodeTotals.ValueBytes += storeStats.ValueBytes
		nodeTotals.RangeKeyBytes += storeStats.RangeKeyBytes
		nodeTotals.RangeValueBytes += storeStats.RangeValueBytes
		nodeTotals.IntentBytes += storeStats.IntentBytes
		nodeTotals.SystemBytes += storeStats.SystemBytes
		nodeTotals.GCBytesAge += storeStats.GCBytesAge
		nodeTotals.TotalReplicas += storeStats.TotalReplicas
		nodeTotals.RaftLeaders += storeStats.RaftLeaders
		nodeTotals.TotalRanges += storeStats.TotalRanges
		nodeTotals.UsedCapacity += storeStats.UsedCapacity
		nodeTotals.AvailableCapacity += storeStats.AvailableCapacity
		nodeTotals.MaximumCapacity += storeStats.MaximumCapacity
	}

	pageData.NodeTotals = nodeTotals

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "node.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          pageData,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

func handleNodeLogs(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()

	// Extract node ID from URL path
	vars := mux.Vars(r)
	nodeIDStr := vars["nodeID"]
	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 32)
	if err != nil {
		http.Error(w, "Invalid node ID", http.StatusBadRequest)
		return
	}

	// Fetch all nodes to get the address for the requested node
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Find the specific node to get its address
	var targetNode *serverpb.NodeResponse
	for i := range nodesResp.Nodes {
		if int32(nodesResp.Nodes[i].Desc.NodeID) == int32(nodeID) {
			targetNode = &nodesResp.Nodes[i]
			break
		}
	}

	if targetNode == nil {
		http.Error(w, "Node not found", http.StatusNotFound)
		return
	}

	// Fetch logs using the Logs RPC
	logsResp, err := cfg.Status.Logs(ctx, &serverpb.LogsRequest{
		NodeId: fmt.Sprintf("%d", nodeID),
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch logs: %v", err)
		http.Error(w, "Failed to fetch logs", http.StatusInternalServerError)
		return
	}

	// Convert log entries to our format
	var entries []LogEntry
	for _, entry := range logsResp.Entries {
		// Convert timestamp to time.Time (nanoseconds to time)
		t := time.Unix(0, entry.Time)
		formattedTime := t.Format("2006-01-02 15:04:05.000000")

		// Map severity to name
		//
		severityName := logpb.Severity_name[int32(entry.Severity)]

		entries = append(entries, LogEntry{
			Timestamp:     entry.Time,
			FormattedTime: formattedTime,
			Severity:      int32(entry.Severity),
			SeverityName:  severityName,
			Message:       entry.Message,
			File:          entry.File,
			Line:          entry.Line,
			Tags:          entry.Tags,
		})
	}

	// Build page data
	pageData := NodeLogsPageData{
		NodeID:  int32(nodeID),
		Address: targetNode.Desc.Address.AddressField,
		Entries: entries,
	}

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "node_logs.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          pageData,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// getSeverityName maps severity integer to human-readable name
func getSeverityName(severity int32) string {
	switch severity {
	case 0:
		return "UNKNOWN"
	case 1:
		return "INFO"
	case 2:
		return "WARNING"
	case 3:
		return "ERROR"
	case 4:
		return "FATAL"
	default:
		return fmt.Sprintf("SEVERITY_%d", severity)
	}
}

func handleDatabases(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Parse query parameters for filtering
	searchFilter := r.URL.Query().Get("search")
	nodeIDStr := r.URL.Query().Get("nodeId")
	var selectedNode int32
	if nodeIDStr != "" {
		if parsed, err := strconv.ParseInt(nodeIDStr, 10, 32); err == nil {
			selectedNode = int32(parsed)
		}
	}

	// Fetch nodes to map stores to regions
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Build a map of storeID -> (nodeID, region) and collect nodes by region
	storeToNodeRegion := make(map[int64]struct {
		nodeID int32
		region string
	})
	nodesByRegion := make(map[string][]NodeDisplayInfo)
	nodeToStores := make(map[int32][]int64) // Track which stores belong to each node

	for _, node := range nodesResp.Nodes {
		region := extractRegion(&node.Desc.Locality)
		nodeID := int32(node.Desc.NodeID)

		// Add node to region group
		nodesByRegion[region] = append(nodesByRegion[region], NodeDisplayInfo{
			NodeID:  fmt.Sprintf("%d", nodeID),
			Address: node.Desc.Address.AddressField,
		})

		// Map stores to nodes
		for _, store := range node.StoreStatuses {
			storeID := int64(store.Desc.StoreID)
			storeToNodeRegion[storeID] = struct {
				nodeID int32
				region string
			}{
				nodeID: nodeID,
				region: region,
			}
			nodeToStores[nodeID] = append(nodeToStores[nodeID], storeID)
		}
	}

	// Sort nodes within each region
	for region := range nodesByRegion {
		sort.Slice(nodesByRegion[region], func(i, j int) bool {
			return nodesByRegion[region][i].NodeID < nodesByRegion[region][j].NodeID
		})
	}

	// Build API URL with filters
	apiURL := fmt.Sprintf("%s/api/v2/database_metadata/", apiBaseURL)
	params := make([]string, 0, 2)
	if searchFilter != "" {
		params = append(params, fmt.Sprintf("name=%s", searchFilter))
	}
	// If filtering by node, add all stores from that node
	if selectedNode > 0 {
		if storeIDs, ok := nodeToStores[selectedNode]; ok {
			for _, storeID := range storeIDs {
				params = append(params, fmt.Sprintf("storeId=%d", storeID))
			}
		}
	}
	if len(params) > 0 {
		apiURL += "?" + strings.Join(params, "&")
	}

	var apiResp apiPaginatedResponse[[]apiDbMetadata]
	if err := fetchJSON(ctx, apiURL, &apiResp); err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch databases: %v", err)
		http.Error(w, "Failed to fetch databases", http.StatusInternalServerError)
		return
	}

	// Enrich databases with region/node information
	var enrichedDatabases []DatabaseWithRegions
	for _, db := range apiResp.Results {
		dbWithRegions := DatabaseWithRegions{
			apiDbMetadata: db,
			RegionNodes:   make(map[string][]int32),
		}

		// Track unique nodes per region
		nodesSeen := make(map[int32]bool)
		for _, storeID := range db.StoreIDs {
			if info, ok := storeToNodeRegion[storeID]; ok {
				// Only add each node once per region
				if !nodesSeen[info.nodeID] {
					dbWithRegions.RegionNodes[info.region] = append(dbWithRegions.RegionNodes[info.region], info.nodeID)
					nodesSeen[info.nodeID] = true
				}
			}
		}

		enrichedDatabases = append(enrichedDatabases, dbWithRegions)
	}

	// Fetch table metadata job status
	var jobStatus apiTableMetadataJobStatus
	jobStatusURL := fmt.Sprintf("%s/api/v2/table_metadata/updatejob/", apiBaseURL)
	jobInfo := &MetadataJobInfo{}
	if err := fetchJSON(ctx, jobStatusURL, &jobStatus); err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch job status: %v", err)
		// Continue without job info
	} else {
		jobInfo.LastCompletedAt = jobStatus.LastCompletedTime
		jobInfo.IsRunning = jobStatus.CurrentStatus == "RUNNING"
	}

	// Determine last refreshed time from the databases
	var lastRefreshed *time.Time
	for _, db := range enrichedDatabases {
		if db.LastUpdated != nil {
			if lastRefreshed == nil || db.LastUpdated.After(*lastRefreshed) {
				lastRefreshed = db.LastUpdated
			}
		}
	}

	pageData := DatabasesPageData{
		Databases:       enrichedDatabases,
		SearchFilter:    searchFilter,
		SelectedNode:    selectedNode,
		NodesByRegion:   nodesByRegion,
		LastRefreshed:   lastRefreshed,
		MetadataJobInfo: jobInfo,
	}

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "databases.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          pageData,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

func handleDatabaseMetadataRefresh(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Trigger the table metadata job via the API
	triggerURL := fmt.Sprintf("%s/api/v2/table_metadata/updatejob/", apiBaseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", triggerURL, nil)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to create request: %v", err)
		http.Error(w, "Failed to trigger refresh", http.StatusInternalServerError)
		return
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to trigger refresh: %v", err)
		http.Error(w, "Failed to trigger refresh", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Dev.Warningf(ctx, "Refresh trigger returned status %d", resp.StatusCode)
		http.Error(w, "Failed to trigger refresh", http.StatusInternalServerError)
		return
	}

	// Extract database ID from URL path
	vars := mux.Vars(r)
	dbIDStr, ok := vars["id"]

	if !ok {
		http.Redirect(w, r, "/future/databases", http.StatusFound)
	} else {
		http.Redirect(w, r, fmt.Sprintf("/future/databases/%s", dbIDStr), http.StatusFound)
	}
}

// formatElapsedTime formats a duration into a human-readable string
func formatElapsedTime(d time.Duration) string {
	if d < time.Minute {
		return "a few seconds ago"
	}
	if d < 2*time.Minute {
		return "1 minute ago"
	}
	if d < time.Hour {
		return fmt.Sprintf("%d minutes ago", int(d.Minutes()))
	}
	if d < 2*time.Hour {
		return "1 hour ago"
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%d hours ago", int(d.Hours()))
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day ago"
	}
	return fmt.Sprintf("%d days ago", days)
}

func handleDatabase(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Extract database ID from URL path
	vars := mux.Vars(r)
	dbIDStr := vars["id"]
	dbID, err := strconv.ParseInt(dbIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid database ID", http.StatusBadRequest)
		return
	}

	// Parse query parameters for filtering
	searchFilter := r.URL.Query().Get("search")
	nodeIDStr := r.URL.Query().Get("nodeId")
	var selectedNode int32
	if nodeIDStr != "" {
		if parsed, err := strconv.ParseInt(nodeIDStr, 10, 32); err == nil {
			selectedNode = int32(parsed)
		}
	}

	// Fetch database details
	var dbDetailsResp apiDbMetadataWithDetails
	dbURL := fmt.Sprintf("%s/api/v2/database_metadata/%d/", apiBaseURL, dbID)
	if err := fetchJSON(ctx, dbURL, &dbDetailsResp); err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch database details: %v", err)
		http.Error(w, "Failed to fetch database details", http.StatusInternalServerError)
		return
	}

	// Fetch nodes to map stores to regions
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Build a map of storeID -> (nodeID, region) and collect nodes by region
	storeToNodeRegion := make(map[int64]struct {
		nodeID int32
		region string
	})
	nodesByRegion := make(map[string][]NodeDisplayInfo)
	nodeToStores := make(map[int32][]int64) // Track which stores belong to each node

	for _, node := range nodesResp.Nodes {
		region := extractRegion(&node.Desc.Locality)
		nodeID := int32(node.Desc.NodeID)

		// Add node to region group
		nodesByRegion[region] = append(nodesByRegion[region], NodeDisplayInfo{
			NodeID:  fmt.Sprintf("%d", nodeID),
			Address: node.Desc.Address.AddressField,
		})

		// Map stores to nodes
		for _, store := range node.StoreStatuses {
			storeID := int64(store.Desc.StoreID)
			storeToNodeRegion[storeID] = struct {
				nodeID int32
				region string
			}{
				nodeID: nodeID,
				region: region,
			}
			nodeToStores[nodeID] = append(nodeToStores[nodeID], storeID)
		}
	}

	// Sort nodes within each region
	for region := range nodesByRegion {
		sort.Slice(nodesByRegion[region], func(i, j int) bool {
			return nodesByRegion[region][i].NodeID < nodesByRegion[region][j].NodeID
		})
	}

	// Build API URL with filters for tables
	tablesURL := fmt.Sprintf("%s/api/v2/table_metadata/?dbId=%d", apiBaseURL, dbID)
	params := make([]string, 0, 2)
	if searchFilter != "" {
		params = append(params, fmt.Sprintf("name=%s", searchFilter))
	}
	// If filtering by node, add all stores from that node
	if selectedNode > 0 {
		if storeIDs, ok := nodeToStores[selectedNode]; ok {
			for _, storeID := range storeIDs {
				params = append(params, fmt.Sprintf("storeId=%d", storeID))
			}
		}
	}
	if len(params) > 0 {
		tablesURL += "&" + strings.Join(params, "&")
	}

	// Fetch tables for this database
	var tablesResp apiPaginatedResponse[[]apiTableMetadata]
	if err := fetchJSON(ctx, tablesURL, &tablesResp); err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch tables: %v", err)
		http.Error(w, "Failed to fetch tables", http.StatusInternalServerError)
		return
	}

	// Fetch database grants
	grants, err := fetchDatabaseGrants(ctx, cfg, dbDetailsResp.Metadata.DbName)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch database grants: %v", err)
		// Continue without grants - not critical
		grants = []DatabaseGrant{}
	}
	// Fetch table metadata job status
	var jobStatus apiTableMetadataJobStatus
	jobStatusURL := fmt.Sprintf("%s/api/v2/table_metadata/updatejob/", apiBaseURL)
	jobInfo := &MetadataJobInfo{}
	if err := fetchJSON(ctx, jobStatusURL, &jobStatus); err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch job status: %v", err)
		// Continue without job info
	} else {
		jobInfo.LastCompletedAt = jobStatus.LastCompletedTime
		jobInfo.IsRunning = jobStatus.CurrentStatus == "RUNNING"
	}

	pageData := DatabasePageData{
		DatabaseID:      dbDetailsResp.Metadata.DbID,
		DatabaseName:    dbDetailsResp.Metadata.DbName,
		Tables:          tablesResp.Results,
		SearchFilter:    searchFilter,
		SelectedNode:    selectedNode,
		NodesByRegion:   nodesByRegion,
		Grants:          grants,
		MetadataJobInfo: jobInfo,
	}

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "database.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          pageData,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

func handleTable(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Extract table ID from URL path
	vars := mux.Vars(r)
	tableIDStr := vars["id"]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid table ID", http.StatusBadRequest)
		return
	}

	// Fetch table details including create statement
	var tableDetailsResp apiTableMetadataWithDetails
	tableURL := fmt.Sprintf("%s/api/v2/table_metadata/%d/", apiBaseURL, tableID)
	if err := fetchJSON(ctx, tableURL, &tableDetailsResp); err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch table details: %v", err)
		http.Error(w, "Failed to fetch table details", http.StatusInternalServerError)
		return
	}

	// Fetch nodes to map stores to regions
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Build a map of storeID -> (nodeID, region)
	storeToNodeRegion := make(map[int64]struct {
		nodeID int32
		region string
	})
	for _, node := range nodesResp.Nodes {
		region := extractRegion(&node.Desc.Locality)
		nodeID := int32(node.Desc.NodeID)
		for _, store := range node.StoreStatuses {
			storeID := int64(store.Desc.StoreID)
			storeToNodeRegion[storeID] = struct {
				nodeID int32
				region string
			}{
				nodeID: nodeID,
				region: region,
			}
		}
	}

	// Calculate region/nodes for this table
	regionNodes := make(map[string][]int32)
	nodesSeen := make(map[int32]bool)
	for _, storeID := range tableDetailsResp.Metadata.StoreIDs {
		if info, ok := storeToNodeRegion[storeID]; ok {
			if !nodesSeen[info.nodeID] {
				regionNodes[info.region] = append(regionNodes[info.region], info.nodeID)
				nodesSeen[info.nodeID] = true
			}
		}
	}

	// Fetch table grants
	grants, err := fetchTableGrants(ctx, cfg, tableDetailsResp.Metadata.DbName, tableDetailsResp.Metadata.SchemaName, tableDetailsResp.Metadata.TableName)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch table grants: %v", err)
		// Continue without grants - not critical
		grants = []TableGrant{}
	}

	// Fetch index statistics
	indexes, indexStatsReset, err := fetchIndexStats(ctx, cfg, tableDetailsResp.Metadata.DbName, tableDetailsResp.Metadata.TableName)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch index stats: %v", err)
		// Continue without index stats - not critical
		indexes = []IndexInfo{}
	}

	pageData := TablePageData{
		Table:           tableDetailsResp.Metadata,
		CreateStatement: tableDetailsResp.CreateStatement,
		DatabaseName:    tableDetailsResp.Metadata.DbName,
		RegionNodes:     regionNodes,
		Grants:          grants,
		Indexes:         indexes,
		LastUpdated:     &tableDetailsResp.Metadata.LastUpdated,
		IndexStatsReset: indexStatsReset,
	}

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "table.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          pageData,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

func handleResetIndexStats(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	ctx := r.Context()

	// Extract table ID from URL path
	vars := mux.Vars(r)
	tableIDStr := vars["id"]

	// Call the ResetIndexUsageStats RPC to reset all index stats across the cluster
	_, err := cfg.Status.ResetIndexUsageStats(ctx, &serverpb.ResetIndexUsageStatsRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to reset index stats: %v", err)
		http.Error(w, "Failed to reset index stats", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/future/tables/%s", tableIDStr), http.StatusFound)
}

func handleGetDiagnosticsControls(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()
	vars := mux.Vars(r)
	stmtID, ok := vars["stmtID"]
	if !ok || stmtID == "" {
		http.Error(w, "Missing statement ID", http.StatusBadRequest)
		return
	}

	// Get fingerprint from query param
	fingerprint := r.URL.Query().Get("fingerprint")
	if fingerprint == "" {
		http.Error(w, "Missing fingerprint query parameter", http.StatusBadRequest)
		return
	}

	tableMode := r.URL.Query().Get("tableMode") == "true"

	stmtIDParsed, err := strconv.ParseUint(stmtID, 10, 64)
	if err != nil {
		http.Error(w, "Invalid diagnostic ID", http.StatusBadRequest)
		return
	}

	// Fetch statement details to get plan gists
	// Use a reasonable time range - last 24 hours
	now := timeutil.Now()
	startTime := now.Add(-24 * time.Hour)
	resp, err := cfg.Status.StatementDetails(ctx, &serverpb.StatementDetailsRequest{
		FingerprintId: stmtID,
		Start:         startTime.Unix(),
		End:           now.Unix(),
	})

	// Collect unique plan gists from plan-grouped stats
	var planGists []string
	if err == nil && resp != nil {
		planGistSet := make(map[string]bool)
		for _, planStat := range resp.StatementStatisticsPerPlanHash {
			// Extract plan gists from the Stats.PlanGists array
			for _, gist := range planStat.Stats.PlanGists {
				if gist != "" {
					planGistSet[gist] = true
				}
			}
		}
		for gist := range planGistSet {
			planGists = append(planGists, gist)
		}
		// Sort for consistent ordering
		sort.Strings(planGists)
	}

	// Fetch diagnostic reports to determine state for this fingerprint
	diagResp, err := cfg.Status.StatementDiagnosticsRequests(ctx, &serverpb.StatementDiagnosticsReportsRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch diagnostic reports: %v", err)
		http.Error(w, "Failed to fetch diagnostic reports", http.StatusInternalServerError)
		return
	}

	// Build diagnostic state for this fingerprint
	diagState := DiagnosticState{
		Query:     fingerprint,
		ID:        appstatspb.StmtFingerprintID(stmtIDParsed),
		PlanGists: planGists,
		TableMode: tableMode,
	}

	for _, report := range diagResp.Reports {
		if report.StatementFingerprint == fingerprint {
			if !report.Completed {
				diagState.HasPendingRequest = true
				diagState.PendingRequestID = report.Id
			} else if report.StatementDiagnosticsId > 0 {
				bundle := DiagnosticBundle{
					ID:          report.StatementDiagnosticsId,
					CollectedAt: report.RequestedAt,
					DownloadURL: fmt.Sprintf("/future/sqlactivity/stmtdiagnostics/%d/download", report.StatementDiagnosticsId),
				}
				diagState.CompletedBundles = append(diagState.CompletedBundles, bundle)
			}
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "statement_diagnostics_controls.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          diagState,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

func handleCancelDiagnostics(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	vars := mux.Vars(r)
	stmtID, ok := vars["stmtID"]
	if !ok || stmtID == "" {
		http.Error(w, "Missing statement ID", http.StatusBadRequest)
		return
	}

	diagID, ok := vars["diagID"]
	if !ok || diagID == "" {
		http.Error(w, "Missing diagnostic ID", http.StatusBadRequest)
		return
	}

	requestID, err := strconv.ParseInt(diagID, 10, 64)
	if err != nil {
		http.Error(w, "Invalid diagnostic ID", http.StatusBadRequest)
		return
	}

	// Call the API to cancel the diagnostics request
	cancelResp, err := cfg.Status.CancelStatementDiagnosticsReport(r.Context(), &serverpb.CancelStatementDiagnosticsReportRequest{
		RequestID: requestID,
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err != nil || !cancelResp.Canceled {
		errorMsg := "Failed to cancel request"
		if err != nil {
			errorMsg = err.Error()
		} else if cancelResp.Error != "" {
			errorMsg = cancelResp.Error
		}
		log.Dev.Warningf(r.Context(), "Failed to cancel diagnostics request: %v", errorMsg)
		// Return error message
		errorHTML := fmt.Sprintf(`<div class="bad box">Error: %s</div>`, errorMsg)
		_, _ = w.Write([]byte(errorHTML))
		return
	}

	// Redirect with fingerprint query param if provided
	redirectURL := fmt.Sprintf("/future/sqlactivity/statements/%s/diagnostics?%s", stmtID, r.URL.Query().Encode())
	http.Redirect(w, r, redirectURL, http.StatusSeeOther)
}

func handleCreateDiagnostics(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	vars := mux.Vars(r)
	stmtID, ok := vars["stmtID"]
	if !ok || stmtID == "" {
		http.Error(w, "Missing statement ID", http.StatusBadRequest)
		return
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	// Extract form parameters
	sampled := r.FormValue("sampled")
	sampleRateStr := r.FormValue("samplerate")
	latThresholdStr := r.FormValue("latthreshold")
	latUnit := r.FormValue("latunit")
	planGist := r.FormValue("plangist")
	planGistSelect := r.FormValue("plangistselect")
	expiresAfter := r.FormValue("expiresafter")
	expiresAfterMinStr := r.FormValue("expiresaftermin")
	redact := r.FormValue("redact")
	fingerprint := r.FormValue("fingerprint")
	tableMode := r.FormValue("tableMode")

	if fingerprint == "" {
		http.Error(w, "Missing fingerprint_id", http.StatusBadRequest)
		return
	}

	// Build the request
	req := &serverpb.CreateStatementDiagnosticsReportRequest{
		StatementFingerprint: fingerprint,
	}

	// Parse sampling probability and latency based on collection mode
	if sampled == "sampled" {
		// Parse sampling rate (e.g., "1%" -> 0.01)
		if sampleRateStr != "" {
			rateStr := strings.TrimSuffix(sampleRateStr, "%")
			if rate, err := strconv.ParseFloat(rateStr, 64); err == nil {
				req.SamplingProbability = rate / 100.0
			}
		}

		// Parse minimum execution latency
		if latThresholdStr != "" {
			if threshold, err := strconv.ParseFloat(latThresholdStr, 64); err == nil {
				var duration time.Duration
				if latUnit == "seconds" {
					duration = time.Duration(threshold * float64(time.Second))
				} else { // milliseconds (default)
					duration = time.Duration(threshold * float64(time.Millisecond))
				}
				req.MinExecutionLatency = duration
			}
		}
	}
	// For "next execution" mode, leave both sampling_probability and min_execution_latency at zero/unset

	// Parse plan gist
	if planGist == "selected" && planGistSelect != "" {
		req.PlanGist = planGistSelect
	}

	// Parse expiration
	if expiresAfter == "on" && expiresAfterMinStr != "" {
		if mins, err := strconv.ParseInt(expiresAfterMinStr, 10, 64); err == nil {
			req.ExpiresAfter = time.Duration(mins) * time.Minute
		}
	}

	// Parse redact checkbox
	req.Redacted = redact == "on"

	// Call the API to create the diagnostics request
	_, err := cfg.Status.CreateStatementDiagnosticsReport(r.Context(), req)

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to create diagnostics request: %v", err)
		// Return only error HTML with OOB swap (no button update, so popover stays open)
		errorHTML := fmt.Sprintf(`<div id="diagnostics-error" hx-swap-oob="true" class="bad box">
  <strong class="titlebar">Error</strong>
  <p>%s</p>
</div>`, err.Error())
		_, _ = w.Write([]byte(errorHTML))
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/future/sqlactivity/statements/%s/diagnostics?fingerprint=%s&tableMode=%s", stmtID, fingerprint, tableMode), http.StatusFound)
}

func handleSqlActivityStatementFingerprint(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Extract statement ID from URL path
	vars := mux.Vars(r)
	stmtID := vars["stmtID"]
	if stmtID == "" {
		http.Error(w, "Missing statement ID", http.StatusBadRequest)
		return
	}

	// Parse statement ID to uint64 for display purposes
	stmtIDParsed, err := strconv.ParseUint(stmtID, 10, 64)
	if err != nil {
		// If it can't be parsed as uint64, it might already be a fingerprint ID string
		stmtIDParsed = 0
	}

	// Parse query parameters for time range
	intervalStr := r.URL.Query().Get("interval")
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	// Set defaults
	interval := "1h" // default interval

	// Parse interval parameter
	if intervalStr != "" {
		interval = intervalStr
	}

	// Calculate time range - use query params if provided, otherwise default to last hour
	var startTime, endTime time.Time
	var startUnix, endUnix int64

	if startStr != "" && endStr != "" {
		// Parse Unix timestamps from query parameters (in seconds)
		if val, err := strconv.ParseInt(startStr, 10, 64); err == nil {
			startTime = time.Unix(val, 0)
			startUnix = val
		}
		if val, err := strconv.ParseInt(endStr, 10, 64); err == nil {
			endTime = time.Unix(val, 0)
			endUnix = val
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

	// Fetch statement details using the dedicated endpoint
	// Use the stmtID directly as the fingerprint_id
	resp, err := cfg.Status.StatementDetails(ctx, &serverpb.StatementDetailsRequest{
		FingerprintId: stmtID,
		Start:         startUnix,
		End:           endUnix,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to get statement details: %v", err)
		http.Error(w, "Failed to get statement details", http.StatusInternalServerError)
		return
	}

	// Extract the statement summary
	stmt := &resp.Statement

	// Format timestamps for display
	loc, _ := timeutil.LoadLocation(timezone)
	startTimeStr := startTime.In(loc).Format("2006-01-02 15:04:05")
	endTimeStr := endTime.In(loc).Format("2006-01-02 15:04:05")

	// Extract query from metadata
	query := stmt.Metadata.Query

	// Fetch diagnostic reports for this fingerprint
	diagResp, err := cfg.Status.StatementDiagnosticsRequests(ctx, &serverpb.StatementDiagnosticsReportsRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch diagnostic reports: %v", err)
		// Continue without diagnostic states
	}

	// Collect unique plan gists from plan-grouped stats
	planGistSet := make(map[string]bool)
	for _, planStat := range resp.StatementStatisticsPerPlanHash {
		// Extract plan gists from the Stats.PlanGists array
		for _, gist := range planStat.Stats.PlanGists {
			if gist != "" {
				planGistSet[gist] = true
			}
		}
	}
	var planGists []string
	for gist := range planGistSet {
		planGists = append(planGists, gist)
	}
	// Sort for consistent ordering
	sort.Strings(planGists)

	// Build diagnostic state for this fingerprint
	diagState := DiagnosticState{
		Query:     query,
		ID:        appstatspb.StmtFingerprintID(stmtIDParsed), // Used for display only
		PlanGists: planGists,
		TableMode: true,
	}

	if diagResp != nil {
		for _, report := range diagResp.Reports {
			if report.StatementFingerprint == query {
				if !report.Completed {
					diagState.HasPendingRequest = true
					diagState.PendingRequestID = report.Id
				} else if report.StatementDiagnosticsId > 0 {
					bundle := DiagnosticBundle{
						ID:          report.StatementDiagnosticsId,
						CollectedAt: report.RequestedAt,
						DownloadURL: fmt.Sprintf("/future/sqlactivity/stmtdiagnostics/%d/download", report.StatementDiagnosticsId),
					}
					diagState.CompletedBundles = append(diagState.CompletedBundles, bundle)
				}
			}
		}
	}

	// Build page data
	pageData := StatementFingerprintPageData{
		Statement: *stmt,
		StmtID:    stmtID,
		Params: SQLActivityParams{
			Interval:  interval,
			Start:     startUnix,
			End:       endUnix,
			StartTime: startTimeStr,
			EndTime:   endTimeStr,
			Timezone:  timezone,
		},
		Diagnostics: diagState,
		PlanStats:   resp.StatementStatisticsPerPlanHash,
	}

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "statement_fingerprint.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          pageData,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

func handleSqlActivityStatements(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
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

	// Fetch diagnostic reports to determine state for each statement
	diagnosticStates := make(map[string]*DiagnosticState)
	diagResp, err := cfg.Status.StatementDiagnosticsRequests(r.Context(), &serverpb.StatementDiagnosticsReportsRequest{})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to fetch diagnostic reports: %v", err)
		// Continue without diagnostic states - we'll just show the Activate button for all
	} else {
		// Group diagnostic reports by statement fingerprint
		for _, report := range diagResp.Reports {
			state, ok := diagnosticStates[report.StatementFingerprint]
			if !ok {
				diagnosticStates[report.StatementFingerprint] = &DiagnosticState{}
				state = diagnosticStates[report.StatementFingerprint]
			}

			if !report.Completed {
				// This is a pending request
				state.HasPendingRequest = true
				state.PendingRequestID = report.Id
			} else if report.StatementDiagnosticsId > 0 {
				// This is a completed bundle
				bundle := DiagnosticBundle{
					ID:          report.StatementDiagnosticsId,
					CollectedAt: report.RequestedAt, // Using RequestedAt as placeholder
					DownloadURL: fmt.Sprintf("/future/sqlactivity/stmtdiagnostics/%d/download", report.StatementDiagnosticsId),
				}
				state.CompletedBundles = append(state.CompletedBundles, bundle)
			}

			diagnosticStates[report.StatementFingerprint] = state
		}
	}

	for _, stmt := range resp.Statements {
		s, ok := diagnosticStates[stmt.Key.KeyData.Query]
		if !ok {
			diagnosticStates[stmt.Key.KeyData.Query] = &DiagnosticState{
				ID:        stmt.ID,
				Query:     stmt.Key.KeyData.Query,
				PlanGists: stmt.Stats.PlanGists,
			}
		} else {
			s.ID = stmt.ID
			s.Query = stmt.Key.KeyData.Query
			s.PlanGists = stmt.Stats.PlanGists
		}
	}

	// Build the data structure with params
	data := SQLActivityData{
		Statements:       resp,
		TotalWorkload:    totalWorkload,
		DiagnosticStates: diagnosticStates,
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

	// Check if request accepts JSON
	acceptHeader := r.Header.Get("Accept")
	if strings.Contains(acceptHeader, "application/json") {
		w.Header().Set("Content-Type", "application/json")

		// Build columns array for AntD
		columns := []map[string]interface{}{
			{"title": "Statements", "dataIndex": "query", "key": "query"},
			{"title": "Execution Count", "dataIndex": "executionCount", "key": "executionCount"},
			{"title": "Database", "dataIndex": "database", "key": "database"},
			{"title": "Application Name", "dataIndex": "applicationName", "key": "applicationName"},
			{"title": "Statement Time", "dataIndex": "statementTime", "key": "statementTime"},
			{"title": "% of All Runtime", "dataIndex": "pctRuntime", "key": "pctRuntime"},
			{"title": "Contention Time", "dataIndex": "contentionTime", "key": "contentionTime"},
			{"title": "SQL CPU Time", "dataIndex": "sqlCPUTime", "key": "sqlCPUTime"},
			{"title": "Min Latency", "dataIndex": "minLatency", "key": "minLatency"},
			{"title": "Max Latency", "dataIndex": "maxLatency", "key": "maxLatency"},
			{"title": "Rows Processed", "dataIndex": "rowsProcessed", "key": "rowsProcessed"},
			{"title": "Bytes Read", "dataIndex": "bytesRead", "key": "bytesRead"},
			{"title": "Max Memory", "dataIndex": "maxMemory", "key": "maxMemory"},
			{"title": "Network", "dataIndex": "network", "key": "network"},
			{"title": "Retries", "dataIndex": "retries", "key": "retries"},
			{"title": "Last Execution Time", "dataIndex": "lastExecTime", "key": "lastExecTime"},
			{"title": "Statement Fingerprint ID", "dataIndex": "fingerprintID", "key": "fingerprintID"},
			{"title": "Diagnostics", "dataIndex": "diagnostics", "key": "diagnostics"},
		}

		// Prepare total workload for % calculations
		totalWorkload := data.TotalWorkload

		// Build rows (dataSource) array
		rows := []map[string]interface{}{}
		for _, stmt := range data.Statements.Statements {
			// Calculate needed values
			var pctRuntime float64
			if totalWorkload != 0 {
				pctRuntime = 100.0 * (stmt.Stats.ServiceLat.Mean * float64(stmt.Stats.Count)) / totalWorkload
			}
			lastExecTime := ""
			if !stmt.Stats.LastExecTimestamp.IsZero() {
				lastExecTime = stmt.Stats.LastExecTimestamp.Format("2006-01-02 15:04:05")
			}

			diagnostics := ""
			if s, ok := data.DiagnosticStates[stmt.Key.KeyData.Query]; ok && s != nil {
				if s.HasPendingRequest {
					diagnostics = "Pending"
				} else if len(s.CompletedBundles) > 0 {
					diagnostics = "Completed"
				} else {
					diagnostics = ""
				}
			}

			// Format values using Go formatting functions
			// formatPercent expects a decimal (0-1), so divide by 100
			pctRuntimeFormatted := formatPercent(pctRuntime / 100.0)
			// formatTime expects nanoseconds as float64
			statementTimeFormatted := formatTime(stmt.Stats.ServiceLat.Mean)
			contentionTimeFormatted := formatTime(stmt.Stats.ExecStats.ContentionTime.Mean)
			sqlCPUTimeFormatted := formatTime(stmt.Stats.ExecStats.CPUSQLNanos.Mean)
			minLatencyFormatted := formatTime(stmt.Stats.LatencyInfo.Min)
			maxLatencyFormatted := formatTime(stmt.Stats.LatencyInfo.Max)
			// formatBytes expects bytes as float64
			bytesReadFormatted := formatBytes(stmt.Stats.BytesRead.Mean)
			maxMemoryFormatted := formatBytes(stmt.Stats.ExecStats.MaxMemUsage.Mean)
			networkFormatted := formatBytes(stmt.Stats.ExecStats.NetworkBytes.Mean)

			row := map[string]interface{}{
				"key":             stmt.ID,
				"query":           stmt.Key.KeyData.Query,
				"executionCount":  stmt.Stats.Count,
				"database":        stmt.Key.KeyData.Database,
				"applicationName": stmt.Key.KeyData.App,
				"statementTime":   statementTimeFormatted,
				"pctRuntime":      pctRuntimeFormatted,
				"contentionTime":  contentionTimeFormatted,
				"sqlCPUTime":      sqlCPUTimeFormatted,
				"minLatency":      minLatencyFormatted,
				"maxLatency":      maxLatencyFormatted,
				"rowsProcessed":   stmt.Stats.RowsRead.Mean + stmt.Stats.RowsWritten.Mean,
				"rowsRead":        stmt.Stats.RowsRead.Mean,
				"rowsWritten":     stmt.Stats.RowsWritten.Mean,
				"bytesRead":       bytesReadFormatted,
				"maxMemory":       maxMemoryFormatted,
				"network":         networkFormatted,
				"retries":         stmt.Stats.Count - stmt.Stats.FirstAttemptCount,
				"lastExecTime":    lastExecTime,
				"fingerprintID":   stmt.ID,
				"diagnostics":     diagnostics,
			}
			rows = append(rows, row)
		}

		// Build diagnostic states for each row
		diagnosticStatesMap := make(map[string]interface{})
		for query, diagState := range data.DiagnosticStates {
			diagnosticStatesMap[query] = map[string]interface{}{
				"hasPendingRequest": diagState.HasPendingRequest,
				"pendingRequestID":  diagState.PendingRequestID,
				"completedBundles":  diagState.CompletedBundles,
				"id":                diagState.ID,
				"query":             diagState.Query,
				"planGists":         diagState.PlanGists,
			}
		}

		// Build interval options with keys and display values
		intervalOptions := []map[string]string{
			{"key": "1h", "value": "Past Hour"},
			{"key": "6h", "value": "Past 6 Hours"},
			{"key": "1d", "value": "Past Day"},
			{"key": "2d", "value": "Past 2 Days"},
			{"key": "3d", "value": "Past 3 Days"},
			{"key": "1w", "value": "Past Week"},
			{"key": "2w", "value": "Past 2 Weeks"},
			{"key": "1m", "value": "Past Month"},
			{"key": "Custom", "value": "Custom"},
		}

		result := map[string]interface{}{
			"columns":    columns,
			"dataSource": rows,
			"meta": map[string]interface{}{
				"total":     len(rows),
				"interval":  data.Params.Interval,
				"startTime": data.Params.StartTime,
				"endTime":   data.Params.EndTime,
				"timezone":  data.Params.Timezone,
				"sortBy":    data.Params.By,
				"top":       data.Params.Top,
				"start":     data.Params.Start,
				"end":       data.Params.End,
			},
			"filters": map[string]interface{}{
				"topOptions":      []int{25, 50, 100, 500, 1000, 5000, 10000},
				"byOptions":       []string{"% of All Runtime", "Contention Time", "Execution Count", "SQL CPU Time", "Statement Time", "Last Execution Time", "Max Latency", "Max Memory", "Min Latency", "Network", "Retries", "Rows Processed"},
				"intervalOptions": intervalOptions,
			},
			"diagnosticStates": diagnosticStatesMap,
		}

		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Dev.Warningf(r.Context(), "Failed to encode JSON response: %v", err)
			http.Error(w, "Failed to encode JSON response", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "sql_activity.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
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
func handleOverview(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	// Set cache control headers to prevent stale data
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Fetch cluster data
	data, err := fetchOverviewData(r.Context(), cfg)
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to fetch overview data: %v", err)
		http.Error(w, "Failed to fetch cluster data", http.StatusInternalServerError)
		return
	}

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "overview.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          data,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// fetchOverviewData fetches and processes cluster overview data
func fetchOverviewData(ctx context.Context, cfg IndexHTMLArgs) (*OverviewData, error) {
	data := &OverviewData{}

	// Fetch nodes information
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch nodes: %w", err)
	}

	// Group nodes by locality (cloud, region, az)
	regionMap := make(map[string][]NodeInfo)
	data.TotalNodes = len(nodesResp.Nodes)

	// Process nodes data to calculate capacity and node status counts
	for _, node := range nodesResp.Nodes {
		// Extract locality grouping (cloud, region, az)
		region := extractLocalityGroup(&node.Desc.Locality)

		// Calculate node-level metrics
		var nodeReplicas int
		var nodeCapacityUsed, nodeCapacityTotal int64
		for _, store := range node.StoreStatuses {
			storeCapacity := store.Desc.Capacity.Capacity
			storeAvailable := store.Desc.Capacity.Available

			// Prefer the .Used field when available, fall back to capacity - available
			// The Used field tracks CockroachDB's actual disk usage
			storeUsed := store.Desc.Capacity.Used
			if storeUsed == 0 && storeCapacity > storeAvailable {
				// Fallback calculation if Used field is not populated
				storeUsed = storeCapacity - storeAvailable
			}

			data.CapacityUsed += storeUsed
			data.CapacityTotal += storeCapacity
			data.CapacityAvailable += storeAvailable

			// Use the "ranges" metric which represents actual ranges on this store
			// (not replicas). Sum across all stores to get total unique ranges.
			if rangesMetric, ok := store.Metrics["ranges"]; ok {
				data.TotalRanges += int(rangesMetric)
			}

			nodeCapacityUsed += storeUsed
			nodeCapacityTotal += storeCapacity
			nodeReplicas += int(store.Desc.Capacity.RangeCount)
		}

		// Calculate node capacity percentage
		var nodeCapacityPercent float64
		if nodeCapacityTotal > 0 {
			nodeCapacityPercent = float64(nodeCapacityUsed) / float64(nodeCapacityTotal)
		}

		// Get node status and class
		status, statusClass := getNodeStatus(int32(nodesResp.LivenessByNodeID[node.Desc.NodeID]))

		// Count node status
		if liveness, ok := nodesResp.LivenessByNodeID[node.Desc.NodeID]; ok {
			switch liveness {
			case 3: // NODE_STATUS_LIVE
				data.LiveNodes++
			case 2: // NODE_STATUS_UNAVAILABLE (suspect)
				data.SuspectNodes++
			case 6: // NODE_STATUS_DRAINING
				data.DrainingNodes++
			case 1: // NODE_STATUS_DEAD
				data.DeadNodes++
			}
		}

		// Calculate uptime
		// Note: StartedAt is in nanoseconds since Unix epoch
		now := timeutil.Now().UnixNano()
		uptimeNanos := now - node.StartedAt
		uptimeSeconds := uptimeNanos / 1e9
		uptimeFormatted := formatUptime(uptimeSeconds)

		// Get memory metrics
		var memoryPercent float64
		if memUsed, ok := node.Metrics["sys.rss"]; ok {
			if memTotal, ok := node.Metrics["sys.totalmem"]; ok && memTotal > 0 {
				memoryPercent = memUsed / memTotal
			}
		}

		// Get CPU count
		var vcpus int
		if cpuCount, ok := node.Metrics["sys.cpu.sys.percent"]; ok {
			vcpus = int(cpuCount)
		}
		// Fallback: try to get CPU count from a different metric or use a default
		if vcpus == 0 {
			vcpus = 1 // Default if we can't determine
		}

		// Create NodeInfo
		nodeInfo := NodeInfo{
			NodeID:          int32(node.Desc.NodeID),
			Address:         node.Desc.Address.AddressField,
			Region:          region,
			UptimeSeconds:   uptimeSeconds,
			UptimeFormatted: uptimeFormatted,
			Replicas:        nodeReplicas,
			CapacityPercent: nodeCapacityPercent,
			MemoryPercent:   memoryPercent,
			VCPUs:           vcpus,
			Version:         node.BuildInfo.Tag,
			Status:          status,
			StatusClass:     statusClass,
		}

		regionMap[region] = append(regionMap[region], nodeInfo)
	}

	// Calculate capacity percentage
	if data.CapacityTotal > 0 {
		data.CapacityPercent = float64(data.CapacityUsed) / float64(data.CapacityTotal)
	}

	// Format capacity strings using the existing formatBytes function
	data.CapacityUsedStr = formatBytes(float64(data.CapacityUsed))
	data.CapacityTotalStr = formatBytes(float64(data.CapacityTotal))

	// Fetch problem ranges
	problemRangesResp, err := cfg.Status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
	if err != nil {
		// Log the error but continue - we can still show the rest of the data
		log.Dev.Warningf(ctx, "Failed to fetch problem ranges: %v", err)
	} else {
		// Count underreplicated and unavailable ranges across all nodes
		for _, nodeProblems := range problemRangesResp.ProblemsByNodeID {
			data.UnderReplicatedRanges += len(nodeProblems.UnderreplicatedRangeIDs)
			data.UnavailableRanges += len(nodeProblems.UnavailableRangeIDs)
		}
	}

	// Build region groups with aggregated metrics
	data.RegionGroups = buildRegionGroups(regionMap)

	return data, nil
}

// extractRegion extracts the region value from locality tiers
func extractRegion(locality *serverpb.Locality) string {
	if locality == nil {
		return ""
	}
	for _, tier := range locality.Tiers {
		if tier.Key == "region" {
			return tier.Value
		}
	}
	return ""
}

// extractLocalityGroup creates a human-readable locality grouping string
// combining cloud, region, and az (availability zone) if available
func extractLocalityGroup(locality *serverpb.Locality) string {
	if locality == nil {
		return "(no locality)"
	}

	var cloud, region, az string
	for _, tier := range locality.Tiers {
		switch tier.Key {
		case "cloud":
			cloud = tier.Value
		case "region":
			region = tier.Value
		case "az", "availability-zone", "zone":
			az = tier.Value
		}
	}

	// Build the group name from available locality tags
	var parts []string
	if cloud != "" {
		parts = append(parts, cloud)
	}
	if region != "" {
		parts = append(parts, region)
	}
	if az != "" {
		parts = append(parts, az)
	}

	if len(parts) == 0 {
		return "(no locality)"
	}

	return strings.Join(parts, ", ")
}

// getNodeStatus returns a human-readable status and CSS class for a liveness status
func getNodeStatus(liveness int32) (string, string) {
	switch liveness {
	case 3: // NODE_STATUS_LIVE
		return "Live", "info"
	case 2: // NODE_STATUS_UNAVAILABLE
		return "Suspect", "warn"
	case 6: // NODE_STATUS_DRAINING
		return "Draining", "warn"
	case 1: // NODE_STATUS_DEAD
		return "Dead", "bad"
	case 4: // NODE_STATUS_DECOMMISSIONING
		return "Decommissioning", "warn"
	case 5: // NODE_STATUS_DECOMMISSIONED
		return "Decommissioned", "bad"
	default:
		return "Unknown", "warn"
	}
}

// formatUptime formats seconds into a human-readable uptime string
func formatUptime(seconds int64) string {
	if seconds < 0 {
		return "0 s"
	}
	if seconds < 60 {
		return fmt.Sprintf("%d s", seconds)
	}
	minutes := seconds / 60
	if minutes < 60 {
		return fmt.Sprintf("%d min", minutes)
	}
	hours := minutes / 60
	if hours < 24 {
		return fmt.Sprintf("%d hr", hours)
	}
	days := hours / 24
	return fmt.Sprintf("%d days", days)
}

// GenerateLicenseNotification generates the license notification text, tooltip, and CSS class
// based on the license type and days until expiry. Returns empty strings if no notification needed.
// Based on logic from pkg/ui/workspaces/db-console/src/views/app/containers/licenseNotification/licenseNotification.tsx
func generateLicenseNotification(
	licenseType string, secondsUntilExpiry int64, isManaged bool,
) (text, tooltip, class string) {
	const daysToWarn = 30 // Equal to trial period

	// No notification for clusters without license or managed clusters
	if licenseType == "None" || isManaged {
		return "", "", ""
	}

	daysUntilExpiry := secondsUntilExpiry / 86400 // Convert seconds to days
	expirationDate := timeutil.Now().Add(time.Duration(secondsUntilExpiry) * time.Second).Format("January 2, 2006")

	learnMoreLink := "https://cockroachlabs.com/docs/stable/licensing-faqs.html"

	if daysUntilExpiry > daysToWarn {
		// Info notification - more than 30 days until expiry
		text = fmt.Sprintf("License expires in %d days", absInt64(daysUntilExpiry))
		tooltip = fmt.Sprintf("You are using an %s license of CockroachDB. To avoid service disruption, please renew before the expiration date of %s. <a href=\"%s\" target=\"_blank\" rel=\"noreferrer\">Learn more</a>",
			licenseType, expirationDate, learnMoreLink)
		class = "info"
	} else if daysUntilExpiry == 0 {
		// Warning - expires today
		text = fmt.Sprintf("License expires in %d days", absInt64(daysUntilExpiry))
		tooltip = fmt.Sprintf("Your %s license of CockroachDB expired today. To re-enable Enterprise features, please renew your license. <a href=\"%s\" target=\"_blank\" rel=\"noreferrer\">Learn more</a>",
			licenseType, learnMoreLink)
		class = "warn"
	} else if daysUntilExpiry <= 0 {
		// Error - already expired
		text = fmt.Sprintf("License expired %d days ago", absInt64(daysUntilExpiry))
		tooltip = fmt.Sprintf("Your %s license of CockroachDB expired on %s. To re-enable Enterprise features, please renew your license. <a href=\"%s\" target=\"_blank\" rel=\"noreferrer\">Learn more</a>",
			licenseType, expirationDate, learnMoreLink)
		class = "bad"
	} else if daysUntilExpiry <= daysToWarn {
		// Warning - 30 days or less until expiry
		text = fmt.Sprintf("License expires in %d days", daysUntilExpiry)
		tooltip = fmt.Sprintf("You are using an %s license of CockroachDB. To avoid service disruption, please renew before the expiration date of %s. <a href=\"%s\" target=\"_blank\" rel=\"noreferrer\">Learn more</a>",
			licenseType, expirationDate, learnMoreLink)
		class = "warn"
	}

	return text, tooltip, class
}

// absInt64 returns the absolute value of an int64
func absInt64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// buildRegionGroups creates RegionGroup entries with aggregated metrics
func buildRegionGroups(regionMap map[string][]NodeInfo) []RegionGroup {
	var groups []RegionGroup

	for regionName, nodes := range regionMap {
		group := RegionGroup{
			RegionName: regionName,
			NodeCount:  len(nodes),
			Nodes:      nodes,
		}

		// Calculate aggregates
		var totalCapacityPercent, totalMemoryPercent float64
		var minUptime int64 = -1
		allLive := true

		for _, node := range nodes {
			group.TotalReplicas += node.Replicas
			totalCapacityPercent += node.CapacityPercent
			totalMemoryPercent += node.MemoryPercent
			group.TotalVCPUs += node.VCPUs

			if minUptime == -1 || node.UptimeSeconds < minUptime {
				minUptime = node.UptimeSeconds
			}

			if node.Status != "Live" {
				allLive = false
			}
		}

		// Average percentages
		if len(nodes) > 0 {
			group.CapacityPercent = totalCapacityPercent / float64(len(nodes))
			group.MemoryPercent = totalMemoryPercent / float64(len(nodes))
		}

		group.UptimeSeconds = minUptime

		// Set region status
		if allLive {
			group.Status = "Live"
		} else {
			group.Status = "Degraded"
		}

		groups = append(groups, group)
	}

	return groups
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

// MetricsDashboardData contains data for rendering the metrics dashboard page
type MetricsDashboardData struct {
	DashboardName        string
	DashboardDisplayName string
	Graphs               []DashboardGraph
	AllDashboards        []DashboardInfo
	NodeIDs              []int32
	Nodes                []NodeDisplayInfo
	SelectedNode         string // Node ID as string, empty string means "Cluster" (all nodes)
}

// NodeDisplayInfo contains minimal info for displaying a node in a dropdown
type NodeDisplayInfo struct {
	NodeID  string
	Address string
}

// DashboardInfo contains the internal name and display name for a dashboard
type DashboardInfo struct {
	Name        string
	DisplayName string
}

const apiBaseURL = "http://localhost:8080"

// API response types for database and table metadata
// These mirror the types from pkg/server/api_v2_databases_metadata.go

type apiPaginatedResponse[T any] struct {
	Results        T                 `json:"results"`
	PaginationInfo apiPaginationInfo `json:"pagination_info"`
}

type apiPaginationInfo struct {
	TotalResults int64 `json:"total_results"`
	PageSize     int   `json:"page_size"`
	PageNum      int   `json:"page_num"`
}

type apiTableMetadata struct {
	DbID                 int64      `json:"db_id"`
	DbName               string     `json:"db_name"`
	TableID              int64      `json:"table_id"`
	SchemaName           string     `json:"schema_name"`
	TableName            string     `json:"table_name"`
	ReplicationSizeBytes int64      `json:"replication_size_bytes"`
	RangeCount           int64      `json:"range_count"`
	ColumnCount          int64      `json:"column_count"`
	IndexCount           int64      `json:"index_count"`
	PercentLiveData      float32    `json:"percent_live_data"`
	TotalLiveDataBytes   int64      `json:"total_live_data_bytes"`
	TotalDataBytes       int64      `json:"total_data_bytes"`
	StoreIDs             []int64    `json:"store_ids"`
	AutoStatsEnabled     bool       `json:"auto_stats_enabled"`
	StatsLastUpdated     *time.Time `json:"stats_last_updated"`
	LastUpdateError      string     `json:"last_update_error,omitempty"`
	LastUpdated          time.Time  `json:"last_updated"`
	ReplicaCount         int64      `json:"replica_count"`
}

type apiDbMetadata struct {
	DbID        int64      `json:"db_id"`
	DbName      string     `json:"db_name"`
	SizeBytes   int64      `json:"size_bytes"`
	TableCount  int64      `json:"table_count"`
	StoreIDs    []int64    `json:"store_ids"`
	LastUpdated *time.Time `json:"last_updated"`
}

type apiTableMetadataWithDetails struct {
	Metadata        apiTableMetadata `json:"metadata"`
	CreateStatement string           `json:"create_statement"`
}

type apiDbMetadataWithDetails struct {
	Metadata apiDbMetadata `json:"metadata"`
}

type apiTableMetadataJobStatus struct {
	CurrentStatus           string     `json:"current_status"`
	Progress                float32    `json:"progress"`
	LastStartTime           *time.Time `json:"last_start_time"`
	LastCompletedTime       *time.Time `json:"last_completed_time"`
	LastUpdatedTime         *time.Time `json:"last_updated_time"`
	DataValidDuration       int        `json:"data_valid_duration"`
	AutomaticUpdatesEnabled bool       `json:"automatic_updates_enabled"`
}

type apiTableMetadataJobTriggered struct {
	JobTriggered bool   `json:"job_triggered"`
	Message      string `json:"message"`
}

// DatabasesPageData contains data for the databases list page
type DatabasesPageData struct {
	Databases       []DatabaseWithRegions
	SearchFilter    string
	SelectedNode    int32
	NodesByRegion   map[string][]NodeDisplayInfo
	LastRefreshed   *time.Time
	MetadataJobInfo *MetadataJobInfo
}

// MetadataJobInfo contains information about the table metadata cache job
type MetadataJobInfo struct {
	IsRunning       bool
	LastCompletedAt *time.Time
}

// DatabaseWithRegions extends database metadata with region/node information
type DatabaseWithRegions struct {
	apiDbMetadata
	RegionNodes map[string][]int32 // Map of region name to node IDs
}

// DatabasePageData contains data for a single database page
type DatabasePageData struct {
	DatabaseID      int64
	DatabaseName    string
	Tables          []apiTableMetadata
	SearchFilter    string
	SelectedNode    int32
	NodesByRegion   map[string][]NodeDisplayInfo
	Grants          []DatabaseGrant
	MetadataJobInfo *MetadataJobInfo
}

// DatabaseGrant represents a privilege grant on a database
type DatabaseGrant struct {
	Grantee    string
	Privileges []string
}

// TablePageData contains data for a single table page
type TablePageData struct {
	Table           apiTableMetadata
	CreateStatement string
	DatabaseName    string
	RegionNodes     map[string][]int32 // Map of region name to node IDs
	Grants          []TableGrant
	Indexes         []IndexInfo
	LastUpdated     *time.Time
	IndexStatsReset *time.Time // Last time index stats were reset
}

// TableGrant represents a privilege grant on a table
type TableGrant struct {
	Grantee    string
	Privileges []string
}

// IndexInfo contains information about a table index
type IndexInfo struct {
	IndexName      string
	LastRead       *time.Time
	TotalReads     int64
	Recommendation string
}

// MetricsSummaryData contains data for the metrics summary box
type MetricsSummaryData struct {
	TotalNodes        int
	CapacityUsedStr   string
	CapacityTotalStr  string
	CapacityPercent   float64
	UnavailableRanges int
	QueriesPerSecond  string
	P99Latency        float64
}

// EventData contains information about a cluster event
type EventData struct {
	Description string
	Timestamp   float64 // Nanoseconds since epoch
}

// fetchJSON makes an HTTP GET request and unmarshals the JSON response
func fetchJSON(ctx context.Context, url string, target interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("API request failed with status %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

// fetchDatabaseGrants fetches the grants for a database
func fetchDatabaseGrants(
	ctx context.Context, cfg IndexHTMLArgs, dbName string,
) ([]DatabaseGrant, error) {
	// Use the DatabaseDetails RPC to get grant information
	resp, err := cfg.Admin.DatabaseDetails(ctx, &serverpb.DatabaseDetailsRequest{
		Database: dbName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch database details: %w", err)
	}

	// Convert grants from the response to our format
	grantsMap := make(map[string][]string)
	for _, grant := range resp.Grants {
		grantsMap[grant.User] = append(grantsMap[grant.User], grant.Privileges...)
	}

	// Convert map to slice
	var grants []DatabaseGrant
	for grantee, privileges := range grantsMap {
		grants = append(grants, DatabaseGrant{
			Grantee:    grantee,
			Privileges: privileges,
		})
	}

	// Sort by grantee for consistent ordering
	sort.Slice(grants, func(i, j int) bool {
		return grants[i].Grantee < grants[j].Grantee
	})

	return grants, nil
}

// fetchTableGrants fetches the grants for a table
func fetchTableGrants(
	ctx context.Context, cfg IndexHTMLArgs, dbName, schemaName, tableName string,
) ([]TableGrant, error) {
	// Use the TableDetails RPC to get grant information
	fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName)
	resp, err := cfg.Admin.TableDetails(ctx, &serverpb.TableDetailsRequest{
		Database: dbName,
		Table:    fullyQualifiedTableName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table details: %w", err)
	}

	// Convert grants from the response to our format
	grantsMap := make(map[string][]string)
	for _, grant := range resp.Grants {
		grantsMap[grant.User] = append(grantsMap[grant.User], grant.Privileges...)
	}

	// Convert map to slice
	var grants []TableGrant
	for grantee, privileges := range grantsMap {
		grants = append(grants, TableGrant{
			Grantee:    grantee,
			Privileges: privileges,
		})
	}

	// Sort by grantee for consistent ordering
	sort.Slice(grants, func(i, j int) bool {
		return grants[i].Grantee < grants[j].Grantee
	})

	return grants, nil
}

// fetchIndexStats fetches index statistics for a table
func fetchIndexStats(
	ctx context.Context, cfg IndexHTMLArgs, dbName, tableName string,
) ([]IndexInfo, *time.Time, error) {
	// Use the TableIndexStats RPC which is specifically for getting index stats for a table
	resp, err := cfg.Status.TableIndexStats(ctx, &serverpb.TableIndexStatsRequest{
		Database: dbName,
		Table:    tableName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch table index stats: %w", err)
	}

	var indexes []IndexInfo
	for _, stat := range resp.Statistics {
		var lastRead *time.Time
		if !stat.Statistics.Stats.LastRead.IsZero() {
			lastRead = &stat.Statistics.Stats.LastRead
		}

		recommendation := "None"
		// Add basic recommendation logic
		if stat.Statistics.Stats.TotalReadCount == 0 {
			recommendation = "Consider dropping - no reads detected"
		}

		indexes = append(indexes, IndexInfo{
			IndexName:      stat.IndexName,
			LastRead:       lastRead,
			TotalReads:     int64(stat.Statistics.Stats.TotalReadCount),
			Recommendation: recommendation,
		})
	}

	// Sort by index name
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].IndexName < indexes[j].IndexName
	})

	// Get last reset time - resp.LastReset is time.Time with stdtime = true
	var lastReset *time.Time
	if resp.LastReset != nil && !resp.LastReset.IsZero() {
		lastReset = resp.LastReset
	}

	return indexes, lastReset, nil
}

// handleMetricsDashboard serves the metrics dashboard page
func handleMetricsDashboard(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	dashboardName := r.URL.Query().Get("dashboard")
	if dashboardName == "" {
		dashboardName = "overview"
	}

	// Get the selected node from query parameter (empty string means cluster-wide)
	selectedNode := r.URL.Query().Get("node")

	// Look up dashboard definition
	graphs, ok := DASHBOARDS[dashboardName]
	if !ok {
		http.Error(w, "Dashboard not found", http.StatusNotFound)
		return
	}

	// Fetch nodes to get node IDs for per-node metric expansion
	nodesResp, err := cfg.Status.NodesUI(r.Context(), &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Extract node IDs and build node display info
	var nodeIDs []int32
	var nodes []NodeDisplayInfo
	for _, node := range nodesResp.Nodes {
		nodeID := int32(node.Desc.NodeID)
		nodeIDs = append(nodeIDs, nodeID)
		nodes = append(nodes, NodeDisplayInfo{
			NodeID:  fmt.Sprintf("%d", nodeID),
			Address: node.Desc.Address.AddressField,
		})
	}

	// Expand per-node and per-store metrics
	expandedGraphs := expandDashboardMetrics(graphs, nodeIDs, selectedNode)

	// Get list of all dashboard names for dropdown
	dashboardOrder := []string{
		"overview", "hardware", "runtime", "sql", "storage",
		"replication", "distributed", "queues", "networking",
		"requests", "overload", "changefeeds", "ttl",
		"logicalDataReplication", "crossClusterReplication",
	}

	var allDashboards []DashboardInfo
	for _, name := range dashboardOrder {
		displayName := DashboardDisplayNames[name]
		if displayName == "" {
			displayName = name // fallback to internal name if no display name defined
		}
		allDashboards = append(allDashboards, DashboardInfo{
			Name:        name,
			DisplayName: displayName,
		})
	}

	// Get display name for current dashboard
	displayName := DashboardDisplayNames[dashboardName]
	if displayName == "" {
		displayName = dashboardName
	}

	data := MetricsDashboardData{
		DashboardName:        dashboardName,
		DashboardDisplayName: displayName,
		Graphs:               expandedGraphs,
		AllDashboards:        allDashboards,
		NodeIDs:              nodeIDs,
		Nodes:                nodes,
		SelectedNode:         selectedNode,
	}

	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Execute the pre-parsed template
	err = templates.ExecuteTemplate(w, "metrics.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          data,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// expandDashboardMetrics expands per-node and per-store metrics into individual metric entries
// If selectedNode is not empty, only metrics for that node are included
func expandDashboardMetrics(
	graphs []DashboardGraph, nodeIDs []int32, selectedNode string,
) []DashboardGraph {
	var expandedGraphs []DashboardGraph

	// Parse selectedNode if present
	var filterNodeID int32 = -1
	if selectedNode != "" {
		if nodeID, err := strconv.ParseInt(selectedNode, 10, 32); err == nil {
			filterNodeID = int32(nodeID)
		}
	}

	// Determine which nodes to include
	var targetNodeIDs []int32
	if filterNodeID != -1 {
		// Filter to specific node
		targetNodeIDs = []int32{filterNodeID}
	} else {
		// Include all nodes
		targetNodeIDs = nodeIDs
	}

	for _, graph := range graphs {
		expandedGraph := graph
		expandedGraph.Metrics = nil // Clear and rebuild

		for _, metric := range graph.Metrics {
			if metric.NonNegativeRate {
				metric.Derivative = "NON_NEGATIVE_DERIVATIVE"
			}
			if metric.StorePrefix || metric.PerStore {
				metric.Name = "cr.store." + metric.Name
			} else {
				metric.Name = "cr.node." + metric.Name
			}
			if metric.PerNode {
				// Create one metric per node (filtered by targetNodeIDs)
				for _, nodeID := range targetNodeIDs {
					expandedMetric := metric
					expandedMetric.PerNode = false
					expandedMetric.Sources = []int{int(nodeID)}
					if expandedMetric.Title == "" {
						expandedMetric.Title = fmt.Sprintf("n%d", nodeID)
					} else {
						// If there's already a title, append the node
						expandedMetric.Title = fmt.Sprintf("%s (n%d)", metric.Title, nodeID)
					}
					expandedGraph.Metrics = append(expandedGraph.Metrics, expandedMetric)
				}
			} else if metric.PerStore {
				// For per-store metrics, we also create one per node (filtered by targetNodeIDs)
				// (each node typically has one store, but we use the same pattern)
				for _, nodeID := range targetNodeIDs {
					expandedMetric := metric
					expandedMetric.PerStore = false
					expandedMetric.Sources = []int{int(nodeID)}
					if expandedMetric.Title == "" {
						expandedMetric.Title = fmt.Sprintf("n%d", nodeID)
					} else {
						expandedMetric.Title = fmt.Sprintf("%s (n%d)", metric.Title, nodeID)
					}
					expandedGraph.Metrics = append(expandedGraph.Metrics, expandedMetric)
				}
			} else {
				// Regular metric - use targetNodeIDs for sources
				for _, nodeID := range targetNodeIDs {
					metric.Sources = append(metric.Sources, int(nodeID))
				}
				// Regular metric, just copy as-is
				expandedGraph.Metrics = append(expandedGraph.Metrics, metric)
			}

		}

		expandedGraphs = append(expandedGraphs, expandedGraph)
	}

	return expandedGraphs
}

// timeSeriesQueryRequestJSON is a JSON-friendly wrapper for TimeSeriesQueryRequest
// that handles nanosecond timestamps as strings (since JavaScript can't handle int64)
type timeSeriesQueryRequestJSON struct {
	StartNanos  string       `json:"start_nanos"`
	EndNanos    string       `json:"end_nanos"`
	Queries     []tspb.Query `json:"queries"`
	SampleNanos string       `json:"sample_nanos"`
}

// timeSeriesDatapointJSON is a JSON-friendly wrapper for TimeSeriesDatapoint
type timeSeriesDatapointJSON struct {
	TimestampNanos string  `json:"timestampNanos"`
	Value          float64 `json:"value"`
}

// timeSeriesQueryResponseJSON is a JSON-friendly wrapper for TimeSeriesQueryResponse
type timeSeriesQueryResponseJSON struct {
	Results []struct {
		Query      tspb.Query                `json:"query"`
		Datapoints []timeSeriesDatapointJSON `json:"datapoints"`
		Sources    []string                  `json:"sources,omitempty"`
	} `json:"results"`
}

// handleTSQuery handles timeseries query requests
func handleTSQuery(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse JSON into wrapper struct
	var reqJSON timeSeriesQueryRequestJSON
	if err := json.Unmarshal(body, &reqJSON); err != nil {
		log.Dev.Warningf(r.Context(), "Failed to parse timeseries query request: %v", err)
		http.Error(w, fmt.Sprintf("Invalid request format: %v", err), http.StatusBadRequest)
		return
	}

	// Convert to protobuf request
	req := tspb.TimeSeriesQueryRequest{
		Queries: reqJSON.Queries,
	}

	// Parse nanosecond timestamps from strings
	if reqJSON.StartNanos != "" {
		startNanos, err := strconv.ParseInt(reqJSON.StartNanos, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid start_nanos: %v", err), http.StatusBadRequest)
			return
		}
		req.StartNanos = startNanos
	}

	if reqJSON.EndNanos != "" {
		endNanos, err := strconv.ParseInt(reqJSON.EndNanos, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid end_nanos: %v", err), http.StatusBadRequest)
			return
		}
		req.EndNanos = endNanos
	}

	if reqJSON.SampleNanos != "" {
		sampleNanos, err := strconv.ParseInt(reqJSON.SampleNanos, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid sample_nanos: %v", err), http.StatusBadRequest)
			return
		}
		req.SampleNanos = sampleNanos
	}

	// Call the gRPC timeseries query endpoint
	resp, err := cfg.TS.Query(r.Context(), &req)
	if err != nil {
		log.Dev.Warningf(r.Context(), "Timeseries query failed: %v", err)
		http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert response to JSON-friendly format
	respJSON := timeSeriesQueryResponseJSON{
		Results: make([]struct {
			Query      tspb.Query                `json:"query"`
			Datapoints []timeSeriesDatapointJSON `json:"datapoints"`
			Sources    []string                  `json:"sources,omitempty"`
		}, len(resp.Results)),
	}

	for i, result := range resp.Results {
		respJSON.Results[i].Query = result.Query
		respJSON.Results[i].Sources = result.Sources
		respJSON.Results[i].Datapoints = make([]timeSeriesDatapointJSON, len(result.Datapoints))
		for j, dp := range result.Datapoints {
			respJSON.Results[i].Datapoints[j] = timeSeriesDatapointJSON{
				TimestampNanos: strconv.FormatInt(dp.TimestampNanos, 10),
				Value:          dp.Value,
			}
		}
	}

	// Marshal response to JSON
	respBytes, err := json.Marshal(respJSON)
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to marshal timeseries response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(respBytes)
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

// handleMetricsSummary serves the summary table partial with real-time metrics
func handleMetricsSummary(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Fetch nodes for capacity information
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch nodes: %v", err)
		http.Error(w, "Failed to fetch nodes", http.StatusInternalServerError)
		return
	}

	// Calculate capacity totals
	var capacityUsed, capacityTotal int64
	var unavailableRanges int
	totalNodes := len(nodesResp.Nodes)

	for _, node := range nodesResp.Nodes {
		for _, store := range node.StoreStatuses {
			capacityUsed += store.Desc.Capacity.Used
			capacityTotal += store.Desc.Capacity.Capacity
		}
	}

	// Fetch problem ranges for unavailable count
	problemRangesResp, err := cfg.Status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch problem ranges: %v", err)
	} else {
		for _, nodeProblems := range problemRangesResp.ProblemsByNodeID {
			unavailableRanges += len(nodeProblems.UnavailableRangeIDs)
		}
	}

	// Query timeseries for queries per second (cr.node.sql.crud_query.count)
	// and P99 latency (cr.node.sql.service.latency-p99)
	now := timeutil.Now()
	endNanos := now.UnixNano()
	startNanos := now.Add(-10 * time.Second).UnixNano()

	// Build queries for QPS and P99 latency
	var nodeIDs []string
	for _, node := range nodesResp.Nodes {
		nodeIDs = append(nodeIDs, fmt.Sprintf("%d", node.Desc.NodeID))
	}

	tsReq := &tspb.TimeSeriesQueryRequest{
		StartNanos: startNanos,
		EndNanos:   endNanos,
		Queries: []tspb.Query{
			{
				Name:             "cr.node.sql.query.count",
				Sources:          nodeIDs,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
			},
			{
				Name:             "cr.node.sql.service.latency-p99",
				Sources:          nodeIDs,
				Downsampler:      tspb.TimeSeriesQueryAggregator_MAX.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
			},
		},
	}

	tsResp, err := cfg.TS.Query(ctx, tsReq)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to query timeseries: %v", err)
	}

	// Extract QPS and P99 latency from timeseries response
	var qps, p99Latency float64
	if tsResp != nil && len(tsResp.Results) >= 2 {
		// QPS - get the latest datapoint
		if len(tsResp.Results[0].Datapoints) > 0 {
			lastDP := tsResp.Results[0].Datapoints[len(tsResp.Results[0].Datapoints)-1]
			qps = lastDP.Value
		}
		// P99 Latency - get the latest datapoint
		if len(tsResp.Results[1].Datapoints) > 0 {
			lastDP := tsResp.Results[1].Datapoints[len(tsResp.Results[1].Datapoints)-1]
			p99Latency = lastDP.Value
		}
	}

	// Build summary data
	summaryData := MetricsSummaryData{
		TotalNodes:        totalNodes,
		CapacityUsedStr:   formatBytes(float64(capacityUsed)),
		CapacityTotalStr:  formatBytes(float64(capacityTotal)),
		CapacityPercent:   float64(capacityUsed) / float64(capacityTotal),
		UnavailableRanges: unavailableRanges,
		QueriesPerSecond:  formatNumber(qps),
		P99Latency:        p99Latency,
	}

	// Execute the summary partial template
	err = templates.ExecuteTemplate(w, "summary_table.html", summaryData)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleMetricsEvents serves the events list partial with the last 5 events
func handleMetricsEvents(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Fetch events from the Events RPC
	eventsResp, err := cfg.Admin.Events(ctx, &serverpb.EventsRequest{
		Limit: 5,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch events: %v", err)
		http.Error(w, "Failed to fetch events", http.StatusInternalServerError)
		return
	}

	// Convert events to our format
	var events []EventData
	for _, event := range eventsResp.Events {
		// Parse the event info JSON to extract meaningful description
		var eventInfo map[string]interface{}
		description := event.EventType

		if err := json.Unmarshal([]byte(event.Info), &eventInfo); err == nil {
			// Try to build a more readable description based on event type
			switch event.EventType {
			case "set_zone_config":
				if target, ok := eventInfo["Target"].(string); ok {
					description = "Zone configuration updated for " + target
				}
			case "create_index":
				if indexName, ok := eventInfo["IndexName"].(string); ok {
					if tableName, ok := eventInfo["TableName"].(string); ok {
						description = "Created index " + indexName + " on " + tableName
					}
				}
			case "finish_schema_change":
				description = "Schema change completed"
			case "drop_index":
				if indexName, ok := eventInfo["IndexName"].(string); ok {
					description = "Dropped index " + indexName
				}
			case "create_table":
				if tableName, ok := eventInfo["TableName"].(string); ok {
					description = "Created table " + tableName
				}
			case "drop_table":
				if tableName, ok := eventInfo["TableName"].(string); ok {
					description = "Dropped table " + tableName
				}
			default:
				// For unknown event types, just use the event type
				description = event.EventType
			}
		}

		events = append(events, EventData{
			Description: description,
			Timestamp:   float64(event.Timestamp.UnixNano()),
		})
	}

	// Execute the events partial template
	err = templates.ExecuteTemplate(w, "events_list.html", struct {
		Events []EventData
	}{
		Events: events,
	})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// DualClusterOverviewData contains data for the dual-cluster magic overview page
type DualClusterOverviewData struct {
	// Empty for now - the actual data is loaded via HTMX
}

// handleOverviewMagic serves the dual-cluster overview page
func handleOverviewMagic(
	w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs, commonData CommonData,
) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	data := DualClusterOverviewData{}

	// Execute the template
	err := templates.ExecuteTemplate(w, "overview_magic.html", PageData{
		IndexHTMLArgs: cfg,
		Common:        commonData,
		Data:          data,
	})
	if err != nil {
		log.Dev.Warningf(r.Context(), "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// handleOverviewMagicSummary serves the summary table partial for either cluster 1 or 2
func handleOverviewMagicSummary(w http.ResponseWriter, r *http.Request, cfg IndexHTMLArgs) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Determine which cluster to fetch data for
	clusterParam := r.URL.Query().Get("cluster")

	var summaryData MetricsSummaryData
	var err error

	if clusterParam == "2" {
		// Fetch data from the second cluster (localhost:36257 for gRPC)
		summaryData, err = fetchCluster2Summary(ctx)
		if err != nil {
			log.Dev.Warningf(ctx, "Failed to fetch cluster 2 summary: %v", err)
			// Render error message
			_, _ = w.Write([]byte(`<div class="box bad"><p>Failed to fetch cluster 2 data: ` + err.Error() + `</p></div>`))
			return
		}
	} else {
		// Fetch data from the primary cluster (cluster 1) using the existing config
		summaryData, err = fetchCluster1Summary(ctx, cfg)
		if err != nil {
			log.Dev.Warningf(ctx, "Failed to fetch cluster 1 summary: %v", err)
			// Render error message
			_, _ = w.Write([]byte(`<div class="box bad"><p>Failed to fetch cluster 1 data: ` + err.Error() + `</p></div>`))
			return
		}
	}

	// Execute the summary table template
	err = templates.ExecuteTemplate(w, "summary_table.html", summaryData)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to execute template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}
}

// fetchCluster1Summary fetches summary metrics from the primary cluster
func fetchCluster1Summary(ctx context.Context, cfg IndexHTMLArgs) (MetricsSummaryData, error) {
	// Fetch nodes for capacity information
	nodesResp, err := cfg.Status.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return MetricsSummaryData{}, fmt.Errorf("failed to fetch nodes: %w", err)
	}

	// Calculate capacity totals
	var capacityUsed, capacityTotal int64
	var unavailableRanges int
	totalNodes := len(nodesResp.Nodes)

	for _, node := range nodesResp.Nodes {
		for _, store := range node.StoreStatuses {
			capacityUsed += store.Desc.Capacity.Used
			capacityTotal += store.Desc.Capacity.Capacity
		}
	}

	// Fetch problem ranges for unavailable count
	problemRangesResp, err := cfg.Status.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch problem ranges: %v", err)
	} else {
		for _, nodeProblems := range problemRangesResp.ProblemsByNodeID {
			unavailableRanges += len(nodeProblems.UnavailableRangeIDs)
		}
	}

	// Query timeseries for queries per second and P99 latency
	now := timeutil.Now()
	endNanos := now.UnixNano()
	startNanos := now.Add(-10 * time.Second).UnixNano()

	// Build queries for QPS and P99 latency
	var nodeIDs []string
	for _, node := range nodesResp.Nodes {
		nodeIDs = append(nodeIDs, fmt.Sprintf("%d", node.Desc.NodeID))
	}

	tsReq := &tspb.TimeSeriesQueryRequest{
		StartNanos: startNanos,
		EndNanos:   endNanos,
		Queries: []tspb.Query{
			{
				Name:             "cr.node.sql.query.count",
				Sources:          nodeIDs,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
			},
			{
				Name:             "cr.node.sql.service.latency-p99",
				Sources:          nodeIDs,
				Downsampler:      tspb.TimeSeriesQueryAggregator_MAX.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
			},
		},
	}

	tsResp, err := cfg.TS.Query(ctx, tsReq)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to query timeseries: %v", err)
	}

	// Extract QPS and P99 latency from timeseries response
	var qps, p99Latency float64
	if tsResp != nil && len(tsResp.Results) >= 2 {
		// QPS - get the latest datapoint
		if len(tsResp.Results[0].Datapoints) > 0 {
			lastDP := tsResp.Results[0].Datapoints[len(tsResp.Results[0].Datapoints)-1]
			qps = lastDP.Value
		}
		// P99 Latency - get the latest datapoint
		if len(tsResp.Results[1].Datapoints) > 0 {
			lastDP := tsResp.Results[1].Datapoints[len(tsResp.Results[1].Datapoints)-1]
			p99Latency = lastDP.Value
		}
	}

	return MetricsSummaryData{
		TotalNodes:        totalNodes,
		CapacityUsedStr:   formatBytes(float64(capacityUsed)),
		CapacityTotalStr:  formatBytes(float64(capacityTotal)),
		CapacityPercent:   float64(capacityUsed) / float64(capacityTotal),
		UnavailableRanges: unavailableRanges,
		QueriesPerSecond:  formatNumber(qps),
		P99Latency:        p99Latency,
	}, nil
}

// fetchCluster2Summary fetches summary metrics from the second cluster
func fetchCluster2Summary(ctx context.Context) (MetricsSummaryData, error) {
	// Connect to the second cluster at localhost:36257
	adminClient, statusClient, tsClient, err := DialRemoteClients(ctx, "localhost:36257")
	if err != nil {
		return MetricsSummaryData{}, fmt.Errorf("failed to dial cluster 2: %w", err)
	}

	// Fetch nodes for capacity information
	nodesResp, err := statusClient.NodesUI(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return MetricsSummaryData{}, fmt.Errorf("failed to fetch nodes: %w", err)
	}

	// Calculate capacity totals
	var capacityUsed, capacityTotal int64
	var unavailableRanges int
	totalNodes := len(nodesResp.Nodes)

	for _, node := range nodesResp.Nodes {
		for _, store := range node.StoreStatuses {
			capacityUsed += store.Desc.Capacity.Used
			capacityTotal += store.Desc.Capacity.Capacity
		}
	}

	// Fetch problem ranges for unavailable count
	problemRangesResp, err := statusClient.ProblemRanges(ctx, &serverpb.ProblemRangesRequest{})
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to fetch problem ranges: %v", err)
	} else {
		for _, nodeProblems := range problemRangesResp.ProblemsByNodeID {
			unavailableRanges += len(nodeProblems.UnavailableRangeIDs)
		}
	}

	// Query timeseries for queries per second and P99 latency
	now := timeutil.Now()
	endNanos := now.UnixNano()
	startNanos := now.Add(-10 * time.Second).UnixNano()

	// Build queries for QPS and P99 latency
	var nodeIDs []string
	for _, node := range nodesResp.Nodes {
		nodeIDs = append(nodeIDs, fmt.Sprintf("%d", node.Desc.NodeID))
	}

	tsReq := &tspb.TimeSeriesQueryRequest{
		StartNanos: startNanos,
		EndNanos:   endNanos,
		Queries: []tspb.Query{
			{
				Name:             "cr.node.sql.query.count",
				Sources:          nodeIDs,
				Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
				Derivative:       tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE.Enum(),
			},
			{
				Name:             "cr.node.sql.service.latency-p99",
				Sources:          nodeIDs,
				Downsampler:      tspb.TimeSeriesQueryAggregator_MAX.Enum(),
				SourceAggregator: tspb.TimeSeriesQueryAggregator_MAX.Enum(),
			},
		},
	}

	tsResp, err := tsClient.Query(ctx, tsReq)
	if err != nil {
		log.Dev.Warningf(ctx, "Failed to query timeseries: %v", err)
	}

	// Extract QPS and P99 latency from timeseries response
	var qps, p99Latency float64
	if tsResp != nil && len(tsResp.Results) >= 2 {
		// QPS - get the latest datapoint
		if len(tsResp.Results[0].Datapoints) > 0 {
			lastDP := tsResp.Results[0].Datapoints[len(tsResp.Results[0].Datapoints)-1]
			qps = lastDP.Value
		}
		// P99 Latency - get the latest datapoint
		if len(tsResp.Results[1].Datapoints) > 0 {
			lastDP := tsResp.Results[1].Datapoints[len(tsResp.Results[1].Datapoints)-1]
			p99Latency = lastDP.Value
		}
	}

	// Suppress unused variable warning
	_ = adminClient

	return MetricsSummaryData{
		TotalNodes:        totalNodes,
		CapacityUsedStr:   formatBytes(float64(capacityUsed)),
		CapacityTotalStr:  formatBytes(float64(capacityTotal)),
		CapacityPercent:   float64(capacityUsed) / float64(capacityTotal),
		UnavailableRanges: unavailableRanges,
		QueriesPerSecond:  formatNumber(qps),
		P99Latency:        p99Latency,
	}, nil
}
