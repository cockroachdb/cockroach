// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import isNil from "lodash/isNil";
import React, { useEffect, useState } from "react";
import { Helmet } from "react-helmet";
import { connect, useSelector } from "react-redux";
import { InlineAlert } from "src/components";
import { history } from "src/redux/history";
import { refreshNodes, refreshUserSQLRoles } from "src/redux/apiReducers";
import { getCookieValue, setCookie } from "src/redux/cookies";
import { nodeIDsStringifiedSelector } from "src/redux/nodes";
import { AdminUIState, featureFlagSelector } from "src/redux/state";
import { selectHasViewActivityRedactedRole } from "src/redux/user";
import { getDataFromServer } from "src/util/dataFromServer";
import DebugAnnotation from "src/views/shared/components/debugAnnotation";
import InfoBox from "src/views/shared/components/infoBox";
import LicenseType from "src/views/shared/components/licenseType";
import {
  PanelSection,
  PanelTitle,
  PanelPair,
  Panel,
} from "src/views/shared/components/panelSection";

import "./debug.scss";

const COMMUNITY_URL = "https://www.cockroachlabs.com/community/";

export function DebugTableLink(props: {
  name: string;
  url: string;
  note?: string;
  params?: {
    node?: string;
    seconds?: string;
    si?: string;
    labels?: string;
  };
  disabled?: boolean;
}) {
  if (props.disabled) {
    return null;
  }

  const params = new URLSearchParams(props.params);
  const urlWithParams = props.params
    ? `${props.url}?${params.toString()}`
    : props.url;
  return (
    <tr className="debug-inner-table__row">
      <td className="debug-inner-table__cell">
        <a className="debug-link" href={urlWithParams}>
          {props.name}
        </a>
      </td>
      <td className="debug-inner-table__cell--notes">
        {isNil(props.note) ? urlWithParams : props.note}
      </td>
    </tr>
  );
}

function DebugTableRow(props: {
  title: string;
  children?: React.ReactNode;
  disabled?: boolean;
}) {
  if (props.disabled) {
    return null;
  }
  return (
    <tr className="debug-table__row">
      <th className="debug-table__cell debug-table__cell--header">
        {props.title}
      </th>
      <td className="debug-table__cell">
        <table className="debug-inner-table">
          <tbody>{props.children}</tbody>
        </table>
      </td>
    </tr>
  );
}

function DebugTable(props: {
  heading: string | React.ReactNode;
  children?: React.ReactNode;
}) {
  return (
    <div>
      <h2 className="base-heading">{props.heading}</h2>
      <table className="debug-table">
        <tbody>{props.children}</tbody>
      </table>
    </div>
  );
}

function DebugPanelLink(props: {
  name: string;
  url: string;
  note: string;
  disabled?: boolean;
}) {
  if (props.disabled) {
    return null;
  }
  return (
    <PanelPair>
      <Panel>
        <a href={props.url}>{props.name}</a>
        <p>{props.note}</p>
      </Panel>
      <Panel>
        <div className="debug-url">
          <div>{props.url}</div>
        </div>
      </Panel>
    </PanelPair>
  );
}

// NodeIDSelector is a standalone component that displays a list of nodeIDs and allows for
// their selection using a standard `<select>` component. If this component is used outside
// of the Advanced Debug page, it should be styled appropriately. In order to make use of
// this component and its "connected" version below (which retrieves and manages the nodeIDs
// in the cluster automatically via redux) you will need to pass it the selected nodeID and
// a function for mutating the nodeID (typical outputs of the `setState` hook) as props.
function NodeIDSelector(props: {
  nodeID: string;
  setNodeID: (nodeID: string) => void;
  nodeIDs: string[];
  refreshNodes: typeof refreshNodes;
}) {
  const { nodeID, setNodeID, nodeIDs, refreshNodes } = props;

  useEffect(() => {
    refreshNodes();
  }, [props, refreshNodes]);

  return (
    <select
      value={nodeID}
      onChange={e => {
        setNodeID(e.target.value);
      }}
    >
      {nodeIDs.map(n => {
        return (
          <option value={n} key={n}>
            {n}
          </option>
        );
      })}
    </select>
  );
}

const NodeIDSelectorConnected = connect(
  (state: AdminUIState) => {
    return {
      nodeIDs: nodeIDsStringifiedSelector(state),
    };
  },
  {
    refreshNodes,
  },
)(NodeIDSelector);

interface ProxyToNodeSelectorProps {
  nodeID: string;
}

// ProxyToNodeSelector is a dropdown that allows the user to select a
// remote nodeID to proxy their DB Console connection to. When a nodeID
// is selected from the dropdown a cookie is set in the browser that
// will instruct CRDB to proxy HTTP requests to that nodeID. Selecting
// "local" will reset the connection back to this specific node.
const ProxyToNodeSelector = (props: ProxyToNodeSelectorProps) => {
  const remoteNodeIDCookieName = "remote_node_id";

  // currentNodeIDCookie will either be empty or contain two elements
  // with the cookie name and value we're looking for
  const currentNodeIDCookie: string = getCookieValue(remoteNodeIDCookieName);
  const setNodeIDCookie = (nodeID: string) => {
    setCookie(remoteNodeIDCookieName, nodeID);
    location.reload();
  };
  let currentNodeID = props.nodeID;
  let proxyEnabled = false;
  if (currentNodeIDCookie) {
    currentNodeID = currentNodeIDCookie;
    if (currentNodeID !== "local" && currentNodeID !== "") {
      proxyEnabled = true;
    }
  }
  return proxyEnabled ? (
    <span>
      Proxied to {currentNodeID}{" "}
      <button
        onClick={() =>
          setNodeIDCookie(";expires=Thu, 01 Jan 1970 00:00:01 GMT;path=/")
        }
      >
        Reset
      </button>
    </span>
  ) : (
    <NodeIDSelectorConnected
      nodeID={currentNodeID}
      setNodeID={setNodeIDCookie}
    />
  );
};

function StatementDiagnosticsSelector(props: {
  canSeeDebugPanelLink: boolean;
  refreshUserSQLRoles: typeof refreshUserSQLRoles;
}) {
  const { canSeeDebugPanelLink, refreshUserSQLRoles } = props;

  useEffect(() => {
    refreshUserSQLRoles();
  }, [refreshUserSQLRoles]);

  return (
    canSeeDebugPanelLink && (
      <DebugPanelLink
        name="Diagnostics History"
        url="#/reports/diagnosticshistory"
        note="View the history of statement and transaction diagnostics requests"
      />
    )
  );
}

const StatementDiagnosticsConnected = connect(
  (state: AdminUIState) => {
    return {
      canSeeDebugPanelLink: !selectHasViewActivityRedactedRole(state),
    };
  },
  {
    refreshUserSQLRoles,
  },
)(StatementDiagnosticsSelector);

interface UploadJobStatus {
  job_id: string;
  status: string;
  session_id: string;
  total_nodes: number;
  nodes_completed: number;
  nodes_failed: number;
  artifacts_uploaded: number;
  failed_node_ids: number[];
  errors: string[];
  fraction_completed: number;
  running_status: string;
  error: string;
  server_url: string;
  redact: boolean;
}

const inputStyle: React.CSSProperties = { padding: "4px 8px", fontSize: "13px" };
const btnStyle: React.CSSProperties = { padding: "4px 12px", fontSize: "13px", alignSelf: "flex-start" };
const errStyle: React.CSSProperties = { color: "red", fontSize: "12px" };
const labelStyle: React.CSSProperties = { fontSize: "12px", fontWeight: 600, marginTop: "4px" };

function UploadDebugDataPanel() {
  const [serverUrl, setServerUrl] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [reuploadSessionId, setReuploadSessionId] = useState("");
  const [nodeIds, setNodeIds] = useState("");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [checkJobId, setCheckJobId] = useState("");
  const [jobStatus, setJobStatus] = useState<UploadJobStatus | null>(null);
  const [statusLoading, setStatusLoading] = useState(false);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [retrying, setRetrying] = useState(false);

  const startUpload = async (
    url: string,
    key: string,
    sessionId?: string,
    nodes?: number[],
  ) => {
    const body: Record<string, unknown> = {
      server_url: url,
      api_key: key,
      redact: false,
      include_goroutine_stacks: true,
    };
    if (sessionId) {
      body.reupload_session_id = sessionId;
    }
    if (nodes && nodes.length > 0) {
      body.node_ids = nodes;
    }
    const resp = await fetch("/_admin/v1/upload_debug_data", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`HTTP ${resp.status}: ${text}`);
    }
    const data = await resp.json();
    return data.job_id || data.jobId || data.JobID;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const parsedNodes = nodeIds
        .split(",")
        .map(s => s.trim())
        .filter(s => s !== "")
        .map(Number)
        .filter(n => !isNaN(n));
      const jobId = await startUpload(
        serverUrl,
        apiKey,
        reuploadSessionId || undefined,
        parsedNodes.length > 0 ? parsedNodes : undefined,
      );
      if (jobId) {
        history.push(`/jobs/${jobId}`);
      } else {
        setError("No job ID returned");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  const handleCheckStatus = async () => {
    if (!checkJobId) return;
    setStatusLoading(true);
    setStatusError(null);
    setJobStatus(null);
    try {
      const resp = await fetch(
        `/_admin/v1/upload_debug_data/status/${checkJobId}`,
      );
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${text}`);
      }
      const data = await resp.json();
      setJobStatus(data);
    } catch (err) {
      setStatusError(err instanceof Error ? err.message : String(err));
    } finally {
      setStatusLoading(false);
    }
  };

  const handleRetryFailed = async () => {
    if (!jobStatus || !apiKey) return;
    setRetrying(true);
    setStatusError(null);
    try {
      const jobId = await startUpload(
        jobStatus.server_url,
        apiKey,
        jobStatus.session_id,
        jobStatus.failed_node_ids,
      );
      if (jobId) {
        history.push(`/jobs/${jobId}`);
      }
    } catch (err) {
      setStatusError(err instanceof Error ? err.message : String(err));
    } finally {
      setRetrying(false);
    }
  };

  return (
    <>
      <PanelPair>
        <Panel>
          <a>Upload Debug Data to Support</a>
          <p>
            Stream debug data from all nodes directly to a CRL upload server.
            Progress is tracked as a background job.
          </p>
        </Panel>
        <Panel>
          <form
            onSubmit={handleSubmit}
            style={{ display: "flex", flexDirection: "column", gap: "6px" }}
          >
            <input
              type="text"
              placeholder="Upload server URL"
              value={serverUrl}
              onChange={e => setServerUrl(e.target.value)}
              required
              style={inputStyle}
            />
            <input
              type="password"
              placeholder="API key"
              value={apiKey}
              onChange={e => setApiKey(e.target.value)}
              required
              style={inputStyle}
            />
            <a
              onClick={() => setShowAdvanced(!showAdvanced)}
              style={{ fontSize: "12px", cursor: "pointer", color: "#0055ff" }}
            >
              {showAdvanced ? "▾ Hide advanced options" : "▸ Advanced options"}
            </a>
            {showAdvanced && (
              <>
                <label style={labelStyle}>Reupload Session ID (for retry)</label>
                <input
                  type="text"
                  placeholder="e.g. sess_abc123"
                  value={reuploadSessionId}
                  onChange={e => setReuploadSessionId(e.target.value)}
                  style={inputStyle}
                />
                <label style={labelStyle}>Node IDs (comma-separated, blank = all)</label>
                <input
                  type="text"
                  placeholder="e.g. 1,3,5"
                  value={nodeIds}
                  onChange={e => setNodeIds(e.target.value)}
                  style={inputStyle}
                />
              </>
            )}
            <button
              type="submit"
              disabled={submitting || !serverUrl || !apiKey}
              style={btnStyle}
            >
              {submitting ? "Starting..." : "Start Upload"}
            </button>
            {error && <div style={errStyle}>{error}</div>}
          </form>
        </Panel>
      </PanelPair>

      <PanelPair>
        <Panel>
          <a>Check Upload Status</a>
          <p>
            Enter a job ID to view upload progress, per-node errors, and retry
            failed nodes.
          </p>
        </Panel>
        <Panel>
          <div style={{ display: "flex", flexDirection: "column", gap: "6px" }}>
            <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
              <input
                type="text"
                placeholder="Job ID"
                value={checkJobId}
                onChange={e => setCheckJobId(e.target.value)}
                style={{ ...inputStyle, flex: 1 }}
              />
              <button
                onClick={handleCheckStatus}
                disabled={statusLoading || !checkJobId}
                style={btnStyle}
              >
                {statusLoading ? "Loading..." : "Check Status"}
              </button>
            </div>
            {statusError && <div style={errStyle}>{statusError}</div>}
            {jobStatus && (
              <div
                style={{
                  fontSize: "12px",
                  border: "1px solid #ddd",
                  borderRadius: "4px",
                  padding: "8px",
                  marginTop: "4px",
                }}
              >
                <div>
                  <strong>Status:</strong> {jobStatus.status}
                  {jobStatus.fraction_completed > 0 &&
                    ` (${(jobStatus.fraction_completed * 100).toFixed(0)}%)`}
                </div>
                {jobStatus.session_id && (
                  <div><strong>Session:</strong> {jobStatus.session_id}</div>
                )}
                {jobStatus.running_status && (
                  <div><strong>Progress:</strong> {jobStatus.running_status}</div>
                )}
                <div>
                  <strong>Nodes:</strong>{" "}
                  {jobStatus.nodes_completed}/{jobStatus.total_nodes} completed
                  {jobStatus.nodes_failed > 0 && (
                    <span style={{ color: "red" }}>
                      , {jobStatus.nodes_failed} failed
                    </span>
                  )}
                </div>
                <div>
                  <strong>Artifacts uploaded:</strong>{" "}
                  {jobStatus.artifacts_uploaded}
                </div>
                {jobStatus.error && (
                  <div style={{ color: "red", marginTop: "4px" }}>
                    <strong>Error:</strong> {jobStatus.error}
                  </div>
                )}
                {jobStatus.failed_node_ids &&
                  jobStatus.failed_node_ids.length > 0 && (
                    <div style={{ marginTop: "4px" }}>
                      <strong>Failed nodes:</strong>{" "}
                      {jobStatus.failed_node_ids.join(", ")}
                    </div>
                  )}
                {jobStatus.errors && jobStatus.errors.length > 0 && (
                  <div style={{ marginTop: "4px" }}>
                    <strong>Per-node errors:</strong>
                    <ul style={{ margin: "4px 0 0 16px", padding: 0 }}>
                      {jobStatus.errors.map((e, i) => (
                        <li key={i} style={{ color: "#c00" }}>{e}</li>
                      ))}
                    </ul>
                  </div>
                )}
                {jobStatus.failed_node_ids &&
                  jobStatus.failed_node_ids.length > 0 && (
                    <div style={{ marginTop: "8px" }}>
                      {!apiKey && (
                        <div style={{ fontSize: "11px", color: "#666", marginBottom: "4px" }}>
                          Enter your API key above to enable retry.
                        </div>
                      )}
                      <button
                        onClick={handleRetryFailed}
                        disabled={retrying || !apiKey}
                        style={{
                          ...btnStyle,
                          backgroundColor: "#c00",
                          color: "#fff",
                          border: "none",
                          borderRadius: "3px",
                          cursor: apiKey ? "pointer" : "not-allowed",
                        }}
                      >
                        {retrying
                          ? "Retrying..."
                          : `Retry Failed Nodes (${jobStatus.failed_node_ids.join(", ")})`}
                      </button>
                    </div>
                  )}
              </div>
            )}
          </div>
        </Panel>
      </PanelPair>
    </>
  );
}

export default function Debug() {
  const [nodeID, setNodeID] = useState<string>(getDataFromServer().NodeID);

  const { disable_kv_level_advanced_debug } = useSelector(featureFlagSelector);

  return (
    <div className="section">
      <Helmet title="Debug" />
      <h3 className="base-heading">Advanced Debug</h3>
      {disable_kv_level_advanced_debug && (
        <section className="section">
          <InlineAlert
            title="Some advanced debug options are only available via the system interface."
            intent="warning"
          />
        </section>
      )}
      <div className="debug-header">
        <InfoBox>
          <p>
            The following pages are meant for advanced monitoring and
            troubleshooting. Note that these pages are experimental and
            undocumented. If you find an issue, let us know through{" "}
            <a href={COMMUNITY_URL}>these channels.</a>
          </p>
        </InfoBox>

        <div className="debug-header__annotations">
          <LicenseType />
          <DebugAnnotation
            label="Web server"
            value={<ProxyToNodeSelector nodeID={nodeID} />}
          />
        </div>
      </div>
      <PanelSection>
        <PanelTitle>Reports</PanelTitle>
        <DebugPanelLink
          name="Custom Time Series Chart"
          url="#/debug/chart"
          note="Create a custom chart of time series data."
        />
        <DebugPanelLink
          name="Problem Ranges"
          url="#/reports/problemranges"
          note="View ranges in your cluster that are unavailable, underreplicated, slow, or have other problems."
          disabled={disable_kv_level_advanced_debug}
        />
        <DebugPanelLink
          name="Data Distribution and Zone Configs"
          url="#/data-distribution"
          note="View the distribution of table data across nodes and verify zone configuration."
          disabled={disable_kv_level_advanced_debug}
        />
        <StatementDiagnosticsConnected />
        <PanelTitle>Support</PanelTitle>
        <UploadDebugDataPanel />
        <PanelTitle>Configuration</PanelTitle>
        <DebugPanelLink
          name="Cluster Settings"
          url="#/reports/settings"
          note="View all cluster settings."
        />
        <DebugPanelLink
          name="Localities"
          url="#/reports/localities"
          note="Check node localities and locations for your cluster."
        />
      </PanelSection>
      <DebugTable heading="Even More Advanced Debugging">
        <DebugTableRow title="Node Diagnostics">
          <DebugTableLink name="All Nodes" url="#/reports/nodes" />
          <DebugTableLink
            name="Nodes filtered by node IDs"
            url="#/reports/nodes?node_ids=1,2"
            note="#/reports/nodes?node_ids=[node_id{,node_id...}]"
          />
          <DebugTableLink
            name="Nodes filtered by locality (regex)"
            url="#/reports/nodes?locality=region=us-east"
            note="#/reports/nodes?locality=[regex]"
          />
          <DebugTableLink
            name="Decommissioned node history"
            url="#/reports/nodes/history"
            note="#/reports/nodes/history"
          />
          <DebugTableLink
            name="Node activity honeycomb visualizer"
            url="#/cluster-explorer"
            note="#/cluster-explorer"
          />
        </DebugTableRow>
        <DebugTableRow
          title="Stores"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="Stores on this node"
            url="#/reports/stores/local"
          />
          <DebugTableLink
            name="Stores on a specific node"
            url="#/reports/stores/1"
            note="#/reports/stores/[node_id]"
          />
          <DebugTableLink
            name="Store LSM details on this node"
            url="debug/lsm"
            note="debug/lsm"
          />
        </DebugTableRow>
        <DebugTableRow title="Security">
          <DebugTableLink
            name="Certificates on this node"
            url="#/reports/certificates/local"
          />
          <DebugTableLink
            name="Certificates on a specific node"
            url="#/reports/certificates/1"
            note="#/reports/certificates/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow
          title="Problem Ranges"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="Problem Ranges on a specific node"
            url="#/reports/problemranges/local"
            note="#/reports/problemranges/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Ranges">
          <DebugTableLink
            name="Range Status"
            url="#/reports/range/1"
            note="#/reports/range/[range_id]"
          />
          <DebugTableLink
            name="Raft Messages"
            url="#/raft/messages/all"
            disabled={disable_kv_level_advanced_debug}
          />
          <DebugTableLink
            name="Raft for all ranges"
            url="#/raft/ranges"
            disabled={disable_kv_level_advanced_debug}
          />
          <DebugTableLink
            name="Key Visualizer"
            url="#/keyvisualizer"
            disabled={disable_kv_level_advanced_debug}
          />
        </DebugTableRow>
        <DebugTableRow
          title="Closed timestamps"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="Sender on this node"
            url="debug/closedts-sender"
          />
          <DebugTableLink
            name="Receiver on this node"
            url="debug/closedts-receiver"
          />
        </DebugTableRow>
      </DebugTable>
      <DebugTable
        heading={
          <>
            {"Profiling UI (Target node: "}
            <NodeIDSelectorConnected nodeID={nodeID} setNodeID={setNodeID} />
            {")"}
          </>
        }
      >
        <DebugTableRow title="Profiling UI/pprof">
          <DebugTableLink
            name="Heap"
            url="debug/pprof/ui/heap/"
            params={{ node: nodeID }}
          />
          <DebugTableLink
            name="Heap (recent allocs)"
            url="debug/pprof/ui/allocs/"
            params={{ node: nodeID, seconds: "5", si: "alloc_objects" }}
          />
          <DebugTableLink
            name="CPU Profile"
            url="debug/pprof/ui/cpu/"
            params={{ node: nodeID, seconds: "5", labels: "true" }}
          />
          <DebugTableLink
            name="Cluster-wide Combined CPU Profile"
            url="debug/pprof/ui/cpu/"
            params={{ node: "all", seconds: "5", labels: "true" }}
          />
          <DebugTableLink
            name="Block"
            url="debug/pprof/ui/block/"
            params={{ node: nodeID }}
          />
          <DebugTableLink
            name="Mutex"
            url="debug/pprof/ui/mutex/"
            params={{ node: nodeID }}
          />
          <DebugTableLink
            name="Thread Create"
            url="debug/pprof/ui/threadcreate/"
            params={{ node: nodeID }}
          />
          <DebugTableLink
            name="Goroutines"
            url="debug/pprof/ui/goroutine/"
            params={{ node: nodeID }}
          />
        </DebugTableRow>
      </DebugTable>
      <DebugTable heading="Tracing and Profiling Endpoints (local node only)">
        <DebugTableRow title="Tracing">
          <DebugTableLink
            name="Active operations"
            url="#/debug/tracez"
            disabled={disable_kv_level_advanced_debug}
          />
          <DebugTableLink
            name="Requests"
            url="debug/requests"
            disabled={disable_kv_level_advanced_debug}
          />
          <DebugTableLink
            name="Events"
            url="debug/events"
            disabled={disable_kv_level_advanced_debug}
          />
          <DebugTableLink
            name="Logs (JSON)"
            url="debug/logspy?count=100&amp;duration=10s&amp;grep=.&flatten=0"
            note="debug/logspy?count=[count]&amp;duration=[duration]&amp;grep=[regexp]"
          />
          <DebugTableLink
            name="Logs (text)"
            url="debug/logspy?count=100&amp;duration=10s&amp;grep=.&flatten=1"
            note="debug/logspy?count=[count]&amp;duration=[duration]&amp;grep=[regexp]&amp;flatten=1"
          />
          <DebugTableLink
            name="Logs (text, high verbosity; IMPACTS PERFORMANCE)"
            url="debug/logspy?count=100&amp;duration=10s&amp;grep=.&flatten=1&vmodule=*=2"
            note="debug/logspy?count=[count]&amp;duration=[duration]&amp;grep=[regexp]&amp;flatten=[0/1]&amp;vmodule=[vmodule]"
          />
          <DebugTableLink
            name="VModule setting"
            url="debug/vmodule"
            note="debug/vmodule?duration=[duration]&amp;vmodule=[vmodule]"
          />
        </DebugTableRow>
        <DebugTableRow
          title="Enqueue Range"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="Run a range through an internal queue"
            url="#/debug/enqueue_range"
            note="#/debug/enqueue_range"
          />
        </DebugTableRow>
        <DebugTableRow title="Stopper">
          <DebugTableLink name="Active Tasks" url="debug/stopper" />
        </DebugTableRow>
        <DebugTableRow title="Goroutines">
          <DebugTableLink name="UI" url="debug/pprof/goroutineui" />
          <DebugTableLink
            name="UI (count)"
            url="debug/pprof/goroutineui?sort=count"
          />
          <DebugTableLink
            name="UI (wait)"
            url="debug/pprof/goroutineui?sort=wait"
          />
          <DebugTableLink name="Raw" url="debug/pprof/goroutine?debug=2" />
        </DebugTableRow>
        <DebugTableRow title="Runtime Trace">
          <DebugTableLink name="Trace" url="debug/pprof/trace?debug=1" />
        </DebugTableRow>
      </DebugTable>
      <DebugTable heading="Raw Status Endpoints (JSON)">
        <DebugTableRow title="Logs (single node only)">
          <DebugTableLink
            name="On a Specific Node"
            url="_status/logs/local"
            note="_status/logs/[node_id]"
          />
          <DebugTableLink
            name="Log Files"
            url="_status/logfiles/local"
            note="_status/logfiles/[node_id]"
          />
          <DebugTableLink
            name="Specific Log File"
            url="_status/logfiles/local/cockroach.log"
            note="_status/logfiles/[node_id]/[filename]"
          />
        </DebugTableRow>
        <DebugTableRow title="Metrics">
          <DebugTableLink name="Variables" url="debug/metrics" />
          <DebugTableLink name="Prometheus" url="_status/vars" />
          <DebugTableLink name="Load" url="_status/load" />
          <DebugTableLink name="Rules" url="api/v2/rules/" />
        </DebugTableRow>
        <DebugTableRow
          title="Node Status"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="All Nodes"
            url="_status/nodes"
            note="_status/nodes"
          />
          <DebugTableLink
            name="Single node status"
            url="_status/nodes/local"
            note="_status/nodes/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Hot Ranges">
          <DebugTableLink
            name="All Nodes"
            url="#/debug/hotranges"
            note="#/debug/hotranges"
          />
          <DebugTableLink
            name="Single node's ranges"
            url="#/debug/hotranges/local"
            note="#/debug/hotranges/[node_id]"
            disabled={disable_kv_level_advanced_debug}
          />
        </DebugTableRow>
        <DebugTableRow
          title="Hot Ranges (legacy)"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="All Nodes"
            url="_status/hotranges"
            note="_status/hotranges"
          />
          <DebugTableLink
            name="Single node's ranges"
            url="_status/hotranges?node_id=local"
            note="_status/hotranges?node_id=[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow
          title="Single Node Specific"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="Stores"
            url="_status/stores/local"
            note="_status/stores/[node_id]"
          />
          <DebugTableLink
            name="Gossip"
            url="_status/gossip/local"
            note="_status/gossip/[node_id]"
          />
          <DebugTableLink
            name="Ranges"
            url="_status/ranges/local"
            note="_status/ranges/[node_id]"
          />
          <DebugTableLink
            name="Stacks"
            url="_status/stacks/local"
            note="_status/stacks/[node_id]"
          />
          <DebugTableLink
            name="Engine Stats"
            url="_status/enginestats/local"
            note="_status/enginestats/[node_id]"
          />
          <DebugTableLink
            name="Certificates"
            url="_status/certificates/local"
            note="_status/certificates/[node_id]"
          />
          <DebugTableLink
            name="Diagnostics Reporting Data"
            url="_status/diagnostics/local"
            note="_status/diagnostics/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Sessions">
          <DebugTableLink name="Local Sessions" url="_status/local_sessions" />
          <DebugTableLink name="All Sessions" url="_status/sessions" />
        </DebugTableRow>
        <DebugTableRow
          title="Cluster Wide"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink name="Raft" url="_status/raft" />
          <DebugTableLink
            name="Range"
            url="_status/range/1"
            note="_status/range/[range_id]"
          />
          <DebugTableLink name="Range Log" url="_admin/v1/rangelog?limit=100" />
          <DebugTableLink
            name="Range Log for Specific Range"
            url="_admin/v1/rangelog/1?limit=100"
            note="_admin/v1/rangelog/[range_id]?limit=100"
          />
        </DebugTableRow>
        <DebugTableRow
          title="Allocator"
          disabled={disable_kv_level_advanced_debug}
        >
          <DebugTableLink
            name="Simulated Allocator Runs on a Specific Node"
            url="_status/allocator/node/local"
            note="_status/allocator/node/[node_id]"
          />
          <DebugTableLink
            name="Simulated Allocator Runs on a Specific Range"
            url="_status/allocator/range/1"
            note="_status/allocator/range/[range_id]"
          />
        </DebugTableRow>
      </DebugTable>
      <DebugTable heading="UI Debugging">
        <DebugTableRow title="Redux State">
          <DebugTableLink
            name="Export the Redux State of the UI"
            url="#/debug/redux"
          />
        </DebugTableRow>
      </DebugTable>
    </div>
  );
}
