// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React, { useEffect, useState } from "react";
import { Helmet } from "react-helmet";

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
import "./debug.styl";
import { connect } from "react-redux";
import { AdminUIState } from "src/redux/state";
import { nodeIDsSelector } from "src/redux/nodes";
import { refreshNodes, refreshUserSQLRoles } from "src/redux/apiReducers";
import { selectHasViewActivityRedactedRole } from "src/redux/user";

const COMMUNITY_URL = "https://www.cockroachlabs.com/community/";

const NODE_ID = getDataFromServer().NodeID;

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
}) {
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
        {_.isNil(props.note) ? urlWithParams : props.note}
      </td>
    </tr>
  );
}

function DebugTableRow(props: { title: string; children?: React.ReactNode }) {
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

function DebugPanelLink(props: { name: string; url: string; note: string }) {
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
        return <option value={n}>{n}</option>;
      })}
    </select>
  );
}

const NodeIDSelectorConnected = connect(
  (state: AdminUIState) => {
    return {
      nodeIDs: nodeIDsSelector(state),
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
  const currentNodeIDCookie: string[] = document.cookie
    .split(";")
    .map(cookieString => {
      return cookieString.split("=").map(kv => {
        return kv.trim();
      });
    })
    .find(cookie => {
      return cookie[0] === remoteNodeIDCookieName;
    });
  const setNodeIDCookie = (nodeID: string) => {
    document.cookie = `${remoteNodeIDCookieName}=${nodeID}`;
    location.reload();
  };
  let currentNodeID = props.nodeID;
  let proxyEnabled = false;
  if (
    currentNodeIDCookie &&
    currentNodeIDCookie.length == 2 &&
    currentNodeIDCookie[1] !== ""
  ) {
    currentNodeID = currentNodeIDCookie[1];
    if (currentNodeID !== "local" && currentNodeID !== "") {
      proxyEnabled = true;
    }
  }
  return proxyEnabled ? (
    <span>
      Proxied to {currentNodeID}{" "}
      <button
        onClick={() =>
          setNodeIDCookie(";expires=Thu, 01 Jan 1970 00:00:01 GMT")
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
        name="Statement Diagnostics History"
        url="#/reports/statements/diagnosticshistory"
        note="View the history of statement diagnostics requests"
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

export default function Debug() {
  const [nodeID, setNodeID] = useState<string>(NODE_ID);
  return (
    <div className="section">
      <Helmet title="Debug" />
      <h3 className="base-heading">Advanced Debugging</h3>
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
        />
        <DebugPanelLink
          name="Data Distribution and Zone Configs"
          url="#/data-distribution"
          note="View the distribution of table data across nodes and verify zone configuration."
        />
        <StatementDiagnosticsConnected />
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
        </DebugTableRow>
        <DebugTableRow title="Stores">
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
        <DebugTableRow title="Problem Ranges">
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
          <DebugTableLink name="Raft Messages" url="#/raft/messages/all" />
          <DebugTableLink name="Raft for all ranges" url="#/raft/ranges" />
        </DebugTableRow>
        <DebugTableRow title="Closed timestamps">
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
            url="debug/pprof/ui/heap/"
            params={{ node: nodeID, seconds: "5", si: "alloc_objects" }}
          />
          <DebugTableLink
            name="CPU Profile"
            url="debug/pprof/ui/cpu/"
            params={{ node: nodeID, seconds: "5", labels: "true" }}
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
          <DebugTableLink name="Active operations" url="debug/tracez" />
          <DebugTableLink name="Requests" url="debug/requests" />
          <DebugTableLink name="Events" url="debug/events" />
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
        <DebugTableRow title="Enqueue Range">
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
          <DebugTableLink name="Rules" url="api/v2/rules/" />
        </DebugTableRow>
        <DebugTableRow title="Node Status">
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
          />
        </DebugTableRow>
        <DebugTableRow title="Hot Ranges (legacy)">
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
        <DebugTableRow title="Single Node Specific">
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
        <DebugTableRow title="Cluster Wide">
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
        <DebugTableRow title="Allocator">
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
