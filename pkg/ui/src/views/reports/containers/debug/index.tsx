import _ from "lodash";
import React from "react";

import LicenseType from "src/views/shared/components/licenseType";

function DebugTableLink(props: { name: string, url: string, note?: string }) {
  return (
    <tr className="debug-inner-table__row">
      <td className="debug-inner-table__cell">
        <a className="debug-link" href={props.url}>{props.name}</a>
      </td>
      <td className="debug-inner-table__cell--notes">
        {_.isNil(props.note) ? props.url : props.note}
      </td>
    </tr>
  );
}

function DebugTableRow(props: { title: string, children?: React.ReactNode }) {
  return (
    <tr className="debug-table__row">
      <th className="debug-table__cell debug-table__cell--header">{props.title}</th>
      <td className="debug-table__cell">
        <table className="debug-inner-table">
          <tbody>
            {props.children}
          </tbody>
        </table>
      </td>
    </tr>
  );
}

function DebugTable(props: { heading: string, children?: React.ReactNode }) {
  return (
    <div>
      <h2>{props.heading}</h2>
      <table className="debug-table">
        <tbody>
          {props.children}
        </tbody>
      </table>
    </div>
  );
}

export default function Debug() {
  return (
    <div className="section">
      <h1>Advanced Debugging</h1>
      <DebugTable heading="Reports">
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
        </DebugTableRow>
        <DebugTableRow title="Localities">
          <DebugTableLink name="Locality Tree" url="#/reports/localities" />
        </DebugTableRow>
        <DebugTableRow title="Network">
          <DebugTableLink name="Latency (on all nodes)" url="#/reports/network" />
          <DebugTableLink
            name="Latency filtered by node IDs"
            url="#/reports/network?node_ids=1,2"
            note="#/reports/network?node_ids=[node_id{,node_id...}]"
          />
          <DebugTableLink
            name="Latency filtered by locality (regex)"
            url="#/reports/network?locality=region=us-east"
            note="#/reports/network?locality=[regex]"
          />
        </DebugTableRow>
        <DebugTableRow title="Settings">
          <DebugTableLink name="Cluster Settings" url="#/reports/settings" />
        </DebugTableRow>
        <DebugTableRow title="Security">
          <DebugTableLink name="Certificates on this node" url="#/reports/certificates/local" />
          <DebugTableLink
            name="Certificates on a specific node"
            url="#/reports/certificates/1"
            note="#/reports/certificates/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Problem Ranges">
          <DebugTableLink name="All Problem Ranges" url="#/reports/problemranges" />
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
      </DebugTable>
      <DebugTable heading="Tracing Endpoints (local node only)">
        <DebugTableRow title="Tracing">
          <DebugTableLink name="Requests" url="/debug/requests" />
          <DebugTableLink name="Events" url="/debug/events" />
          <DebugTableLink
            name="Logs"
            url="/debug/logspy?count=1&amp;duration=10s&amp;grep=."
            note="/debug/logspy?count=[count]&amp;duration=[duration]&amp;grep=[regexp]"
          />
        </DebugTableRow>
        <DebugTableRow title="Stopper">
          <DebugTableLink name="Active Tasks" url="/debug/stopper" />
        </DebugTableRow>
        <DebugTableRow title="pprof">
          <DebugTableLink name="Heap (UI)" url="/debug/pprof/ui/heap/" />
          <DebugTableLink name="Heap (raw)" url="/debug/pprof/heap?debug=1" />
          <DebugTableLink name="Profile (UI)" url="/debug/pprof/ui/profile/" />
          <DebugTableLink name="Profile (raw)" url="/debug/pprof/profile?debug=1" />
          <DebugTableLink name="Block (UI)" url="/debug/pprof/ui/block/" />
          <DebugTableLink name="Block (raw)" url="/debug/pprof/block?debug=1" />
          <DebugTableLink name="Thread Create (UI)" url="/debug/pprof/ui/threadcreate/" />
          <DebugTableLink name="Thread Create (raw)" url="/debug/pprof/threadcreate?debug=1" />
          <DebugTableLink name="Goroutines (UI)" url="/debug/pprof/ui/goroutine/" />
          <DebugTableLink name="Goroutines (raw)" url="/debug/pprof/goroutine?debug=1" />
          <DebugTableLink name="All Goroutines (raw)" url="/debug/pprof/goroutine?debug=2" />
          <DebugTableLink name="Trace (raw)" url="/debug/pprof/trace?debug=1" />
        </DebugTableRow>
      </DebugTable>
      <DebugTable heading="Raw Status Endpoints (JSON)">
        <DebugTableRow title="Logs (single node only)">
          <DebugTableLink
            name="On a Specific Node"
            url="/_status/logs/local"
            note="/_status/logs/[node_id]"
          />
          <DebugTableLink
            name="Log Files"
            url="/_status/logfiles/local"
            note="/_status/logfiles/[node_id]"
          />
          <DebugTableLink
            name="Specific Log File"
            url="/_status/logfiles/local/cockroach.log"
            note="/_status/logfiles/[node_id]/[filename]"
          />
        </DebugTableRow>
        <DebugTableRow title="Metrics">
          <DebugTableLink name="Variables" url="/debug/metrics" />
          <DebugTableLink name="Prometheus" url="/_status/vars" />
        </DebugTableRow>
        <DebugTableRow title="Node Status">
          <DebugTableLink
            name="All Nodes"
            url="/_status/nodes"
            note="/_status/nodes"
          />
          <DebugTableLink
            name="Single node status"
            url="/_status/nodes/local"
            note="/_status/nodes/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Single Node Specific">
          <DebugTableLink
            name="Gossip"
            url="/_status/gossip/local"
            note="/_status/gossip/[node_id]"
          />
          <DebugTableLink
            name="Ranges"
            url="/_status/ranges/local"
            note="/_status/ranges/[node_id]"
          />
          <DebugTableLink
            name="Stacks"
            url="/_status/stacks/local"
            note="/_status/stacks/[node_id]"
          />
          <DebugTableLink
            name="Certificates"
            url="/_status/certificates/local"
            note="/_status/certificates/[node_id]"
          />
        </DebugTableRow>
        <DebugTableRow title="Sessions">
          <DebugTableLink name="Local Sessions" url="/_status/local_sessions" />
          <DebugTableLink name="All Sessions" url="/_status/sessions" />
        </DebugTableRow>
        <DebugTableRow title="Cluster Wide">
          <DebugTableLink name="Cluster Settings" url="/_admin/v1/settings" />
          <DebugTableLink name="Raft" url="/_status/raft" />
          <DebugTableLink
            name="Range"
            url="/_status/range/1"
            note="/_status/range/[range_id]"
          />
          <DebugTableLink name="Range Log" url="/_admin/v1/rangelog?limit=100" />
          <DebugTableLink
            name="Range Log for Specific Range"
            url="/_admin/v1/rangelog/1?limit=100"
            note="/_admin/v1/rangelog/[range_id]?limit=100"
          />
        </DebugTableRow>
        <DebugTableRow title="Allocator">
          <DebugTableLink
            name="Simulated Allocator Runs on a Specific Node"
            url="/_status/allocator/node/local"
            note="/_status/allocator/node/[node_id]"
          />
          <DebugTableLink
            name="Simulated Allocator Runs on a Specific Range"
            url="/_status/allocator/range/1"
            note="/_status/allocator/range/[range_id]"
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
        <DebugTableRow title="Custom Time-Series Chart">
          <DebugTableLink
            name="Customizable chart of time series metrics"
            url="#/debug/chart"
          />
        </DebugTableRow>
      </DebugTable>
      <LicenseType />
    </div>
  );
}
