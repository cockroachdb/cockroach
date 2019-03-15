// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import Long from "long";
import * as React from "react";

import * as protos from  "src/js/protos";

import Loading from "src/views/shared/components/loading";
import { ComponentActivity, ComponentActivityMetricsI, ComponentActivityMetrics } from "./component_activity";
import { SampleOptions, SampleState } from "./sample";
import { Legend } from "./legend";
import { SortProps, SampleTable } from "./sample_table";
import * as time_util from "./time_util";

import "./component_node_matrix.styl";

// A system component (e.g. gateway.sql.execution), or collapsed set
// of system components, forms a row in the component node matrix.
interface ISystemComponentRow {
  // Displayed name (may not be full name if there are collapsed children.
  name: string;
  // Hierarchical name part (e.g., "gateway", "sql", or "execution" in
  // "gateway.sql.execution").
  partName: string;
  // Depth in hierarchy.
  depth: number;
  // True if children are collapsed.
  collapsed: boolean;
  // Child components as a map from child part name to system component.
  childMap: {[name: string]: ISystemComponentRow};
  // Children ordered by dependency graph.
  children: ISystemComponentRow[];
  // Map from node id to a map from component name (full) to component
  // activities. If there are no collapsed children, the map will have
  // just one entry.
  nodeActivities?: {[id: number]: {[name: string]: protos.cockroach.server.serverpb.ComponentsResponse.IComponentActivity[]}};
}

export class SystemComponentRow {
  name = "";
  partName = ""
  depth = 0;
  collapsed = 0;
  childMap = {};
  children = [];

  getNodeActivity(node_id: number) {
    if (!this.nodeActivities || !(node_id in this.nodeActivities)) {
      return null;
    }
    const compNames: string[] = Object.keys(this.nodeActivities[node_id]);
    if (compNames.length == 1) {
      return this.nodeActivities[node_id][compNames[0]];
    }
    // First clone the result using the first component activity and then aggregate the remainder.
    const result: protos.cockroach.util.tracing.IComponentActivity = new protos.cockroach.util.tracing.ComponentActivity();
    result.span_count = Long.fromNumber(0);
    result.event_count = Long.fromNumber(0);
    result.stuck_count = Long.fromNumber(0);
    result.errors = Long.fromNumber(0);
    _.map(compNames, (name) => {
      const ca: protos.cockroach.server.serverpb.ComponentsResponse.IComponentActivity = this.nodeActivities[node_id][name];
      result.span_count = result.span_count.add(ca.span_count);
      result.stuck_count = result.stuck_count.add(ca.stuck_count);
      result.event_count = result.event_count.add(ca.event_count);
      result.errors = result.errors.add(ca.errors);
    });
    return result;
  }

  createCell(node_id: number) {
    var components: string[] = [];
    if (node_id in this.nodeActivities) {
      _.map(Object.keys(this.nodeActivities[node_id]), (name) => { components.push(name); });
    }
    return new Cell(node_id, this.name, components);
  }
}

export class Cell {
  node_id: number;
  name: string;
  components: string[];

  constructor(node_id: number, name: string, components: string[]) {
    this.name = name;
    this.node_id = node_id;
    this.components = components
  }
}

export class CombinedTraces {
  active: boolean;
  error: any;
  traces: protos.cockroach.util.tracing.ComponentTraces;

  constructor(name: string, traceMap: {[component: string]: protos.cockroach.util.tracing.ComponentTraces}) {
    this.traces = this.collapseMap(name, traceMap);
  }

  // Because the system components may have been collapsed, the
  // traceMap may have multiple components which need to be combined
  // into a single ComponentTraces object so the samples can be sorted
  // by the sample table. When we combine, we set each sample's
  // prefix.
  collapseMap(name, traceMap) {
    const keys: string[] = Object.keys(traceMap);
    if (keys.length == 1) {
      _.map(traceMap[keys[0]].samples.samples, (s) => {
        delete s.prefix;
      });
      return traceMap[keys[0]];
    }
    var ct: protos.cockroach.util.tracing.IComponentTraces = new protos.cockroach.util.tracing.ComponentTraces({
      samples: new protos.cockroach.util.tracing.ComponentSamples()
    });
    _.map(traceMap, (val, key) => {
      ct.timestamp = val.timestamp;
      _.map(val.samples.samples, (s) => {
        s.prefix = key.substring(name.length + 1);
        ct.samples.samples.push(s);
      });
      _.map(val.events, (val, key) => {
        if (!ct.events.hasOwnProperty(key)) {
          ct.events[key] = val;
        } else {
          if (val.error && !ct.events[key].error) {
            ct.events[key].error = val.error;
          }
          ct.events[key].count += val.count;
        }
      });
    });
    return ct;
  }
}

function childCount(sc: SystemComponentRow, expanded: Cell) {
  if (Object.keys(sc.childMap).length == 0) {
    return (expanded && expanded.name == sc.name) ? 2 : 1;
  }
  var sum: number = 0;
  _.map(sc.childMap, (c) => {
    sum += childCount(c, expanded);
  });
  return sum;
}

function MatrixHeader(props) {
  const { maxDepth, nodeIDs, metrics, cellWidth, legendWidth } = props;
  const headerStyle = {
    transform: "translate(0px, 10px) rotate(-45deg)",
    width: cellWidth
  }
  const nodeIDHeaders = nodeIDs.map((n) => (
      <th class={n.error ? "node-header node-header--error" : "node-header"} title={n.error}>
        <div style={headerStyle}>
          <span>{"Node " + n.id.toString()}</span>
        </div>
      </th>
  ));

  return (
      <tr>
        <th className="matrix-legend-cell" colspan={maxDepth}>
          <Legend metrics={metrics} width={legendWidth} />
        </th>
        {nodeIDHeaders}
      </tr>
  );
}

function ExpandedCellGuts(props) {
  const { expanded, componentTraces } = props;
  if (!componentTraces) {
    return (
        <span id="title">Start cluster-wide sampling to inspect traces through system components</span>
    );
  }
  const { traces } = componentTraces;
  if (!traces || traces.samples.samples.length == 0) {
    return (
        <span id="title">
          No traces for <span className="component-name">{expanded.name}</span> on
          <span className="component-node"> node {expanded.node_id}</span>
        </span>
    );
  }
  return (
      <React.Fragment>
        <span id="title">
          {traces.samples.samples.length} traces for <span className="component-name">{expanded.name}</span> on
          <span className="component-node"> node {expanded.node_id}</span> collected @
          {time_util.formatDateTime(time_util.timestampToDate(traces.timestamp))}
        </span>
        <SampleTable
          name={expanded.name}
          node_id={expanded.node_id}
          samples={traces.samples.samples}
          expandedSample={props.expandedSample}
          sortProps={props.sortProps}
          onSortSamples={props.onSortSamples}
          onToggleSample={props.onToggleSample}
        />
      </React.Fragment>
  );
}

function ExpandedCell(props) {
  const { sc, componentTraces } = props;
  return (
      <td className="matrix-cell-expanded" colspan={props.colspan}>
        <SampleOptions state={props.sampleState} onChange={props.onSampleChange} />
        <Loading
          loading={componentTraces && componentTraces.active}
          error={componentTraces && componentTraces.error}
          render={() => (
            <ExpandedCellGuts {...props} />
          )}
        />
      </td>
  );
}

function MatrixCell(props) {
  const { metrics, cellWidth, onToggleCell, expandedCell, sc, node_id, error } = props;
  const isExpanded: boolean = expandedCell && expandedCell.node_id == node_id && expandedCell.name == sc.name;

  function onClick(e) {
    onToggleCell(sc.createCell(node_id));
  }

  const reported: boolean = (node_id in sc.nodeActivities);
  const activity: protos.cockroach.util.tracing.IComponentActivity =
    reported ? sc.getNodeActivity(node_id) : new protos.cockroach.util.tracing.ComponentActivity({
      span_count: 0, event_count: 0, stuck_count: 0, errors: 0});
  const title: string =
    reported ? "Request count: " + activity.span_count + ", errors: " + activity.errors + ", stuck: " + activity.stuck_count : "Unreported";
  const className: string = isExpanded ? "matrix-cell matrix-cell--expanded" : "matrix-cell";
  const style: any = { width: cellWidth };

  return (
      <td className={className} style={style} title={title} onClick={onClick}>
        <ComponentActivity activity={activity} metrics={metrics} error={error} />
      </td>
  );
}

function ComponentNameCols(props) {
  const { maxDepth, nameParts, sc, onToggleComponent } = props;
  return nameParts.map((part, idx) => {
    const lastComp = idx == nameParts.length - 1;
    const colspan = lastComp ? maxDepth - part.depth + 1 : 1;
    const rowspan = part.rowspan == 0 ? 1 : part.rowspan;
    if (part.name != "") {
      const parts: string[] = sc.name.split(".")
      const prefix: string = _.join(parts.slice(0, parts.length - nameParts.length + (idx + 1)), ".");
      function onClick(e) {
        onToggleComponent(prefix);
      }
      const className: string = "matrix-name-cell matrix-name-cell--part-" + part.depth;
      var name: string = part.name + (sc.collapsed && lastComp ? " ↓" : "");
      return (
          <td className={className} colspan={colspan} rowspan={rowspan} onClick={onClick} title={prefix}>{name}</td>
      );
    }
  });
}

function NodeActivityCols(props) {
  const { nodeIDs } = props;
  return nodeIDs.map((n) => (
      <MatrixCell {...props} node_id={n.id} error={n.error} />
  ));
}

function MatrixRow(props) {
  const { nodeIDs, sc, expandedCell  } = props;

  return (
      <React.Fragment>
        <tr>
          <React.Fragment>
            <ComponentNameCols {...props} />
            <NodeActivityCols {...props} />
          </React.Fragment>
        </tr>
        {expandedCell && sc.name == expandedCell.name &&
          <tr>
            <ExpandedCell {...props} expanded={expandedCell} colspan={nodeIDs.length} />
          </tr>
        }
      </React.Fragment>
  );
}

function RecursiveMatrixBody(props) {
  const { maxDepth, nodeIDs, metrics, parent, nameParts, expandedCell } = props;
  if (parent.nodeActivities) {
    return (
        <MatrixRow {...props} sc={parent} />
    );
  }

  var newNameParts = nameParts;
  var idx: number = 0;
  return (
    _.map(parent.childMap, (sc, key) => {
      if (idx == 0) {
        newNameParts = newNameParts.concat([{name:sc.partName, depth: sc.depth, rowspan: childCount(sc, expandedCell)}]);
      } else {
        newNameParts = [{name:sc.partName, depth: sc.depth, rowspan: childCount(sc, expandedCell)}];
      }
      idx++;
      return (
          <RecursiveMatrixBody {...props} parent={sc} nameParts={newNameParts} />
      );
    })
  );
}

interface ComponentNodeMatrixProps {
  nodes: protos.cockroach.server.serverpb.ComponentsResponse.INodeResponse[];
}

export class ComponentNodeMatrix extends React.Component<ComponentNodeMatrixProps> {
  constructor(props: ComponentNodeMatrixProps) {
    super(props);
    this.state = {};
  }

  onSortSamples = (sp: SortProps) => {
    this.setState({sortProps: sp});
  }

  render() {
    const { activityResponses, collapsedComponents } = this.props;
    if (!activityResponses || !activityResponses.length) {
      return null;
    }
    const nodeIDs: number[] = _.map(activityResponses, (n) => { return {id: n.node_id, error: n.error_message}; });
    nodeIDs.sort((a,b) => a.id - b.id);

    const root: ISystemComponentRow = new SystemComponentRow();
    var maxDepth: number = 0;

    const getChild = function(parent: ISystemComponentRow, partName: string, name: string) {
      if (!(partName in parent.childMap)) {
        const sc: ISystemComponentRow = new SystemComponentRow();
        sc.partName = partName;
        sc.name = name;
        sc.depth = parent.depth + 1;
        if (sc.depth > maxDepth) {
          maxDepth = sc.depth;
        }
        // If there are already node activities, move them to a new child node called "-".
        if (parent.nodeActivities) {
          var na = parent.nodeActivities;
          delete parent.nodeActivities;
          var nc: ISystemComponentRow = getChild(parent, "-", parent.name + ".-");
          nc.nodeActivities = na;
        }
        parent.childMap[partName] = sc;
      }
      return parent.childMap[partName];
    }

    const addComponent = function(parent: ISystemComponentRow, nameParts: string[], prefix: string) {
      if (nameParts.length == 0) {
        return null;
      }
      prefix += (prefix == "" ? "" : ".") + nameParts[0];
      if (nameParts.length == 1) {
        return getChild(parent, nameParts[0], prefix);
      } else if (prefix in collapsedComponents) {
        const child: SystemComponentRow = getChild(parent, nameParts[0], prefix);
        child.collapsed = true;
        return child;
      }
      return addComponent(getChild(parent, nameParts[0], prefix), nameParts.slice(1), prefix);
    }

    activityResponses.forEach((n) => {
      _.map(n.components, (ca, name) => {
        const parts: string[] = name.split(".");
        var sc: ISystemComponentRow = addComponent(root, parts, "");
        // If there are already children, we have overlapping component names.
        if (Object.keys(sc.childMap).length > 0) {
          parts.push("-");
          sc = addComponent(root, parts, "");
        }
        if (!sc.nodeActivities) {
          sc.nodeActivities = {};
        }
        if (!(n.node_id in sc.nodeActivities)) {
          sc.nodeActivities[n.node_id] = {}
        }
        sc.nodeActivities[n.node_id][name] = ca;
      })
    });

    // Compute metrics across all components.
    const metrics: ComponentActivityMetricsI = new ComponentActivityMetrics();
    const computeMetrics = function(sc: ISystemComponentRow) {
      if (sc.nodeActivities) {
        _.map(sc.nodeActivities, (na, node_id) => {
          metrics.addComponentActivity(sc.getNodeActivity(node_id));
        });
      }
      _.map(sc.childMap, (child) {
        computeMetrics(child);
      });
    }
    computeMetrics(root);

    const cellWidth: number = (nodeIDs.length < 8) ? 100 : (nodeIDs.length < 16) ? 50 : 25;
    const legendWidth: number = maxDepth * 100 - 80;

    return (
        <table id="matrix">
        <MatrixHeader
          maxDepth={maxDepth}
          nodeIDs={nodeIDs}
          metrics={metrics}
          legendWidth={legendWidth}
          cellWidth={cellWidth}
        />
        <RecursiveMatrixBody
          {...this.props}
          maxDepth={maxDepth}
          nodeIDs={nodeIDs}
          metrics={metrics}
          cellWidth={cellWidth}
          parent={root}
          nameParts={[]}
          sortProps={this.state.sortProps}
          onSortSamples={this.onSortSamples}
        />
        </table>
    );
  }
}
