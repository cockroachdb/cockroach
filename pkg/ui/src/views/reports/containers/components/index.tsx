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

import classNames from "classnames";
import _ from "lodash";
import Long from "long";
import React from "react";
import { Helmet } from "react-helmet";
import { connect } from "react-redux";
import { RouterState } from "react-router";

import * as protos from "src/js/protos";
import { getSampleTraces, getComponentTraces, getComponentTrace } from "src/util/api";
import { componentsRequestKey, refreshComponents } from "src/redux/apiReducers";
import { CachedDataReducerState } from "src/redux/cachedDataReducer";
import { AdminUIState } from "src/redux/state";
import { nodeIDAttr } from "src/util/constants";

import { ExpandedSample } from "./expanded_sample";
import { ComponentNodeMatrix, CombinedTraces, Cell } from "./component_node_matrix";
import { SampleState } from "./sample";
import { genActivityResponses, genSampleMap, genComponentTraces, genComponentTrace } from "./testing";

function isTesting() {
  return window.location.hash.includes("test_ui");
}

interface ComponentsState {
  sampleState: SampleState;
}

interface ComponentsOwnProps {
  components: CachedDataReducerState<protos.cockroach.server.serverpb.ComponentsResponse>;
  refreshComponents: typeof refreshComponents;
  state: ComponentsState;
}

type ComponentsProps = ComponentsOwnProps & RouterState;

function componentsRequestFromProps(props: ComponentsProps) {
  return new protos.cockroach.server.serverpb.ComponentsRequest({
    node_id: props.params[nodeIDAttr],
  });
}

class Components extends React.Component<ComponentsProps, ComponentsState> {
  constructor(props) {
    super(props);
    this.state = {
      sampleState: new SampleState(),
      collapsed: {}
    };
    if (isTesting()) {
      // NOTE: For testing purposes only.
      this.state.testActivityResponses = genActivityResponses();
      this.state.testSampleMap = genSampleMap();
    }
  };

  componentWillMount() {
    this.props.refreshComponents(componentsRequestFromProps(this.props));
  }

  componentWillReceiveProps(nextProps: ComponentsProps) {
    this.props.refreshComponents(componentsRequestFromProps(nextProps));
  }

  renderReportBody() {
    if (_.isNil(this.props.components) || _.isNil(this.props.components.data)) {
      return null;
    }
    if (!_.isNil(this.props.components.lastError)) {
      if (_.isEmpty(this.props.params[nodeIDAttr])) {
        return (
          <div>
            <h2>Error loading data for system component activity</h2>
            {this.props.components.lastError.toString()}
          </div>
        );
      } else {
        return (
          <div>
            <h2>Error loading data for system component activity for node n{this.props.params[nodeIDAttr]}</h2>
            {this.props.components.lastError.toString()}
          </div>
        );
      }
    }

    var activityResponses = this.props.components.data.nodes;
    const nodeIDs = _.keys(_.pickBy(activityResponses, d => {
      return _.isEmpty(d.error_message);
    }));
    if (nodeIDs.length === 0) {
      if (_.isEmpty(this.props.params[nodeIDAttr])) {
        return <h2>No nodes returned any results</h2>;
      } else {
        return <h2>No results reported for node n{this.props.params[nodeIDAttr]}</h2>;
      }
    }
    if (isTesting()) {
      activityResponses = this.state.testActivityResponses;
    }

    return (
        <ComponentNodeMatrix
          activityResponses={activityResponses}
          componentTraces={this.state.componentTraces}
          collapsedComponents={this.state.collapsed}
          onToggleComponent={this.onToggleComponent}
          expandedCell={this.state.expandedCell}
          onToggleCell={this.onToggleCell}
          expandedSample={this.state.expandedSample}
          onToggleSample={this.onToggleSample}
          sampleState={this.state.sampleState}
          onSampleChange={this.onSampleChange}
        />
    );
  }

  getSampleTraces = () => {
    this.setState({componentTraces: {active: true}});
    getSampleTraces(new protos.cockroach.server.serverpb.SampleTracesRequest({
      node_id: this.props.params[nodeIDAttr],
      duration: this.state.sampleState.duration,
      target_count: this.state.sampleState.target_count,
    })).then((result) => {
      console.log("sampled system component traces", result);
      this.setState({
        sample_traces_id: result.sample_traces_id,
        sampleState: Object.assign(this.state.sampleState, {active: false})
      });
      this.getComponentTraces(result.sample_traces_id, this.state.expandedCell)
    }).catch((err) => {
      this.setState({sampleState: Object.assign(this.state.sampleState, {active: false, error: err})});
    });
  }

  getComponentTraces = (sample_traces_id: Long, cell: Cell) => {
    if (!sample_traces_id) {
      return
    }
    // Clear the current component trace before fetching the new one.
    this.setState({componentTraces: {active: true}});

    // Fetch the requested component traces.
    var node_id: string = cell.node_id.toString();
    if (isTesting()) {
      node_id = "local";
    }
    getComponentTraces(new protos.cockroach.server.serverpb.ComponentTracesRequest({
      node_id: node_id,
      sample_traces_id: sample_traces_id,
      components: cell.components
    })).then((result) => {
      console.log("fetched component traces", result);
      if (isTesting()) {
        result.traces = genComponentTraces(cell.node_id, cell.components, this.state.testSampleMap);
      }
      this.setState({componentTraces: new CombinedTraces(result.traces)});
    }).catch((err) => {
      this.setState({componentTraces: {error: err}});
    });
  }

  getComponentTrace = (sample_traces_id: Long, trace_id: Long) => {
    if (!sample_traces_id) {
      return
    }
    // Clear the current component trace before fetching the new one.
    this.setState({expandedSample: {active: true, trace_id: trace_id}});
    // Fetch the requested component traces.
    getComponentTrace(new protos.cockroach.server.serverpb.ComponentTraceRequest({
      node_id: this.props.params[nodeIDAttr],
      sample_traces_id: sample_traces_id,
      trace_id: trace_id
    })).then((result) => {
      console.log("fetched component trace", result);
      if (isTesting()) {
        result.nodes = genComponentTrace(trace_id, this.state.testSampleMap);
      }
      this.setState({expandedSample: new ExpandedSample(trace_id, result.nodes)});
    }).catch((err) => {
      this.setState({expandedSample: {error: err, trace_id: trace_id}});
    });
  }

  onSampleChange = (update: SampleState) => {
    this.setState({sampleState: update});
    if (update.active) {
      this.getSampleTraces();
    }
  }

  onToggleComponent = (component: string) => {
    if (component in this.state.collapsed) {
      delete this.state.collapsed[component];
    } else {
      this.state.collapsed[component] = true;
    }
    this.setState({collapsed: this.state.collapsed, componentTraces: null});
  }

  onToggleCell = (cell: Cell) => {
    const isExpanded: boolean =
      this.state.expandedCell && this.state.expandedCell.node_id == cell.node_id && this.state.expandedCell.name == cell.name;
    if (isExpanded) {
      this.setState({expandedCell: null, componentTraces: null});
    } else {
      this.setState({expandedCell: cell});
      this.getComponentTraces(this.state.sample_traces_id, cell);
    }
  }

  onToggleSample = (trace_id: Long) => {
    if (this.state.expandedTrace && this.state.expandedTrace.equals(trace_id)) {
      this.setState({expandedTrace: null, expandedSample: null});
    } else {
      this.setState({expandedTrace: trace_id});
      this.getComponentTrace(this.state.sample_traces_id, trace_id);
    }
  }

  render() {
    return (
      <div className="section" style={{maxWidth: "100%"}}>
        <Helmet>
          <title>Component Activity | Debug</title>
        </Helmet>
        <h1>System Component Activity</h1>
        <div style={{marginTop: "10px"}}>
          {this.renderReportBody()}
        </div>
      </div>
    );
  }
}

function mapStateToProps(state: AdminUIState, props: ComponentsProps) {
  const nodeIDKey = componentsRequestKey(componentsRequestFromProps(props));
  return {
    components: state.cachedData.components[nodeIDKey] && state.cachedData.components[nodeIDKey],
    lastError: state.cachedData.components[nodeIDKey] && state.cachedData.components[nodeIDKey].lastError,
  };
}

const actions = {
  refreshComponents,
};

export default connect(mapStateToProps, actions)(Components);
