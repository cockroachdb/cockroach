import React from "react";
import _ from "lodash";
import Combokeys from "combokeys";
import { RouterState } from "react-router";
import { connect } from "react-redux";
import CodeMirror from "react-codemirror";

// This import makes SQL highlighting info available to CodeMirror.
import "codemirror/mode/sql/sql";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { refreshQueryPlan } from "src/redux/apiReducers";

import { QueryPlanGraph } from "src/views/devtools/components/queryPlanGraph";

// Constants used to store per-page sort settings in the redux UI store.
export const UI_QUERY_PLAN_SOURCE_STRING_KEY = "queryPlan/query";

/**
 * QueryPlanData are the data properties which should be passed to the QueryPlan
 * container.
 */
interface QueryPlanData {
  queryError: boolean;
  queryPlan: string;
}

/**
 * QueryPlanActions are the action dispatchers which should be passed to the
 * QueryPlan container.
 */
interface QueryPlanActions {
  // Refresh the table data
  refreshQueryPlan: typeof refreshQueryPlan;
}

interface QueryPlanState {
  currentQuery: string;
  query: string;
}

/**
 * QueryPlanProps is the type of the props object that must be passed to
 * QueryPlan component.
 */
type QueryPlanProps = QueryPlanData & QueryPlanActions & RouterState;

/**
 * Renders the layout of the nodes page.
 */
class QueryPlan extends React.Component<QueryPlanProps, QueryPlanState> {
  state: QueryPlanState = {
    query: "",
    currentQuery: "",
  };
  editor: HTMLElement;
  keyBindings: Combokeys.Combokeys;

  static title() {
    return <h2>Query Plan</h2>;
  }

  getQueryPlan = () => {
    const { query } = this.state;
    this.setState({currentQuery: query});
    this.props.refreshQueryPlan(
      new protos.cockroach.server.serverpb.QueryPlanRequest({ query }));
  }

  updateCode = (query: string) => {
    this.setState({ query });
  }

  componentDidMount() {
    this.keyBindings = new Combokeys(this.editor);
    // Allow callback to fire in inputs
    // https://github.com/avocode/combokeys/issues/7
    this.keyBindings.stopCallback = () => false;
    this.keyBindings.bind("command+enter", () => this.getQueryPlan());
  }

  componentWillUnmount() {
    this.keyBindings.detach();
  }

  getQueryError() {
    if (this.props.queryError) {
      return <div className="query-plan__error">Error loading query plan.</div>;
    }
  }

  render() {
    const options = {
      lineNumbers: true,
      lineWrapping: true,
      mode: "text/x-sql",
      theme: "neat",
    };
    return <div className="section query-plan">
      <div className="query-plan__editor" ref={(editor) => this.editor = editor}>
        <CodeMirror
          value={this.state.query}
          onChange={this.updateCode}
          options={options}
        />
      </div>
      <div className="query-plan__graph">
        {this.getQueryError()}
        <button className="query-plan__refresh" onClick={this.getQueryPlan}>
          Refresh plan
        </button>
        <QueryPlanGraph data={this.props.queryPlan}/>
      </div>
    </div>;
  }
}

// Connect the QueryPlan class with our redux store.
const queryPlanConnected = connect(
  (state: AdminUIState) => {
    const cached = state.cachedData.queryPlan;
    const queryError = !cached.valid && !cached.inFlight && !_.isUndefined(cached.lastError);
    return {
      queryError,
      queryPlan: !queryError && cached.data ? cached.data.distsql_physical_query_plan : "",
    };
  },
  {
    refreshQueryPlan,
  },
)(QueryPlan);

export default queryPlanConnected;
