import Combokeys from "combokeys";
import * as React from "react";
import { RouterState } from "react-router";
import { connect } from "react-redux";
import CodeMirror from "react-codemirror";
import "codemirror/mode/sql/sql";
import "codemirror/lib/codemirror.css";
import "codemirror/theme/neat.css";

import * as protos from "../js/protos";
import { AdminUIState } from "../redux/state";
import { refreshPhysicalQueryPlan } from "../redux/apiReducers";

import { QueryPlanGraph } from "../components/queryPlanGraph";

// Constants used to store per-page sort settings in the redux UI store.
export const UI_QUERY_PLAN_SOURCE_STRING_KEY = "queryPlan/query";

/**
 * QueryPlanData are the data properties which should be passed to the QueryPlan
 * container.
 */
interface QueryPlanData {
  queryError: boolean;
  queryPlan: string;
  queryString: string;
}

/**
 * QueryPlanActions are the action dispatchers which should be passed to the
 * QueryPlan container.
 */
interface QueryPlanActions {
  // Refresh the table data
  refreshPhysicalQueryPlan: typeof refreshPhysicalQueryPlan;
}

interface QueryPlanState {
  currentQuery: string;
  query: string;
}

/**
 * QueryPlanProps is the type of the props object that must be passed to
 * QueryPlan component.
 */
type QueryPlanProps = QueryPlanData & QueryPlanActions & QueryPlanState & RouterState;

/**
 * Renders the layout of the nodes page.
 */
class QueryPlan extends React.Component<QueryPlanProps, {}> {
  state: QueryPlanState = {
    query: "",
    currentQuery: "",
  };
  editor: HTMLElement;
  keyBindings: Combokeys.Combokeys;

  static title() {
    return <h2>Query Plan</h2>;
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

  getQueryPlan() {
    const { query } = this.state;
    this.setState({currentQuery: query});
    this.props.refreshPhysicalQueryPlan(
      new protos.cockroach.server.serverpb.PhysicalQueryPlanRequest({ query }));
  }

  updateCode(query: string) {
    this.setState({ query });
  }

  getQueryError() {
    if (this.props.queryError) {
      return <div className="query-plan__error">Error loading query plan.</div>;
    }
  }

  render() {
    let options = {
      lineNumbers: true,
      lineWrapping: true,
      mode: "text/x-sql",
      theme: "neat",
    };
    return <div className="section query-plan">
      <div className="query-plan__editor" ref={(editor) => this.editor = editor}>
        <CodeMirror
          value={this.state.query}
          onChange={this.updateCode.bind(this)}
          options={options}
        />
      </div>
      <div className="query-plan__graph">
        {this.getQueryError()}
        <button className="query-plan__refresh" onClick={this.getQueryPlan.bind(this)}>
          Refresh plan
        </button>
        <QueryPlanGraph data={this.props.queryPlan}/>
      </div>
    </div>;
  }
}

// Connect the QueryPlan class with our redux store.
let queryPlanConnected = connect(
  (state: AdminUIState) => {
    const cached = state.cachedData.physicalQueryPlan as any;
    const queryError = !cached.valid && !cached.inFlight && cached.lastError;
    return {
      queryError,
      queryPlan: !queryError && cached.data ? cached.data.physical_query_plan : "",
    };
  },
  {
    refreshPhysicalQueryPlan,
  },
)(QueryPlan);

export default queryPlanConnected;
