/**
 * UI/NEXT TODO LIST
 *
 * ! = Potentially difficult to implement
 *
 * - All Pages / Shared Components
 *    - "Last Updated"
 *    - Dropdowns
 *      - Automatic Width to fit current selection
 *      - Match menu styling to design
 *      - Fix 1px offset bug
 *    - Timespan Selector
 *      - Add Dropdown Styling
 *      - Add Left/Right Buttons
 *      - Add current time window display
 *    - Tables
 *      - CSS Match to design
 *      - Management of column widths
 * - Cluster Page
 *    - Alert notifications
 *      - Mismatched/Out-of-date Version
 *      - Help us
 *    - Right-side Summary Section
 *      - Link to Nodes page
 *      - Stats
 *        - Unavailable Ranges
 *        - Queries Per Second?
 *      - Events?
 *    - Graphs
 *      - Intelligent Tick Selection
 *      - Always label Y-axis units
 *      - Appearance
 *        - New Colors
 *      - Tooltip when hover over title
 *      - Show full domain for time span (#10362)
 *    - Code block syntax highlighting
 *      - Choose keywords correctly
 *      - Fix bug on direct page load
 * - Databases Page
 *    - Last Updated Column
 *      - Retrieve/Filter events
 *    - Single database page
 *       - Table component row limit
 *       - Route to single database
 *    - Schema Change
 *      - Retrieve information from backend
 *      - Display in table list column
 *      - Display alert on table details page
 *    - Table details page
 *      - Schema Change notification
 *      - Fill out summary stats
 *      - Back Button
 *      - Column widths for grants table
 * - Nodes page
 *  - Table Style
 *  - Add Summary Section
 *  - Remove Link from Navigation Bar
 * - Helpus Page
 *  - New Navigation Bar Icon
 *  - Header links
 *  - New form field Appearance
 *
 * NICE TO HAVE:
 *  - Create a "NodeStatusProvider" similar to "MetricsDataProvider", allowing
 *  different components to access nodes data.
 *
 *  - Commonize code between different graph types (LineGraph and
 *  StackedAreaGraph). This can likely be done by converting them into stateless
 *  functions, that return an underlying "Common" graph component. The props of
 *  the Common graph component would include the part of `initGraph` and
 *  `drawGraph` that are different for these two chart types.
 *
 */

import "nvd3/build/nv.d3.min.css!";
import "react-select/dist/react-select.css!";
import "build/app.css!";

import * as React from "react";
import * as ReactDOM from "react-dom";
import { Provider } from "react-redux";
import { Router, Route, IndexRoute, IndexRedirect } from "react-router";

import {
  tableNameAttr, databaseNameAttr, nodeIDAttr, dashboardNameAttr,
} from "./util/constants";

import { store, history } from "./redux/state";
import Layout from "./containers/layout";
import { DatabaseTablesList, DatabaseGrantsList } from "./containers/databases/databases";
import TableDetails from "./containers/databases/tableDetails";
import HelpUs from "./containers/helpus";
import Nodes from "./containers/nodes";
import Node from "./containers/node";
import NodesOverview from "./containers/nodesOverview";
import NodeOverview from "./containers/nodeOverview";
import NodeGraphs from "./containers/nodeGraphs";
import NodeLogs from "./containers/nodeLogs";
import Raft from "./containers/raft";
import RaftRanges from "./containers/raftRanges";
import registrationSyncListener from "./services/registrationService";

// tslint:disable-next-line:variable-name
const DOMNode = document.getElementById("react-layout");

// Voodoo to force react-router to reload stuff when directed by livereload.
// See https://github.com/capaj/systemjs-hot-reloader.
export function __unload() {
  ReactDOM.unmountComponentAtNode(DOMNode);
}

ReactDOM.render(
  <Provider store={store}>
    <Router history={history}>
      <Route path="/" component={Layout}>
        <IndexRedirect to="cluster" />
        <Route path="cluster" component={ Nodes }>
          <IndexRedirect to="all/activity" />
          <Route path={`all/:${dashboardNameAttr}`} component={NodeGraphs} />
          <Route path={ `node/:${nodeIDAttr}/:${dashboardNameAttr}` } component={NodeGraphs} />
        </Route>
        <Route path="nodes" >
          <IndexRedirect to="overview" />
          <Route path="overview" component={ NodesOverview } />
          <Route path="graphs" component={NodeGraphs} />
        </Route>
        <Route path="nodes">
          // This path has to match the "nodes" route for the purpose of
          // highlighting links, but the page does not render as a child of the
          // Nodes component.
          <Route path={ `:${nodeIDAttr}` } component={ Node }>
            <IndexRoute component={ NodeOverview } />
            <Route path="graphs" component={ NodeGraphs } />
            <Route path="logs" component={ NodeLogs } />
          </Route>
        </Route>
        <Route path="databases">
          <IndexRedirect to="tables" />
          <Route path="tables" component={ DatabaseTablesList } />
          <Route path="grants" component={ DatabaseGrantsList } />
          <Route path={ `database/:${databaseNameAttr}/table/:${tableNameAttr}` } component={ TableDetails } />
        </Route>
        <Route path="help-us/reporting" component={ HelpUs } />
        <Route path="raft" component={ Raft }>
          <IndexRedirect to="ranges" />
          <Route path="ranges" component={ RaftRanges } />
        </Route>
      </Route>
    </Router>
  </Provider>,
  DOMNode
);

// Subscribe store listeners.
store.subscribe(registrationSyncListener(store));
