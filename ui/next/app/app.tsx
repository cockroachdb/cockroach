/// <reference path="../typings/main.d.ts" />

import * as React from "react";
import * as ReactDOM from "react-dom";
import { Router, Route, IndexRedirect, hashHistory } from "react-router";
import Layout from "./containers/layout.tsx";
import {ClusterMain, ClusterTitle} from "./containers/cluster.tsx";
import {DatabasesMain, DatabasesTitle} from "./containers/databases.tsx";
import {HelpUsMain, HelpUsTitle} from "./containers/helpus.tsx";
import {NodesMain, NodesTitle} from "./containers/nodes.tsx";

ReactDOM.render(
  <Router history={hashHistory}>
	<Route path="/" component={Layout}>
		<IndexRedirect to="cluster" />
		<Route path="cluster"
			   components={{main: ClusterMain, title:ClusterTitle}}/>
		<Route path="nodes"
			   components={{main: NodesMain, title:NodesTitle}}/>
		<Route path="databases"
			   components={{main: DatabasesMain, title:DatabasesTitle}}/>
		<Route path="help-us/reporting"
			   components={{main: HelpUsMain, title:HelpUsTitle}}/>
	</Route>
  </Router>,
  document.getElementById("react-layout")
);
