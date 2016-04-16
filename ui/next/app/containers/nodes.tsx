/// <reference path="../../typings/main.d.ts" />

import * as React from "react";

/**
 * NodesMain renders the main content of the nodes page.
 */
export class NodesMain extends React.Component<{}, {}> {
	render() {
		return <div className="section">
			<h1>Nodes Page</h1>
		</div>;
	}
}

/**
 * NodesTitle renders the header of the nodes page.
 */
export class NodesTitle extends React.Component<{}, {}> {
	render() {
		return <h2>Nodes</h2>;
	}
}
