import * as React from "react";
import {runVisualization, viewWidth, viewHeight} from "../js/sim/index";

/**
 * Renders the layout of the ClusterViz page. "ClusterViz" is an experimental
 * visual map of the cluster which is being rapidly prototyped.
 */
export default class ClusterViz extends React.Component<{}, {}> {
    modelElem: Element;

    static title() {
        return "Cluster Visualization";
    }

    componentDidMount() {
        // After mounting, turn the DOM element over to the visualization code.
        runVisualization(this.modelElem);
    }

    render() {
        let style = {
            height: viewHeight,
            width: viewWidth,
        };
        return <div className="cluster-viz" style={style} ref={(elem) => this.modelElem = elem}/>;
    }
}
