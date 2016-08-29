import _ from "lodash";
import * as React from "react";
import * as d3 from "d3";
import { connect } from "react-redux";

import * as protos from "../js/protos";
import { AdminUIState } from "../redux/state";
import { refreshConstraints } from "../redux/apiReducers";
import { KeyedCachedDataReducerState } from "../redux/cachedDataReducer";

interface Datum {
  name?: string;
  attrs?: string[];
  children?: Datum[];
  candidate?: Proto2TypeScript.cockroach.server.serverpb.Candidate;
}

interface ConstraintsState {
  constraints: string;
}

/**
 * ConstraintsProps is the type of the props object that must be passed to
 * Constraints component.
 */
interface ConstraintsProps {
  state: KeyedCachedDataReducerState<Proto2TypeScript.cockroach.server.serverpb.ConstraintsResponseMessage>;
  refreshConstraints: typeof refreshConstraints;
}

/**
 * Renders the main content of the help us page.
 */
class Constraints extends React.Component<ConstraintsProps, ConstraintsState> {
  state: ConstraintsState = {
    constraints: "",
  };
  chart: d3.Selection<Datum>;
  debounceRefreshConstraints = _.debounce(this.refreshConstraints, 300);

  refreshConstraints() {
    this.props.refreshConstraints(new protos.cockroach.server.serverpb.ConstraintsRequest({
      constraints: this.state.constraints,
    }));
  }

  componentDidMount() {
    this.refreshConstraints();
    this.updateChart();
  }

  componentDidUpdate() {
    this.updateChart();
  }

  // nodeDatum returns a datum with all node and store info.
  nodeDatum(stores: Proto2TypeScript.cockroach.roachpb.StoreDescriptor[], candidates: _.Dictionary<Proto2TypeScript.cockroach.server.serverpb.Candidate>): Datum {
    const nodeDesc = stores[0].node;
    const node = {
      name: `node=${nodeDesc.node_id}`,
      attrs: nodeDesc.attrs.attrs,
      children: _.map(stores, (store: Proto2TypeScript.cockroach.roachpb.StoreDescriptor): Datum => {
        return {
          name: `store=${store.store_id}`,
          attrs: store.attrs.attrs,
          candidate: candidates[store.store_id],
        };
      }),
    };
    const tiers = stores[0].locality.tiers;
    return this.tiersDatum(tiers, node);
  }

  // tiersDatum builds a recursive datum for each tier with base as the child.
  tiersDatum(tiers: Proto2TypeScript.cockroach.roachpb.Tier[], base: Datum): Datum {
    if (tiers.length === 0) {
      return base;
    }

    const tier = tiers[tiers.length - 1];
    return this.tiersDatum(tiers.slice(0, tiers.length - 1), {
      name: `${tier.key}=${tier.value}`,
      children: [base],
    });
  }

  // mergeDatum merges the addition into the children of datum.
  mergeDatum(base: Datum, addition: Datum) {
    const d = _.find(base.children, {name: addition.name});
    if (d) {
      _.each(addition.children, (c) => this.mergeDatum(d, c));
    } else {
       base.children.push(addition);
    }
  }

  structuredStores(): Datum {
    const state = this.props.state[this.state.constraints];
    if (!state || !state.valid) {
      return {};
    }

    const candidates = _.keyBy(state.data.candidates, (c) => c.desc.store_id);
    const nodes = _.groupBy(state.data.stores, (s) => s.node.node_id);

    const root: Datum = {name: "Cluster", children: []};
    _.each(nodes, (stores) => {
      this.mergeDatum(root, this.nodeDatum(stores, candidates));
    });
    return root;
  }

  updateChartHeight(): number {
    const state = this.props.state[this.state.constraints];
    const height = (state && state.valid) ? state.data.stores.length * 50 : 0;
    this.chart.attr("height", height);
    return height;
  }

  updateChart() {
    // Created with help from https://bl.ocks.org/mbostock/4063570.
    const root = this.structuredStores();

    this.chart.select("g").remove();

    const {width} = (this.chart.node() as any).getBoundingClientRect();
    const height = this.updateChartHeight();
    const cluster = d3.layout.cluster()
      .size([height, width - 260]);

    const g = this.chart.append("g")
      .attr("transform", "translate(10,0)");

    const nodes = cluster.nodes(root);

    const diagonal = d3.svg.diagonal()
      .projection((d) => {
        return [d.y, d.x];
      });

    g.selectAll("path.link")
      .data(cluster.links(nodes))
      .enter().append("path")
        .attr("class", "link")
        .attr("d", diagonal)
        .style("stroke", "#555")
        .style("stroke-opacity", "0.2")
        .style("stroke-width", "1px")
        .style("fill", "transparent");

    const gnode = g.selectAll("g.node")
      .data(nodes)
      .enter().append("g")
        .attr("class", "node")
        .attr("transform", (d) => "translate(" + d.y + "," + d.x + ")")
        .attr("opacity", (d: Datum) => d.candidate ? 1 : 0.5);

    gnode.append("circle")
      .attr("r", 4.5)
      .style("fill", (d: Datum) => d.candidate ? "green" : "black");

    const text = gnode.append("text");
    text.append("tspan")
      .attr("x", 8)
      .text((d: Datum) => `${d.name} ${d.candidate ? "score=" + d.candidate.score : ""}`);
    text.append("tspan")
      .attr("x", 8)
      .attr("dy", "1em")
      .text((d: Datum) => d.attrs ? `attrs=${d.attrs.join(",")}` : "");
  }

  handleChange(event: MouseEvent) {
    this.setState({constraints: (event.target as HTMLInputElement).value});
    this.debounceRefreshConstraints();
  }

  renderLoadingError() {
    const state = this.props.state[this.state.constraints];
    return <div>
      {state && state.lastError ? <div>Error: {state.lastError.message},{state.data.Error}</div> : ""}
      {state && state.inFlight ? <div>Loading...</div> : ""}
    </div>;
  }

  render() {
    return <div className="section node">
      <div>
        <b>Filter by constraints</b>
        <input type="text" placeholder="Ex. +mem,rack=3" value={this.state.constraints} onChange={this.handleChange.bind(this)}/>
      </div>
      {this.renderLoadingError()}
      <svg className="d3" width="100%" ref={(ref) => this.chart = d3.select(ref)}></svg>
    </div>;
  }
}

// Connect to the redux store.
export default connect(
  (state: AdminUIState) => {
    return {
      state: state.cachedData.constraints,
    };
  },
  {
    refreshConstraints,
  }
)(Constraints);
