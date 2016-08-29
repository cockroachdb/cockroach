import _ from "lodash";
import * as React from "react";
import * as ReactDom from "react-dom";
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
  candidate?: cockroach.server.serverpb.Candidate;
}

interface ConstraintsData {
  state: KeyedCachedDataReducerState<cockroach.server.serverpb.ConstraintsResponseMessage>;
}

/**
 * RangesMainActions are the action dispatchers which should be passed to the
 * RangesMain container.
 */
interface ConstraintsActions {
  refreshConstraints: typeof refreshConstraints;
}

interface ConstraintsState {
  constraints: string;
}

/**
 * ConstraintsProps is the type of the props object that must be passed to
 * Constraints component.
 */
type ConstraintsProps = ConstraintsData & ConstraintsActions;

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
    const node = ReactDom.findDOMNode(this);
    this.chart = d3.select(node).select("svg");
    setTimeout(this.updateChart.bind(this), 1);
  }

  componentDidUpdate() {
    this.updateChart();
  }

  structuredStores(): Datum {
    const state = this.props.state[this.state.constraints];
    if (!state || !state.valid) {
      return {};
    }

    const candidates: {[id: number]: cockroach.server.serverpb.Candidate} = {};
    for (let candidate of state.data.candidates) {
      candidates[candidate.desc.store_id] = candidate;
    }

    const nodes: {[id: number]: cockroach.roachpb.StoreDescriptor[]} = {};
    for (let store of state.data.stores) {
      let stores = nodes[store.node.node_id];
      if (!stores) {
        stores = [];
        nodes[store.node.node_id] = stores;
      }
      stores.push(store);
    }

    const datums: {[name: string]: Datum} = {};
    const root: Datum = {name: "Cluster", children: []};
    _.each(nodes, (stores: cockroach.roachpb.StoreDescriptor[], id: string) => {
      const nodeDesc = stores[0].node;
      const node = {
        name: `node=${id}`,
        attrs: nodeDesc.attrs.attrs,
        children: _.map(stores, (store: cockroach.roachpb.StoreDescriptor): Datum => {
          return {
            name: `store=${store.store_id}`,
            attrs: store.attrs.attrs,
            candidate: candidates[store.store_id],
          };
        }),
      };

      let container: Datum = root;
      let prefix: string;
      const tiers = stores[0].locality.tiers;
      _.each(tiers, (tier: cockroach.roachpb.Tier) => {
        const name = `${tier.key}=${tier.value}`;
        const tierName = `${prefix}:${name}`;
        let c = datums[tierName];
        if (!c) {
          c = {
            name: name,
            children: [],
          };
          container.children.push(c);
          datums[tierName] = c;
        }
        prefix = tierName;
        container = c;
      });
      container.children.push(node);
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

  render() {
    return <div className="section node">
      <div>
        <b>Filter by constraints</b>
        <input type="text" placeholder="Ex. +mem,rack=3" value={this.state.constraints} onChange={this.handleChange.bind(this)}/>
      </div>
      <svg className="d3" width="100%"></svg>
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
