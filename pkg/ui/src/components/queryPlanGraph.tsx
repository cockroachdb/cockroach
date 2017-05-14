import * as React from "react";
import * as d3 from "d3";
import {event as d3Event} from "d3";

import * as cola from "webcola";

interface QueryPlanGraphProps {
  data: string;
}

interface DataGraph {
  nodes: any[];
  links: any[];
  groups: any[];
  constraints: any[];
}

export class QueryPlanGraph extends React.Component<QueryPlanGraphProps, {}> {
  onResize: () => void;

  // SVG Elements in the DOM used to render the graph.
  svg: d3.Selection<SVGElement>;
  vis: d3.Selection<SVGElement>;
  zoomRect: d3.Selection<SVGElement>;
  groupsLayer: d3.Selection<SVGElement>;
  nodesLayer: d3.Selection<SVGElement>;
  linksLayer: d3.Selection<SVGElement>;

  constructor() {
    super();
    this.onResize = function() {
      this.redrawPlan(this.props.data);
    }.bind(this);
  }

  componentDidMount () {
    this.setupPlan();
    window.addEventListener("resize", this.onResize);
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.onResize);
  }

  componentWillReceiveProps(nextProps: QueryPlanGraphProps) {
    if (nextProps.data !== this.props.data) {
      this.redrawPlan(nextProps.data);
    }
  }

  setupPlan() {
    const zoom = d3.behavior.zoom()
      .scale(0.2)
      .translate([80, 80])
      .on("zoom", () => {
        const zoomEvent = d3Event as d3.ZoomEvent;
        this.vis.attr("transform", "translate(" + zoomEvent.translate + ")" + " scale(" + zoomEvent.scale + ")");
      });

    // Rectangle spanning entire visualization to capture zoom events
    this.zoomRect = this.svg.append("g")
      .append("rect")
      .attr("fill", "none")
      .attr("pointer-events", "all");

    this.vis = this.svg.append("g");

    this.zoomRect
      .call(zoom)
      .call(zoom.event);
  }

  redrawPlan(jsonString: string) {
    this.vis.selectAll("*").remove();
    if (!jsonString) {
      return;
    }

    const data = JSON.parse(jsonString);

    const containerWidth = this.svg.node().parentElement.clientWidth;
    const containerHeight = this.svg.node().parentElement.clientHeight;

    this.svg.attr("viewBox", `0 0 ${Math.min(containerWidth, containerHeight)} ${Math.min(containerWidth, containerHeight)}`);
    this.zoomRect
      .attr("width", Math.min(containerWidth, containerHeight))
      .attr("height", Math.min(containerWidth, containerHeight));

    this.groupsLayer = this.vis.append("g");
    this.nodesLayer = this.vis.append("g");
    this.linksLayer = this.vis.append("g");

    const d3cola = cola.d3adaptor()
      .jaccardLinkLengths(40, 0.7)
      .avoidOverlaps(true)
      .flowLayout("y", 80)
      .size([containerWidth, containerHeight]);

    const graph = {} as DataGraph;
    graph.nodes = [];
    for (let i = 0; i < data.processors.length; i++) {
      let p = data.processors[i];
      p.core.graphNodeIdx = graph.nodes.length;
      graph.nodes.push({
        title: p.core.title,
        details: p.core.details,
        width: 60,
        height: 40,
        rx: 5,
        ry: 5,
        type: "core",
      });
      for (let j = 0; j < p.inputs.length; j++) {
        p.inputs[j].graphNodeIdx = graph.nodes.length;
        graph.nodes.push({
          title: p.inputs[j].title,
          details: p.inputs[j].details,
          width: 60,
          height: 40,
          rx: 15,
          ry: 15,
          type: "synchronizer",
        });
      }
      for (let j = 0; j < p.outputs.length; j++) {
        p.outputs[j].graphNodeIdx = graph.nodes.length;
        graph.nodes.push({
          title: p.outputs[j].title,
          details: p.outputs[j].details,
          width: 60,
          height: 40,
          rx: 15,
          ry: 15,
          type: "node",
        });
      }
    }

    graph.links = [];
    for (let i = 0; i < data.edges.length; i++) {
      let srcNode, destNode;

      let e = data.edges[i];
      let p1 = data.processors[e.sourceProc];
      let siblings = 1;
      if (e.sourceOutput) {
        srcNode = p1.outputs[e.sourceOutput - 1].graphNodeIdx;
        if (p1.outputs[e.sourceOutput - 1].title === "by hash") {
          for (let j = 0; j < data.edges.length; j++) {
            if (i !== j && data.edges[j].sourceProc === e.sourceProc &&
                data.edges[j].sourceOutput === e.sourceOutput) {
              siblings = siblings + 1;
            }
          }
        }
      } else {
        srcNode = p1.core.graphNodeIdx;
      }
      let p2 = data.processors[e.destProc];
      if (e.destInput) {
        destNode = p2.inputs[e.destInput - 1].graphNodeIdx;
      } else {
        destNode = p2.core.graphNodeIdx;
      }
      let width = 3.0 / siblings;
      if (width < 1) {
        width = 1;
      }
      graph.links.push({source: srcNode, target: destNode, width: width});
    }

    // Generate groups.
    graph.groups = [];
    for (let i = 0; i < data.nodeNames.length; i++) {
      graph.groups.push({
        nodeID: data.nodeNames[i],
        leaves: [],
        padding: 15,
      });
    }
    for (let i = 0; i < data.processors.length; i++) {
      let p = data.processors[i];
      let n = p.nodeIdx;
      graph.groups[n].leaves.push(p.core.graphNodeIdx);
      for (let j = 0; j < p.inputs.length; j++) {
        graph.groups[n].leaves.push(p.inputs[j].graphNodeIdx);
      }
      for (let j = 0; j < p.outputs.length; j++) {
        graph.groups[n].leaves.push(p.outputs[j].graphNodeIdx);
      }
    }

    // Generate constraints to align input synchronizers and output
    // synchronizers next to the core.
    graph.constraints = [];
    for (let i = 0; i < data.processors.length; i++) {
      let p = data.processors[i];
      if (p.inputs.length === 0 && p.outputs.length === 0) {
        continue;
      }
      let xConstr = {
        type: "alignment",
        axis: "x",
        offsets: [{node: p.core.graphNodeIdx, offset: 0}],
      };
      let yConstr = {
        type: "alignment",
        axis: "y",
        offsets: [{node: p.core.graphNodeIdx, offset: 0}],
      };
      let hSpacing = 80, vSpacing = 28 + 10 * graph.nodes[p.core.graphNodeIdx].details.length;
      for (let j = 0; j < p.inputs.length; j++) {
        let n = p.inputs[j].graphNodeIdx;
        xConstr.offsets.push({node: n, offset: hSpacing * (2 * j + 1 - p.inputs.length)});
        yConstr.offsets.push({node: n, offset: -vSpacing});
        // These edges are not visible, but they help with the layout.
        graph.links.push({source: n, target: p.core.graphNodeIdx, invisible: true});
      }
      for (let j = 0; j < p.outputs.length; j++) {
        let n = p.outputs[j].graphNodeIdx;
        xConstr.offsets.push({node: n, offset: hSpacing * (2 * j + 1 - p.outputs.length)});
        yConstr.offsets.push({node: n, offset: +vSpacing});
        // These edges are not visible, but they help with the layout.
        graph.links.push({source: p.core.graphNodeIdx, target: n, invisible: true});
      }
      graph.constraints.push(xConstr, yConstr);
    }

    let color = d3.scale.category20();

    d3cola
      .nodes(graph.nodes)
      .links(graph.links)
      .groups(graph.groups)
      .constraints(graph.constraints)
      .start(10, 10, 10);

    // define arrow markers for graph links
    this.svg.append("svg:defs")
      .append("svg:marker")
        .attr("id", "end-arrow")
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 5)
        .attr("markerWidth", 3)
        .attr("markerHeight", 3)
        .attr("orient", "auto")
      .append("svg:path")
        .attr("d", "M0,-5L10,0L0,5L2,0")
        .attr("stroke-width", "0px")
        .attr("fill", "#000");

    let group = this.groupsLayer.selectAll(".group")
      .data(graph.groups)
      .enter()
        .append("rect")
          .attr("rx", 4).attr("ry", 4)
          .attr("class", "group")
          .style("fill-opacity", 0.2)
          .style("fill", (d: any) => color(d.nodeID))
          .call(d3cola.drag);

    let link = this.linksLayer.selectAll(".link")
      .data(graph.links.filter((d: any) => !d.invisible))
      .enter()
      .append("line")
      .attr("class", "link")
      .style("stroke-width", (d: any) => d.width);

    let margin = 10, pad = 12;
    let node = this.nodesLayer.selectAll(".node")
      .data(graph.nodes)
      .enter().append("rect")
      .attr("class", (d: any) => d.type)
      .attr("width", (d: any) => d.width + 2 * pad + 2 * margin)
      .attr("height", (d: any) => d.height + 2 * pad + 2 * margin)
      .attr("rx", (d: any) => d.rx)
      .attr("ry", (d: any) => d.rx)
      .call(d3cola.drag);

    let label = this.nodesLayer.selectAll(".label")
      .data(graph.nodes)
      .enter()
      .append("text")
        .attr("class", "label")
        .call(d3cola.drag);

    let setLabels = function (d: any) {
      let el = d3.select(this);
      el.text("");
      let size = 0;
      if (d.type === "core") {
        size = 4;
      }

      el.append("tspan")
       .text(d.title)
       .attr("x", 0).attr("dy", 18 + size)
       .attr("font-size", 14 + size)
       .attr("font-weight", "bold");

      if (!d.details) {
        return;
      }
      for (let i = 0; i < d.details.length; i++) {
        el.append("tspan")
          .text(d.details[i])
          .attr("x", 0).attr("dy", 16 + size)
          .attr("font-size", 12 + size);
      }
    };

    label.each(setLabels);

    let groupLabel = this.vis.selectAll(".groupLabel")
      .data(graph.groups)
      .enter().append("text")
      .attr("font-size", "15")
      .text((d: any) => "Node " + d.nodeID);

    d3cola.on("tick", function () {
      node.each(function (d: any) {
        d.innerBounds = d.bounds.inflate(- margin);
      });
      link.each(function (d: any) {
        d.route = cola.makeEdgeBetween(d.source.innerBounds, d.target.innerBounds, 1);
        if (isIE()) {
          this.parentNode.insertBefore(this, this);
        }
      });

      link.attr("x1", (d: any) => d.route.sourceIntersection.x)
          .attr("y1", (d: any) => d.route.sourceIntersection.y)
          .attr("x2", (d: any) => d.route.arrowStart.x)
          .attr("y2", (d: any) => d.route.arrowStart.y);

      label.each(function (d: any) {
        let b = this.getBBox();
        d.width = b.width + 4 * margin + 8;
        d.height = b.height + 2 * margin + 8;
      });

      node.attr("x", (d: any) => d.innerBounds.x)
          .attr("y", (d: any) => d.innerBounds.y)
          .attr("width", (d: any) => d.innerBounds.width())
          .attr("height", (d: any) => d.innerBounds.height());

      group.attr("x", (d: any) => d.bounds.x)
        .attr("y", (d: any) => d.bounds.y)
        .attr("width", (d: any) => d.bounds.width())
        .attr("height", (d: any) => d.bounds.height());

      groupLabel.data(group.data())
        .attr("x", (d: any) => d.bounds.x + 5)
        .attr("y", (d: any) => d.bounds.y + 15);

      label.attr(
        "transform",
        (d: any) => "translate(" + d.x + margin + "," + (d.y + margin - d.height / 2) + ")");
    });
  }

  render() {
    return <svg ref={(svg) => this.svg = d3.select(svg)} />;
  }
}

function isIE() {
  return ((navigator.appName === "Microsoft Internet Explorer") ||
          ((navigator.appName === "Netscape") &&
           (new RegExp("Trident/.*rv:([0-9]{1,}[\.0-9]{0,})").exec(navigator.userAgent) != null)));
}
