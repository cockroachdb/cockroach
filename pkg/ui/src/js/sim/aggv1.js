let drawBox = require("./boxes.js").drawBox;
let timeScale = require("./visualization.js").timeScale;

function AggV1() {
}

AggV1.prototype.init = function(model) {
  // Create 101 linear gradients to represent 0-100% fullness,
  // inclusive.
  for (var pctUsage = 0; pctUsage <= 100; pctUsage++) {
    var color = "008000";
    if (pctUsage > 95) {
      color = "ff4500";
    } else if (pctUsage > 90) {
      color = "ff8c00";
    } else if (pctUsage > 85) {
      color = "#ffd700";
    }
    var grad = model.defs
        .append("linearGradient")
        .attr("id", "fullnessGradient-" + pctUsage)
        .attr("x1", "0%").attr("x2", "0%")
        .attr("y1", "100%").attr("y2", "0%");
    grad.append("stop").attr("offset", (pctUsage + "%")).style("stop-color", color);
    grad.append("stop").attr("offset", (pctUsage + "%")).style("stop-color", "white");
  }
}

AggV1.prototype.dc = function(model, sel) {
  var g = sel.append("g")
      .attr("class", "dc-contents");
  g.append("path")
    .attr("d", function(d) { return drawBox(2, 2, 0.1) })
    .attr("vector-effect", "non-scaling-stroke")
    .attr("class", function(d) { return d.clazz })
    .attr("transform", function(d) { return "translate(-1, -1)" });
  sel.append("text")
    .attr("class", "dclabel")
    //.attr("dx", function(d) { return -d.radius })
    .attr("dy", function(d) { return "1em" })
    .text(function(d) { return d.label });
}

// Distance that nodes are draw from center of datacenter.
var nodeDistance = 0.7

function computeNodeRadius(numNodes) {
  // Compute internode distance, which is length of chord separating nodes.
  var interNodeDistance = Math.abs(2 * nodeDistance * Math.sin(Math.PI / numNodes));
  var radius = interNodeDistance / 3
  if (radius > 0.20) {
    return 0.20;
  }
  return radius;
}

function computeNodeAngle(i, numNodes) {
  return 2 * Math.PI * (i + 1) / numNodes - Math.PI / 2
}

AggV1.prototype.node = function(model, sel) {
  return sel.append("circle")
    .attr("vector-effect", "non-scaling-stroke")
    .attr("r", function(d) {
      return computeNodeRadius(d.dc.roachNodes.length)
    })
    .attr("class", function(d) { return d.clazz })
    .attr("cx", function(d, i) {
      d.x = nodeDistance * Math.cos(computeNodeAngle(i, d.dc.roachNodes.length))
      return d.x
    })
    .attr("cy", function(d, i) {
      d.y = nodeDistance * Math.sin(computeNodeAngle(i, d.dc.roachNodes.length))
      return d.y
    })
    //.attr("visibility", "hidden")
}

AggV1.prototype.packRanges = function(model, n, sel) {
  var pctUsage = Math.floor(n.pctUsage(true))
  model.svg.select("#" + n.id).selectAll(".roachnode")
    .style("fill", "url(#fullnessGradient-" + pctUsage + ")")
}

AggV1.prototype.sendRequest = function(model, payload, link, reverse, endFn) {
  setTimeout(function() { endFn() }, link.latency * timeScale)
}

module.exports = AggV1;