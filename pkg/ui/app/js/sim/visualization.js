// This file defines the visual elements corresponding to the CockroachDB
// distributed system and their animations.
var d3 = require("d3");
var HeartbeatPayload = require("./request.js").HeartbeatPayload;

var viewWidth = 960, viewHeight = 500
var timeScale = 2 // multiple for slowing down (< 1) or speeding up (> 1) animations
var color = d3.scale.category20()

d3.selection.prototype.moveToBack = function() {
  return this.each(function() {
    var firstChild = this.parentNode.firstChild
    if (firstChild) {
      this.parentNode.insertBefore(this, firstChild)
    }
  })
}

function mountModel(el, model, enableControls) {
  var div = d3.select(el);

  model.svgParent = div.append("div")
    .classed("model-container", true)
    .style("position", "relative")
    .style("padding-bottom", (100 * model.height / model.width) + "%")
    .append("svg")
    .attr("preserveAspectRatio", "xMinYMin meet")
    .attr("viewBox", "0 0 " + model.width + " " + model.height)
      .classed("model-content-responsive", true)
  model.svg = model.svgParent.append("g")

  model.rpcSendCount = 0
  model.svg.append("text")
    .attr("class", "stats")
    .attr("id", "rpc-count")
    .attr("x", 20)
    .attr("y", 32)

  model.bytesXfer = 0
  model.svg.append("text")
    .attr("class", "stats")
    .attr("id", "bytes-xfer")
    .attr("x", 20)
    .attr("y", 54)

  model.svg.append("text")
    .attr("class", "stats")
    .attr("id", "elapsed")
    .attr("x", model.width-20)
    .attr("y", 32)
    .style("text-anchor", "end")

  // Add control group to hold play or reload button.
  model.controls = model.svgParent.append("g")
  model.controls.append("rect")
    .attr("class", "controlscreen")
  model.controls.append("image")
    .attr("class", "button-image")
    .attr("x", "50%")
    .attr("y", "50%")
    .attr("width", 200)
    .attr("height", 200)
    .attr("transform", "translate(-100,-100)")
    .on("click", function() { model.start() })

  model.layout()

  if (enableControls) {
    row = div.append("table")
      .attr("width", "100%")
      .append("tr")
    for (var i = 0; i < model.datacenters.length; i++) {
      var td = row.append("td")
          .attr("align", "center")
      td.append("input")
        .attr("class", "btn-addnode")
        .attr("type", "button")
        .attr("value", "Add Node")
        .on("click", function() {
          addNode(model.index, i)
        })
      td.append("input")
        .attr("class", "btn-addapp")
        .attr("type", "button")
        .attr("value", "Add App")
        .on("click", function() {
          addApp(model.index, i)
        })
    }
  }
}

function removeModel(model) {
  d3.select("#" + model.id).select(".model-container").remove()
}

function layoutModel(model) {
  if (model.svg == null) return

  var forceNodeSel = model.svg.selectAll(".forcenode"),
      forceLinkSel = model.svg.selectAll(".forcelink"),
      linkSel = model.svg.selectAll(".link")

  if (model.force == null) {
    model.force = d3.layout.force()
      .nodes(model.forceNodes)
      .links(model.forceLinks)
      .gravity(0)
      .charge(0)
      .linkDistance(function(d) { return d.distance })
      .size([model.width, model.height])
  }

  forceNodeSel = forceNodeSel.data(model.force.nodes(), function(d) { return d.id })
  model.skin.node(model, forceNodeSel.enter().append("g")
                  .attr("id", function(d) { return d.id })
                  .attr("class", "forcenode")
                  .attr("transform", function(d) { return "translate(-" + d.radius + ",-" + d.radius + ")" }))
  forceNodeSel.exit().remove()

  forceLinkSel = forceLinkSel.data(model.force.links(), function(d) { return d.source.id + "-" + d.target.id })
  forceLinkSel.enter().insert("line", ".node")
    .attr("id", function(d) { return d.source.id + "-" + d.target.id })
    .attr("class", function(d) { return "forcelink " + d.clazz })
  forceLinkSel.exit().remove()
  forceLinkSel.moveToBack()

  linkSel = linkSel.data(model.links, function(d) { return d.source.id + "-" + d.target.id })
  linkSel.enter().insert("line", ".node")
    .attr("id", function(d) { return d.source.id + "-" + d.target.id })
    .attr("class", function(d) { return "link " + d.clazz })
  linkSel.exit().remove()
  linkSel.moveToBack()

  model.controls.transition()
    .duration(100 * timeScale)
    .attr("visibility", model.stopped ? "visible" : "hidden")
  model.controls.select(".button-image")
    .attr("xlink:href", model.played ? "app/js/sim/reload-button.png" : "app/js/sim/play-button.png")

  model.force.on("tick", function(e) {
    forceNodeSel
      .each(gravity(0.2 * e.alpha))
      .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")" })
    forceLinkSel.attr("x1", function(d) { return d.source.x })
      .attr("y1", function(d) { return d.source.y })
      .attr("x2", function(d) { return d.target.x })
      .attr("y2", function(d) { return d.target.y })
    linkSel.attr("x1", function(d) { return d.source.x })
      .attr("y1", function(d) { return d.source.y })
      .attr("x2", function(d) { return d.target.x })
      .attr("y2", function(d) { return d.target.y })
  })

  model.force.start()
}

function setNodeHealthy(model, n) {
}

function setNodeUnreachable(model, n, endFn) {
  model.svg.select("#" + n.id).selectAll(".roachnode")
}

function gravity(alpha, dc) {
  return function(d) {
    d.x += (d.dc.cx - d.x) * alpha
    d.y += (d.dc.cy - d.y) * alpha
  }
}

function packRanges(model, n) {
  if (model.svg == null) return
  model.skin.packRanges(model, n, model.svg.select("#" + n.id).selectAll(".range"))
}

function setAppClass(model, n) {
  model.svg.select("#" + n.id).selectAll("circle").attr("class", n.clazz)
}

// Animate circle which is the request along the link. If the supplied
// endFn returns false, show a quick red flash around the source node.
function sendRequest(model, payload, link, reverse, endFn) {
  // Light up link connection to show activity.
  if (link.source.clazz == "roachnode" || link.source.clazz == "switch") {
    var stroke = "#aaa"
    if (payload instanceof HeartbeatPayload) {
      stroke = payload.color()
    }
    var width = Math.min(3, payload.radius())
    model.svg.select("#" + link.source.id + "-" + link.target.id)
      .transition()
      .duration(0.8 * link.latency * timeScale)
      .style("stroke-width", width)
      .style("stroke", stroke)
      .transition()
      .duration(0.2 * link.latency * timeScale)
      .style("stroke-width", 0)
      .style("stroke", "#aaa")
  }

  var source = link.source,
      target = link.target
  if (reverse) {
    source = link.target
    target = link.source
  }

  var circle = model.svg.append("circle")
  circle.attr("class", "request")
    .attr("fill", payload.color())
    .attr("cx", source.x)
    .attr("cy", source.y)
    .attr("r", payload.radius())
    .transition()
    .ease("linear")
    .duration(link.latency * timeScale)
    .attrTween("cx", function(d, i, a) {return function(t) { return source.x + (target.x - source.x) * t }})
    .attrTween("cy", function(d, i, a) {return function(t) { return source.y + (target.y - source.y) * t }})
    .each("end", function() {
      circle.remove()
      model.rpcSendCount++
      model.bytesXfer += payload.size * model.unitSize
      model.svg.select("#rpc-count").text("RPCs: " + model.rpcSendCount)
      model.svg.select("#bytes-xfer").text("MBs: " + Math.round(model.bytesXfer / (1<<20)))
      model.svg.select("#elapsed").text("Elapsed: " + Number(model.elapsed() / 1000).toFixed(1) + "s")
      if (!endFn()) {
        model.svg.select("#" + target.id).append("circle")
          .attr("r", payload.radius())
          .attr("class", "request node-full")
          .transition()
          .duration(75 * timeScale)
          .attr("r", target.radius * 1.2)
          .transition()
          .remove()
      }
    })
}

function clearRequests(model) {
  var sel = model.svg.selectAll(".request")
  sel.transition().duration(0).remove()
}

module.exports.mountModel = mountModel;
module.exports.layoutModel = layoutModel;
module.exports.viewWidth = viewWidth;
module.exports.viewHeight = viewHeight;
module.exports.timeScale = timeScale;
module.exports.color = color;
module.exports.packRanges = packRanges;
module.exports.sendRequest = sendRequest;
module.exports.setAppClass = setAppClass