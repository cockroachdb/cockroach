var timeScale = require("./visualization.js").timeScale;

function Circles() {
}

Circles.prototype.node = function(model, sel) {
  return sel.append("circle")
    .attr("r", function(d) { return d.radius })
  //.on("click", function(d) { d.clicked() })
    .attr("class", function(d) { return d.clazz })
    .call(model.force.drag)
}

Circles.prototype.packRanges = function(model, n, sel) {
  var packed = d3.layout.pack()
      .size([n.radius - 4, n.radius - 4])
      .value(function(d) { return d.size })
      .radius(model.replicaRadius.bind(model))
      .nodes({children: n.children, size: 0})
  packed.shift()

  sel = sel.data(packed, function(d) { return d.range.id })
  sel.enter().append("circle")
    .attr("class", "range")
    .attr("id", function(d) { return d.range.id })
    .style("fill", function(d) { return d.color })
  sel.exit().remove()
  sel.transition()
    .duration(250 * timeScale)
    .attr("cx", function(d) { return d.x - n.radius / 2 + 2 })
    .attr("cy", function(d) { return d.y - n.radius / 2 + 2 })
    .attr("r", function(d) { return d.r })
    .style("stroke-width", function(d) { return d.flushed ? 0 : 1 })
}

module.exports = Circles