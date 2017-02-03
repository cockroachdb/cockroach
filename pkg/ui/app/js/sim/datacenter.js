var viewWidth = require("./visualization.js").viewWidth;
var viewHeight = require("./visualization.js").viewHeight;

function Datacenter(cx, cy, model) {
  this.index = model.dcCount++
  this.id = "dc" + this.index
  this.cx = cx
  this.cy = cy
  this.apps = []
  this.roachNodes = []
  this.model = model
  this.blackHole = {id: "blackhole" + this.index, x: this.cx, y: this.cy, fixed: true, radius: 3, clazz: "switch", dc: this, links: {}}
  this.model.forceNodes.push(this.blackHole)
  if (!this.model.useSwitches) {
    this.blackHole.radius = 0
  }

  // Link this datacenter to all others.
  for (var i = 0; i < this.model.datacenters.length; i++) {
    var dc = this.model.datacenters[i]
    latency = 4000 * Math.sqrt((cx - dc.cx) * (cx - dc.cx) + (cy - dc.cy) * (cy - dc.cy)) / viewWidth
    var l = {id: "link" + this.model.linkCount++, source: this.blackHole, target: dc.blackHole, clazz: "dclink", latency: latency}
    this.blackHole.links[dc.blackHole.id] = l
    var rl = {id: "link" + this.model.linkCount++, source: dc.blackHole, target: this.blackHole, clazz: "dclink", latency: latency}
    dc.blackHole.links[this.blackHole.id] = rl
    // Use non-force links.
    this.model.links.push(l)
    this.model.links.push(rl)
  }

  // Add to array of datacenters.
  this.model.datacenters.push(this)
}

Datacenter.prototype.addNode = function(rn) {
  // Link this node to the blackHole.
  var clazz = "switchlink"
  if (!this.model.useSwitches) {
    clazz = ""
  }
  l = {id: "link" + this.model.linkCount++, source: rn, target: this.blackHole, clazz: clazz, distance: this.model.nodeDistance, latency: this.model.dcLatency}
  rn.links[this.blackHole.id] = l
  rl = {id: "link" + this.model.linkCount++, source: this.blackHole, target: rn, clazz: clazz, distance: this.model.nodeDistance, latency: this.model.dcLatency}
  this.blackHole.links[rn.id] = rl
  this.model.forceLinks.push(l)
  this.model.forceLinks.push(rl)

  // Add new node & update visualization.
  this.roachNodes.push(rn)
  this.buildInterNodeLinks()
  this.model.forceNodes.push(rn)
  this.model.layout()
}

Datacenter.prototype.removeNode = function(rn) {
  var index = this.roachNodes.indexOf(this)
  if (index != -1) {
    this.roachNodes.splice(index, 1)
  }
  index = this.model.forceNodes.indexOf(this)
  if (index != -1) {
    this.model.forceNodes.splice(index, 1)
  }
  for (var i = 0, keys = Object.keys(rn.links); i < keys.length; i++) {
    var l = rn.links[keys[i]]
    index = this.model.forceLinks.indexOf(l)
    if (index != -1) {
      this.model.forceLinks.splice(index, 1)
    }
    var rl = l.target.links[rn.id]
    delete l.target.links[rn.id]
    index = this.model.forceLinks.indexOf(rl)
    if (index != -1) {
      this.model.forceLinks.splice(index, 1)
    }
  }
  if (rn.app != null) {
    this.removeApp(rn.app)
  }
  this.buildInterNodeLinks()
  this.model.layout()
}

Datacenter.prototype.buildInterNodeLinks = function() {
  // Create forward and backward connections from each node to all other nodes in the datacenter.
  for (var i = 0; i < this.roachNodes.length; i++) {
    source = this.roachNodes[i]
    for (var j = i + 1; j < this.roachNodes.length; j++) {
      target = this.roachNodes[j]
      // Compute internode distance, which is length of chord separating nodes.
      var distance = j - i
      var half = this.roachNodes.length / 2
      if (distance > half) {
        distance = i + (this.roachNodes.length - j)
      }
      var angle = Math.PI * 0.5 / (half / distance)
      var interNodeDistance = Math.abs((2 * this.model.nodeDistance) * Math.sin(angle))
      //console.log("internodedistance: " + interNodeDistance + ", angle: " + angle)
      if (!(target.id in source.links)) {
        var l = {id: "link" + this.model.linkCount++, source: source, target: target, clazz: "nodelink", latency: this.model.dcLatency}
        source.links[target.id] = l
        var rl = {id: "link" + this.model.linkCount++, source: target, target: source, clazz: "nodelink", latency: this.model.dcLatency}
        target.links[source.id] = rl
        this.model.forceLinks.push(source.links[target.id])
        this.model.forceLinks.push(target.links[source.id])
      }
      source.links[target.id].distance = interNodeDistance
      target.links[source.id].distance = interNodeDistance
    }
  }
}

Datacenter.prototype.addApp = function(app) {
  if (this.blackHole != null) {
    // Link to blackHole node.
    app.blackholeLink = {source: app, target: this.blackHole, clazz: "", distance: this.model.nodeDistance + this.model.appDistance(), latency: 0.25}
    this.model.forceLinks.push(app.blackholeLink)
  }

  // Add link from app to node.
  this.model.forceLinks.push(app.link)

  // Add new app & update visualization.
  this.apps.push(app)
  this.model.forceNodes.push(app)
  this.model.layout()
}

Datacenter.prototype.removeApp = function(app) {
  app.stop()
  var index = this.apps.indexOf(app)
  if (index != -1) {
    this.apps.splice(index, 1)
  }
  index = this.model.forceNodes.indexOf(app)
  if (index != -1) {
    this.model.forceNodes.splice(index, 1)
  }
  index = this.model.forceLinks.indexOf(app.link)
  if (index != -1) {
    this.model.forceLinks.splice(index, 1)
  }
  if (app.blackholeLink != null) {
    index = this.model.forceLinks.indexOf(app.blackholeLink)
    if (index != -1) {
      this.model.forceLinks.splice(index, 1)
    }
  }
  this.model.layout()
}

Datacenter.prototype.leaderCount = function() {
  var count = 0
  for (var i = 0; i < this.roachNodes.length; i++) {
    count += this.roachNodes[i].leaderCount()
  }
  return count
}

module.exports = Datacenter