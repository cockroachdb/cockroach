var viewWidth = require("./visualization.js").viewWidth;
var viewHeight = require("./visualization.js").viewHeight;

function Datacenter(longitude, latitude, model) {
  this.index = model.dcCount++
  this.id = "dc" + this.index
  this.radius = model.dcRadius
  this.clazz = "dc"
  this.apps = []
  this.roachNodes = []
  this.nodeLinks = []
  this.model = model
  this.location = [longitude, latitude]
  this.links = {}

  // Link this datacenter to all others.
  var loc = model.projection(this.location)
  for (var i = 0; i < this.model.datacenters.length; i++) {
    var dc = this.model.datacenters[i]
    var dcLoc = model.projection(dc.location)
    latency = 4000 * Math.sqrt((loc[0] - dcLoc[0]) * (loc[0] - dcLoc[0]) + (loc[1] - dcLoc[1]) * (loc[1] - dcLoc[1])) / viewWidth
    var l = {id: "link" + this.model.linkCount++, source: this, target: dc, clazz: "dclink", latency: latency}
    this.links[dc.id] = l
    var rl = {id: "link" + this.model.linkCount++, source: dc, target: this, clazz: "dclink", latency: latency}
    dc.links[this.id] = rl
    this.model.dcLinks.push(l)
    this.model.dcLinks.push(rl)
  }

  // Add to array of datacenters.
  this.model.datacenters.push(this)
}

Datacenter.prototype.addNode = function(rn) {
  // Link this node to the node.
  var clazz = "link"
  if (!this.model.useSwitches) {
    clazz = ""
  }
  l = {id: "link" + this.model.linkCount++, source: rn, target: this, clazz: clazz, distance: this.model.nodeDistance, latency: this.model.dcLatency}
  rn.links[this.id] = l
  rl = {id: "link" + this.model.linkCount++, source: this, target: rn, clazz: clazz, distance: this.model.nodeDistance, latency: this.model.dcLatency}
  this.links[rn.id] = rl

  // Add new node & update visualization.
  this.roachNodes.push(rn)
  this.nodeLinks.push(l)
  this.model.layout()
}

Datacenter.prototype.removeNode = function(rn) {
  var index = this.roachNodes.indexOf(this)
  if (index != -1) {
    this.roachNodes.splice(index, 1)
  }
  var index = this.nodeLinks.indexOf(this.links[rn.id])
  if (index != -1) {
    this.nodeLinks.splice(index, 1)
  }
  for (var i = 0, keys = Object.keys(rn.links); i < keys.length; i++) {
    var l = rn.links[keys[i]]
    var rl = l.target.links[rn.id]
    delete l.target.links[rn.id]
  }
  if (rn.app != null) {
    this.removeApp(rn.app)
  }
  this.model.layout()
}

// Note that we've disabled visualization of apps. They now send
// requests directly from the gateway node they're connected to.
Datacenter.prototype.addApp = function(app) {
  /*
  if (this.dcNode != null) {
    // Link to node node.
    app.datacenterLink = {source: app, target: this.dcNode, clazz: "", distance: this.model.nodeDistance + this.model.appDistance(), latency: 0.25}
  }

  // Add link from app to node.
  */

  this.apps.push(app)
  //this.model.layout()
}

Datacenter.prototype.removeApp = function(app) {
  app.stop()
  var index = this.apps.indexOf(app)
  if (index != -1) {
    this.apps.splice(index, 1)
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