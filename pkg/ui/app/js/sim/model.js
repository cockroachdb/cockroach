var Circles = require("./circles.js");
var RoachNode = require("./node.js");
var Range = require("./range.js");
var Replica = require("./replica.js");
var App = require("./app.js");
var layoutModel = require("./visualization.js").layoutModel;
let packRanges = require("./visualization.js").packRanges;
let sendRequest = require("./visualization.js").sendRequest;

// This file defines a simple model for describing a CockroachDB cluster.

var modelCount = 0
var models = []

function Model(id, width, height, initFn) {
  this.index = modelCount++
  this.id = id
  this.width = width
  this.height = height
  this.initFn = initFn
  this.dcRadius = 30
  this.nodeRadius = 35
  this.appRadius = 0
  this.nodeDistance = 150
  this.interNodeDistance = 25
  this.nodeCapacity = 3.0
  this.reqSize = 0.1
  this.unitSize = 64<<20
  this.splitSize = 1.0
  this.dcLatency = 200 // in ms
  this.appXfer = 3000           // in ms
  this.minAppXfer = 1000        // in ms
  this.maxAppXfer = 10000       // in ms
  this.heartbeatInterval = 1500 // in ms
  this.periodicInterval = 1000  // in ms
  this.ranges = []
  this.datacenters = []
  this.dcCount = 0
  this.reqCount = 0
  this.linkCount = 0
  this.replicaCount = 0
  this.useSwitches = true
  this.exactRebalancing = false
  this.desiredReplicas = 3
  this.showHeartbeats = false
  this.quiesceRaft = true
  this.stopped = true
  this.played = false

  this.projectionName = "none"
  this.projection = function(p) { return p }
  this.skin = new Circles()
  this.enablePlayAndReload = true
  this.enableAddNodeAndApp = false
  this.dcLinks = []
  models.push(this)

  if (initFn != null) {
    initFn(this)
  }
}

Model.prototype.start = function() {
  this.startTime = Date.now()
  if (this.played) {
    this.restart()
  }
  this.stopped = false
  // If there are no ranges, create the first range with the first
  // node as the only replica.
  if (this.ranges.length == 0) {
    if (this.datacenters.length == 0 || this.datacenters[0].roachNodes.length == 0) {
      alert("There are no nodes; add nodes before starting visualization")
      return
    }
    var range = new Range(this)
    var r = new Replica(0, range, this.datacenters[0].roachNodes[0], true, this)
    range.leader = r
  }

  for (var j = 0; j < this.datacenters.length; j++) {
    var dc = this.datacenters[j]
    for (var i = 0; i < dc.apps.length; i++) {
      dc.apps[i].start()
    }
  }
  for (var i = 0; i < this.ranges.length; i++) {
    this.ranges[i].start()
  }
  if (this.simTime > 0) {
    var that = this
    setTimeout(function() { that.stop() }, this.simTime)
  }
  this.layout()
}

Model.prototype.stop = function() {
  for (var j = 0; j < this.datacenters.length; j++) {
    var dc = this.datacenters[j]
    for (var i = 0; i < dc.apps.length; i++) {
      dc.apps[i].stop()
    }
  }
  for (var i = 0; i < this.ranges.length; i++) {
    this.ranges[i].stop()
  }
  this.stopped = true
  this.played = true
  this.layout()
}

Model.prototype.restart = function() {
  // Remove model.
  removeModel(this)

  // Clean up each datacenter.
  for (var j = 0; j < this.datacenters.length; j++) {
    var dc = this.datacenters[j]
    dc.links = {}
    dc.apps = []
    dc.roachNodes = []
  }
  this.ranges = []
  this.datacenters = []

  this.clearRequests()
  while (this.dcLinks.length > 0) {
    this.dcLinks.pop()
  }

  // Re-initialize from scratch.
  this.initFn(this)
}

Model.prototype.elapsed = function() {
  return Date.now() - this.startTime
}

Model.prototype.appDistance = function() {
  return this.nodeRadius * 1.5 + this.appRadius
}

Model.prototype.capConstant = function() {
  return Math.sqrt(this.nodeCapacity / Math.PI) / (this.nodeRadius * 0.70)
}

Model.prototype.replicaRadius = function(size) {
  return Math.sqrt(size / Math.PI) / this.capConstant()
}

Model.prototype.repFactor = function() {
  if (this.desiredReplicas < 3) {
    return 3
  }
  return this.desiredReplicas
}

// leaderCount returns either the number of replicas held by the node
// which are leaders of their respective ranges, or, if there is more
// than one datacenter in the model, the total number of replicas
// across all nodes in the passed node's datacenter.
Model.prototype.leaderCount = function(roachNode) {
  if (this.datacenters.length > 1) {
    return roachNode.dc.leaderCount()
  }
  return roachNode.leaderCount()
}

Model.prototype.sendRequest = function(payload, link, reverse, endFn) {
  sendRequest(this, payload, link, reverse, endFn)
}

Model.prototype.layout = function() {
  layoutModel(this)
  for (var i = 0; i < this.ranges.length; i++) {
    this.ranges[i].flushLog()
  }
}

Model.prototype.packRanges = function(node) {
  packRanges(this, node)
}

Model.prototype.setAppClass = function(app) {
  setAppClass(this, app)
}

Model.prototype.clearRequests = function() {
  clearRequests(this)
}

Model.prototype.setNodeHealthy = function(node) {
  setNodeHealthy(this, node)
}

Model.prototype.setNodeUnreachable = function(node, endFn) {
  setNodeUnreachable(this, node, endFn)
}

function addNodeToModel(model, dc) {
  // Create the new node, identified by node ID.
  var nodeID = dc.roachNodes.length
  rn = new RoachNode(dc.id + "node" + nodeID, 0, 0, model, dc)

  return rn
}

function addAppToModel(model, dc) {
  var nodes = dc.roachNodes
  var avail = []
  for (var j = 0; j < nodes.length; j++) {
    if (nodes[j].app == null) {
      avail.push(nodes[j])
    }
  }
  if (avail.length == 0) {
    alert("There are no nodes without apps; add more nodes")
    return
  }
  var rn = avail[Math.floor(Math.random() * avail.length)]

  // Create the new application, identified by app ID.
  var l = {source: null, target: rn, clazz: "applink", distance: model.appDistance(), latency: model.dcLatency}
  var app = new App(dc.id + "app" + dc.apps.length, 0, 0, l, rn, model, dc)
  return app
}

function restart(modelIdx) {
  var model = models[modelIdx]
  model.restart()
}

function addNode(modelIdx, dcIdx) {
  var model = models[modelIdx]
  var dc = model.datacenters[dcIdx]
  addNodeToModel(model, dc)
}

function addApp(modelIdx, dcIdx) {
  var model = models[modelIdx]
  if (model.datacenters.length == 0) {
    alert("Add a datacenter first!")
    return
  }
  var dc = model.datacenters[dcIdx]
  if (dc.roachNodes.length == 0) {
    alert("Add a node to this datacenter first!")
    return
  }
  addAppToModel(model, dc)
}

module.exports.Model = Model;
module.exports.addNodeToModel = addNodeToModel;
module.exports.addAppToModel = addAppToModel;
module.exports.restart = restart;
module.exports.addNode = addNode;
module.exports.addApp = addApp;
