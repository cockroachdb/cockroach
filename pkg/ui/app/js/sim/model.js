var Circles = require("./circles.js");
var RoachNode = require("./node.js");
var Range = require("./range.js");
var Replica = require("./replica.js");
var App = require("./app.js");

// This file defines a simple model for describing a CockroachDB cluster.
function randomOffset(x, y, radius) {
  x = x + Math.floor(Math.random() * radius)
  y = y + Math.floor(Math.random() * radius)
  return [x, y]
}

function addNodeToModel(model, dc) {
  // Offset new node randomly.
  var x = dc.cx,
      y = dc.cy
  if (dc.roachNodes.length > 0) {
    var last = dc.roachNodes[dc.roachNodes.length-1]
    offset = randomOffset(last.x, last.y, model.nodeRadius)
    x = offset[0]
    y = offset[1]
  }

  // Create the new node, identified by node ID.
  var nodeID = dc.roachNodes.length
  rn = new RoachNode(dc.id + "node" + nodeID, x, y, model, dc)

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
  var offset = randomOffset(rn.x, rn.y, model.nodeRadius)
  var x = offset[0]
  var y = offset[1]

  // Create the new application, identified by app ID.
  var l = {source: null, target: rn, clazz: "applink", distance: model.appDistance(), latency: model.dcLatency}
  var app = new App(dc.id + "app" + dc.apps.length, x, y, l, rn, model, dc)
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

var layoutModel = require("./visualization").layoutModel;
let packRanges = require("./visualization").packRanges;
let sendRequest = require("./visualization").sendRequest;
let setAppClass = require("./visualization").setAppClass;

var modelCount = 0
var models = []

function Model(id, width, height, initFn) {
  this.index = modelCount++
  this.id = id
  this.width = width
  this.height = height
  this.initFn = initFn
  this.nodeRadius = 35
  this.appRadius = 10
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

  this.skin = new Circles()
  this.force = null
  this.forceNodes = []
  this.forceLinks = []
  this.links = []
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
    if (dc.blackHole != null) {
      dc.blackHole.links = {}
    }
    dc.apps = []
    dc.roachNodes = []
  }
  this.ranges = []
  this.datacenters = []

  this.clearRequests()
  while (this.forceNodes.length > 0) {
    this.forceNodes.pop()
  }
  while (this.forceLinks.length > 0) {
    this.forceLinks.pop()
  }
  while (this.links.length > 0) {
    this.links.pop()
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



module.exports.Model = Model;
module.exports.randomOffset = randomOffset; 
module.exports.addNodeToModel = addNodeToModel;
module.exports.addAppToModel = addAppToModel;
module.exports.restart = restart;
module.exports.addNode = addNode;
module.exports.addApp = addApp;
