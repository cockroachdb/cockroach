var timeScale = require("./visualization.js").timeScale;
var DataPayload = require("./request.js").DataPayload;
var Request = require("./request.js").Request;

// Creates a replica and adds it to the roach node and the
// range.
function Replica(size, range, roachNode, add, model) {
  this.id = "replica" + model.replicaCount++
  this.size = size
  this.logIndex = 0
  this.flushed = true
  this.color = range.color
  this.range = range
  this.roachNode = roachNode
  this.model = model
  this.splitting = false
  this.splitEpoch = 0
  this.stopped = true
  if (add) {
    this.roachNode.replicas.push(this)
    this.range.addReplica(this)
    this.range.flushLog()
  }
}

Replica.prototype.isLeader = function() {
  return this == this.range.leader
}

Replica.prototype.hasSpace = function(size, countLog) {
  return this.roachNode.hasSpace(size, countLog)
}

Replica.prototype.getSize = function(countLog) {
  var size = this.size
  if (countLog) {
    for (var i = this.logIndex; i < this.range.log.length; i++) {
      var req = this.range.log[i]
      size += req.size()
    }
  }
  return size
}

Replica.prototype.start = function() {
  this.stopped = false
  this.setTimeout(this.model.periodicInterval * timeScale * Math.random())
}

Replica.prototype.stop = function() {
  clearTimeout(this.timeout)
  this.stopped = true
}

Replica.prototype.run = function() {
  if (this.stopped) return
  if (!this.roachNode.down()) {
    this.replicate()
    this.rebalance()
    this.leadership()
  }
  this.setTimeout(this.model.periodicInterval * timeScale)
}

Replica.prototype.setTimeout = function(timeout) {
  var that = this
  clearTimeout(this.timeout)
  this.timeout = setTimeout(function() { that.run() }, timeout)
}

Replica.prototype.replicate = function() {
  if (this.roachNode.busy || !this.isLeader() || this.range.replicas.length == this.model.repFactor()) return
  // Choose target and create new replica.
  var that = this
  var targetNode = this.chooseReplicateTarget(
    function(n) { return n.hasSpace(that.size, true /* count log */) },
    function(nA, nB) { return nA.nonSplitting() < nB.nonSplitting() })
  if (targetNode == null) {
    return
  }
  // Set nodes busy until replication is complete.
  this.roachNode.setBusy(true)
  targetNode.setBusy(true)
  var newReplica = new Replica(this.size, this.range, targetNode, false, this.model)
  newReplica.logIndex = this.logIndex
  newReplica.flushed = this.flushed
  newReplica.splitEpoch = this.splitEpoch
  // Send the replicated snapshot.
  var req = new Request(new DataPayload(this.size), newReplica, null, this.model)
  req.route(this.roachNode, function() {
    that.roachNode.setBusy(false)
    targetNode.setBusy(false)
    targetNode.replicas.push(newReplica)
    that.range.addReplica(newReplica)
    that.range.flushLog()
    that.model.packRanges(targetNode)
    if (!newReplica.range.stopped) {
      newReplica.start()
    }
    return true
  })
  // Forward any requests which are still pending quorum from the leader.
  for (var i = 0, keys = Object.keys(this.range.reqMap); i < keys.length; i++) {
    var req = this.range.reqMap[keys[i]]
    this.range.forwardReqToReplica(req, newReplica)
  }
}

Replica.prototype.rebalance = function() {
  if (this.roachNode.busy) return
  var that = this
  var targetNode = this.chooseRebalanceTarget(
    function(n) { return n.hasSpace(that.size, true /* count log */) },
    function(nA, nB) { return nA.nonSplitting() < nB.nonSplitting() })
  if (targetNode == null) return
  // Set nodes busy until rebalance is complete.
  this.roachNode.setBusy(true)
  targetNode.setBusy(true)
  var newReplica = new Replica(this.size, this.range, targetNode, false, this.model)
  newReplica.logIndex = this.logIndex
  newReplica.flushed = this.flushed
  newReplica.splitEpoch = this.splitEpoch
  // Send the replicated snapshot.
  var req = new Request(new DataPayload(this.size), newReplica, null, this.model)
  req.route(this.roachNode, function() {
    that.stop()
    that.roachNode.setBusy(false)
    targetNode.setBusy(false)
    // Remove this replica from its node.
    var index = that.roachNode.replicas.indexOf(that)
    if (index == -1) return
    that.roachNode.replicas.splice(index, 1)
    index = that.range.replicas.indexOf(that)
    if (index == -1) return
    that.range.replicas.splice(index, 1)
    targetNode.replicas.push(newReplica)
    that.range.addReplica(newReplica)
    that.range.flushLog()
    that.model.packRanges(that.roachNode)
    that.model.packRanges(targetNode)
    if (!newReplica.range.stopped) {
      newReplica.start()
    }
    return true
  })
}

// Let replicas which are leaders pass leadership to other replicas
// periodically.
Replica.prototype.leadership = function() {
  if (!this.isLeader()) return
  var nodeLeaderCount = this.model.leaderCount(this.roachNode)
  //console.log("considering leadership change for " + this.range.id + " from " + this.id + ", node " + this.roachNode.id + " lc=" + nodeLeaderCount)
  // Choose target and create new replica.
  var that = this
  var targetNode = this.chooseExisting(
    function(n) { return n != that.roachNode && that.model.leaderCount(n) < nodeLeaderCount - 1 },
    function(nA, nB) { return that.model.leaderCount(nA) < that.model.leaderCount(nB) })
  if (targetNode == null) {
    //console.log("found no suitable target for leadership change for " + this.range.id + " from " + this.id)
    return
  }
  for (var i = 0; i < this.range.replicas.length; i++) {
    if (this.range.replicas[i].roachNode == targetNode) {
      //console.log("changing leadership for " + this.range.id + " from " + this.id + " to " + this.range.replicas[i].id)
      this.range.leader = this.range.replicas[i]
      return
    }
  }
}

// Chooses a target which returns true for filterFn and sorts lowest
// on the scoreFn.
Replica.prototype.chooseExisting = function(filterFn, scoreFn) {
  var best = null
  for (var i = 0; i < this.range.replicas.length; i++) {
    if (this.range.replicas[i].splitting) continue
    var rn = this.range.replicas[i].roachNode
    // Skip any nodes which are currently busy rebalancing or already part of the range.
    //console.log("candidate " + rn.id + " busy: " + rn.busy + " filtered? " + !filterFn(rn) + " lc=" + rn.leaderCount())
    if (rn.down() || rn.busy || !filterFn(rn)) continue
    if (best == null || scoreFn(rn, best)) {
      best = rn
    }
  }
  return best
}

// Chooses a target from another datacenter (or if there's only one,
// use that), which returns true for filterFn and sorts lowest on the
// scoreFn.
Replica.prototype.chooseReplicateTarget = function(filterFn, scoreFn) {
  var dcExist = {}
  var repExist = {}
  // Create a set of existing replica IDs for this range.
  for (var i = 0; i < this.range.replicas.length; i++) {
    dcExist[this.range.replicas[i].roachNode.dc.id] = true
    repExist[this.range.replicas[i].roachNode.id] = true
  }

  var dcs = []
  // If there's only one datacenter, use it.
  if (this.model.datacenters.length == 1) {
    dcs = [this.roachNode.dc]
  } else {
    for (var i = 0; i < this.model.datacenters.length; i++) {
      var dc = this.model.datacenters[i]
      if (!(dc.id in dcExist)) {
        dcs.push(dc)
      }
    }
  }

  var best = null
  for (var i = 0; i < dcs.length; i++) {
    for (var j = 0; j < dcs[i].roachNodes.length; j++) {
      var rn = dcs[i].roachNodes[j]
      // Skip any nodes which are currently busy rebalancing or already part of the range.
      if (rn.down() || rn.busy || (rn.id in repExist) || !filterFn(rn)) continue
      if (best == null || scoreFn(rn, best)) {
        best = rn
      }
    }
  }
  return best
}

// Chooses a target from within the specified datacenter which returns
// true for filterFn and sorts lowest on the scoreFn.
Replica.prototype.chooseRebalanceTarget = function(filterFn, scoreFn) {
  var repExist = {}
  // Create a set of existing replica IDs for this range.
  for (var i = 0; i < this.range.replicas.length; i++) {
    repExist[this.range.replicas[i].roachNode.id] = true
  }

  var dc = this.roachNode.dc
  var mean = 0
  var candidates = []
  for (var i = 0; i < dc.roachNodes.length; i++) {
    var rn = dc.roachNodes[i]
    mean += rn.nonSplitting()
    // Skip any nodes which are currently busy rebalancing or already part of the range.
    if (rn.down() || rn.busy || (rn.id in repExist) || !filterFn(rn)) continue
    candidates.push(rn)
  }
  mean /= dc.roachNodes.length

  var reqDistance = 0.5
  if (this.model.exactRebalancing) {
    reqDistance = 0
  }
  var best = null
  for (var i = 0; i < candidates.length; i++) {
    var rn = candidates[i]
    if (this.roachNode.nonSplitting() - mean < reqDistance || mean - rn.nonSplitting() < reqDistance) continue
    if (best == null || scoreFn(rn, best)) {
      best = rn
    }
  }

  return best
}

// Writes data to the replica.
Replica.prototype.add = function(req) {
  // Update the node once the data has been set.
  this.size += req.size()
  if (req.originApp != null) {
    req.originApp.success()
  }
}

// Heartbeats in this model are for show; do nothing.
Replica.prototype.heartbeat = function() {
}

Replica.prototype.split = function(newReplica) {
  var leftover = this.size - this.size / 2
  this.size = this.size / 2
  this.splitEpoch = this.range.nextSplitEpoch
  // If the new replica is null, it means that the original pre-split
  // range was not fully up-replicated at the time of the split, so
  // no replica was created to house the right hand side of this replica's
  // split. That's OK as that RHS will be up-replicated from the new range
  // automatically. We just want to set this replica's size appropriately
  // and return.
  if (newReplica == null) {
    return
  }
  // The first split replica is set as the leader. Having a leader set
  // enables this range to receive app writes.
  if (newReplica.range.leader == null) {
    newReplica.range.leader = newReplica
  }
  newReplica.splitting = false
  newReplica.splitEpoch = this.range.nextSplitEpoch
  newReplica.size = leftover
  newReplica.start() // start now that the split is complete
  //console.log("split " + this.range.id + " " + this.id + " left=" + this.size + ", right=" + leftover)
}

module.exports = Replica