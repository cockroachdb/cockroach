function Request(payload, destReplica, app, model) {
  this.id = "req" + model.reqCount++
  this.payload = payload
  this.payload.req = this
  this.destReplica = destReplica
  this.originApp = app
  this.replicated = {}
  this.links = []
  this.done = false
  this.model = model
}

Request.prototype.clone = function(newDestReplica) {
  var cloned = new Request(this.payload.clone(), newDestReplica, null, this.model)
  cloned.id = this.id
  cloned.replicated = this.replicated
  return cloned
}

Request.prototype.size = function() {
  return this.payload.size
}

Request.prototype.send = function(link, endFn) {
  this.model.sendRequest(this.payload, link, false, endFn)
}

Request.prototype.applySuccess = function() {
  this.payload.applySuccess()
}

Request.prototype.propagateError = function() {
  if (this.links.length == 0) {
    // Notify the app if origin is set.
    if (this.originApp != null) {
      this.originApp.backoff()
      return false
    }
    return true
  }
  link = this.links.pop()
  var that = this
  // Propagate error backwards using sendRequest with reverse=true and
  // an endFn which recursively descends into the link stack.
  var error_payload = new ErrorPayload(this.payload)
  this.model.sendRequest(error_payload, link, true, function() { return that.propagateError() })
  return true
}

Request.prototype.route = function(sourceNode, endFn) {
  var destNode = this.destReplica.roachNode
  if (destNode.id in sourceNode.links) {
    this.writeDirect(sourceNode, destNode, endFn)
    return
  }
  // Need to route to a new datacenter...
  if (sourceNode.clazz == "dc") {
    // Datacenter to datacenter.
    this.writeDirect(sourceNode, destNode.dc, endFn)
  } else if (sourceNode.id.startsWith(sourceNode.dc.id + "app")) {
    // App to app's node.
    this.writeDirect(sourceNode, sourceNode.roachNode, endFn)
  } else {
    // If we're not at a datacenter switch, go to the one in our datacenter first.
    this.writeDirect(sourceNode, sourceNode.dc, endFn)
  }
}

Request.prototype.writeDirect = function(sourceNode, targetNode, endFn) {
  // Route the request.
  if (!(targetNode.id in sourceNode.links)) {
    throw "missing link from " + sourceNode.id + " to " + targetNode.id + "; ignoring."
    return true
  }
  var link = sourceNode.links[targetNode.id]

  // Animate the request from the app's graph node along the graph
  // link to the roachnode's graph node.
  this.links.push(link)
  var that = this
  this.send(link, function() {
    // Check if the link target has reached the destination replica.
    var destNode = that.destReplica.roachNode
    if (targetNode != destNode) {
      // If we're not at the correct node yet, route.
      that.route(targetNode, endFn)
      return true
    } else {
      // Check if the leader has changed; if so, and this request has
      // not been replicated yet, we need to forward to the leader.
      if (that.replicated.length == 0 && !that.destReplica.isLeader()) {
        that.destReplica = that.destReplica.range.leader
        that.route(targetNode, endFn)
        //console.log(req.id + " being reforwarded to changed leader")
        return true
      }
      // We've arrived at the correct replica; try to add the request
      // to the replica. If there's no space here and we're the
      // leader, fail the request immediately (i.e. skip forwarding to
      // followers).
      that.success = that.payload.canSucceed()
      if (endFn != null) {
        endFn()
      } else {
        that.destReplica.range.add(that)
        if (!that.success && that.destReplica.isLeader()) {
          //console.log(req.id + " arrived at full leader; propagating error without forwarding")
          that.propagateError()
        }
      }
      return that.success
    }
    return true
  })
}

function DataPayload(size) {
  this.size = size
}

DataPayload.prototype.clone = function() {
  return new DataPayload(this.size)
}

DataPayload.prototype.color = function() {
  return this.req.destReplica.color
}

DataPayload.prototype.radius = function() {
  return this.req.model.replicaRadius(this.size)
}

DataPayload.prototype.canSucceed = function() {
  return this.req.destReplica.hasSpace(this.size, true /* count log */)
}

DataPayload.prototype.applySuccess = function() {
  this.req.destReplica.add(this.req)
}

// SplitPayload contains information necessary to effect a split request.
// The new range created by the split and the new replicas are provided
// as arguments.
function SplitPayload(newRange, newReplicas) {
  this.newRange = newRange
  this.newReplicas = newReplicas
  this.size = 0
}

SplitPayload.prototype.clone = function() {
  return new SplitPayload(this.newRange, this.newReplicas)
}

SplitPayload.prototype.color = function() {
  return "#ee0"
}

SplitPayload.prototype.radius = function() {
  return 4
}

SplitPayload.prototype.canSucceed = function() {
  return true
}

SplitPayload.prototype.applySuccess = function() {
  this.req.destReplica.split(this.newReplicas[this.req.destReplica.id])
}

// HeartbeatPayload contains information necessary to effect a heartbeat request.
function HeartbeatPayload() {
  this.size = 0
}

HeartbeatPayload.prototype.clone = function() {
  return new HeartbeatPayload()
}

HeartbeatPayload.prototype.color = function() {
  return "#f00"
}

HeartbeatPayload.prototype.radius = function() {
  return 2
}

HeartbeatPayload.prototype.canSucceed = function() {
  return true
}

HeartbeatPayload.prototype.applySuccess = function() {
  this.req.destReplica.heartbeat()
}

// ErrorPayload is sent on a failed request.
function ErrorPayload(orig_payload) {
  this.size = orig_payload.size
}

ErrorPayload.prototype.clone = function() {
  return new ErrorPayload()
}

ErrorPayload.prototype.color = function() {
  return "#f00"
}

ErrorPayload.prototype.radius = function() {
  return 2
}

ErrorPayload.prototype.canSucceed = function() {
  return true
}

ErrorPayload.prototype.applySuccess = function() {
}

module.exports.Request = Request
module.exports.ErrorPayload = ErrorPayload
module.exports.HeartbeatPayload = HeartbeatPayload
module.exports.SplitPayload = SplitPayload
module.exports.DataPayload = DataPayload
