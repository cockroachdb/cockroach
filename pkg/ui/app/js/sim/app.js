var timeScale = require("./visualization.js").timeScale;
var DataPayload = require("./request.js").DataPayload;
var Request = require("./request.js").Request;

function App(id, x, y, link /* graph link to node */, roachNode, model, dc) {
  this.id = id
  this.x = x
  this.y = y
  this.radius = model.appRadius
  this.clazz = "app"
  link.source = this
  this.link = link
  this.links = {}
  this.links[roachNode.id] = link
  this.roachNode = roachNode
  this.roachNode.app = this
  this.retries = 0
  this.stopped = true
  this.model = model
  this.dc = dc
  this.dc.addApp(this)
}

App.prototype.run = function() {
  if (this.stopped) return
  this.write()
  this.resetTimeout()
}

App.prototype.start = function() {
  this.stopped = false
  this.resetTimeout()
}

App.prototype.stop = function() {
  clearTimeout(this.timeout)
  this.stopped = true
}

App.prototype.resetTimeout = function() {
  clearTimeout(this.timeout)
  var that = this
  var timeout = Math.max(this.model.minAppXfer * timeScale, Math.random() * this.model.appXfer * timeScale)
  timeout = Math.min(this.model.maxAppXfer * timeScale, Math.pow(2, this.retries) * timeout)
  this.timeout = setTimeout(function() { that.run() }, timeout)
}

App.prototype.backoff = function() {
  this.retries++
  this.resetTimeout()
  if (this.retries == 1) {
    this.clazz = "app backoff"
    this.model.setAppClass(this)
  }
}

App.prototype.success = function() {
  if (this.retries == 0) {
    return
  }
  this.clazz = "app"
  this.retries = 0
  this.resetTimeout()
  this.model.setAppClass(this)
}

// Send a randomly sized request from app to a randomly chosen range.
App.prototype.write = function() {
  if (this.model.ranges.length == 0) {
    return
  }
  var idx = Math.floor(Math.random() * this.model.ranges.length)
  if (this.model.ranges[idx].leader != null) {
    var size = Math.random() * this.model.reqSize
    req = new Request(new DataPayload(size), this.model.ranges[idx].leader, this, this.model)
    req.route(this.roachNode, null)
  }
}

module.exports = App