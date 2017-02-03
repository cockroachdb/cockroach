var Model = require("./model.js");
var Visualization = require("./visualization.js");
var Datacenter = require("./datacenter.js");

function initModel(el, model) {
  var viewHeight = Visualization.viewHeight;
  var viewWidth = Visualization.viewWidth;
  model.nodeRadius = 20
  model.nodeDistance = 80
  model.desiredReplicas = 5

  new Datacenter(viewWidth * 0.2, viewHeight * 0.50, model)
  new Datacenter(viewWidth * 0.33, viewHeight * 0.933, model)
  new Datacenter(viewWidth * 0.5, viewHeight * 0.333, model)
  new Datacenter(viewWidth * 0.66, viewHeight * 0.933, model)
  new Datacenter(viewWidth * 0.8, viewHeight * 0.50, model)
  for (var i = 0; i < model.datacenters.length; i++) {
    for (var j = 0; j < 5; j++) {
      Model.addNodeToModel(model, model.datacenters[i])
    }
    Model.addAppToModel(model, model.datacenters[i])
  }
  Visualization.mountModel(el, model, false /* controls, currently broken */)
}

function runVisualization(el) {
    new Model.Model(
        "model",
        Visualization.viewWidth,
        Visualization.viewHeight * 1.33,
        function(m) {
            initModel(el, m);
        }
    );
}

exports.runVisualization = runVisualization;
exports.viewHeight = Visualization.viewHeight;
exports.viewWidth = Visualization.viewWidth;