var d3 = require("d3");
var Model = require("./model.js");
var Visualization = require("./visualization.js");
var Datacenter = require("./datacenter.js");
var AggV1 = require("./aggv1.js");

function initModel(el, model) {
  model.nodeRadius = 10
  model.nodeDistance = 30
  model.desiredReplicas = 5
  model.projection = d3.geo.mercator()
  model.skin = new AggV1()
  model.enablePlayAndReload = false
  model.enableAddNodeAndApp = false
  model.displaySimState = false

  new Datacenter(-74.0059, 40.7128, model).label = "New York City";
  new Datacenter(-93.6091, 41.6005, model).label = "Des Moines";
  new Datacenter(-96.7970, 32.7767, model).label = "Dallas";
  new Datacenter(-122.0839, 37.3861, model).label = "Mountain";
  new Datacenter(-122.3321, 47.6062, model).label = "Seattle";
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