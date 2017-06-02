var express = require('express');
var models = require('../models');
var router = express.Router();

// GET all orders from database.
router.get('/', function(req, res, next) {
  models.Order.findAll().then(function(orders) {
    var result = orders.map(function(order) { return order.jsonObject(); });
    res.json(result);
  }).catch(next);
});

// POST a new order to the database.
router.post('/', function(req, res, next) {
  var o = {
    id: parseInt(req.body.id),
    subtotal: parseFloat(req.body.subtotal),
    customer_id: parseInt(req.body.customer.id)
  };
  models.Order.create(o).then(function(order) {
    res.json(order.jsonObject());
  }).catch(next);
});

// GET one order using its id, from the database.
router.get('/:id', function(req, res, next) {
  var id = parseInt(req.params.id);
  models.Order.findById(id).then(function(order) {
    res.json(order.jsonObject());
  }).catch(next);
});

module.exports = router;
