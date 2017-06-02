var express = require('express');
var models = require('../models');
var router = express.Router();

// GET all customers.
router.get('/', function(req, res, next) {
  models.Customer.findAll().then(function(customers) {
    var result = customers.map(function(customer) { return customer.jsonObject(); });
    res.json(result);
  }).catch(next);
});

// POST a new customer.
router.post('/', function(req, res, next) {
  var c = {id: req.body.id, name: req.body.name};
  models.Customer.create(c).then(function(customer) {
    res.json(customer.jsonObject());
  }).catch(next);
});

// GET one customer.
router.get('/:id', function(req, res, next) {
  var id = parseInt(req.params.id);
  models.Customer.findById(id).then(function(customer) {
    res.json(customer.jsonObject());
  }).catch(next);
});

module.exports = router;
