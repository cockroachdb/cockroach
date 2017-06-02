var express = require('express');
var models = require('../models');
var router = express.Router();

// GET all products from database.
router.get('/', function(req, res, next) {
  models.Product.findAll().then(function(products) {
    var result = products.map(function(product) { return product.jsonObject(); });
    res.json(result);
  }).catch(next);
});

// POST a new product to the database.
router.post('/', function(req, res, next) {
  var id = req.body.id || null;
  var p = {
    id: id,
    name: req.body.name,
    price: parseFloat(req.body.price)
  };
  models.Product.create(p).then(function(product) {
    res.json(product.jsonObject());
  }).catch(next);
});

// GET one product using its id, from the database.
router.get('/:id', function(req, res, next) {
  var id = parseInt(req.params.id);
  models.Product.findById(id).then(function(product) {
    res.json(product.jsonObject());
  }).catch(next);
});

module.exports = router;
