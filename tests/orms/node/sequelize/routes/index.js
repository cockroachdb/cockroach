var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.type("text/plain");
  res.send('Node.js Sequelize test server');
});

router.get('/ping', function(req, res, next) {
  res.type("text/plain");
  res.send('node/sequelize');
});

module.exports = router;
