'use strict';

var fs        = require('fs');
var Sequelize = require('sequelize-cockroachdb');
var sequelize = new Sequelize(process.env.ADDR, {});
var DataTypes = Sequelize.DataTypes;

if (!Sequelize.supportsCockroachDB) {
  throw new Error("CockroachDB dialect for Sequelize not installed");
}

if (process.env.ADDR === undefined) {
  throw new Error("ADDR (database URL) must be specified.");
}

module.exports.Customer = sequelize.define('customer', {
  name: DataTypes.STRING
}, {
  instanceMethods: {
    // jsonObject returns an object that JSON.stringify can trivially
    // convert into the JSON response the test driver expects.
    jsonObject: function() {
      return {
        id: parseInt(this.id),
        name: this.name
      };
    }
  },
  timestamps: false
});

module.exports.Order = sequelize.define('order', {
  customer_id: DataTypes.INTEGER,
  subtotal: DataTypes.DECIMAL(18, 2)
}, {
  instanceMethods: {
    // jsonObject returns an object that JSON.stringify can trivially
    // convert into the JSON response the test driver expects.
    jsonObject: function() {
      return{
        id: parseInt(this.id),
        subtotal: this.subtotal,
        customer: {
          id: this.customer_id
        }
      };
    }
  },
  timestamps: false
});

module.exports.OrderProduct = sequelize.define('order_products', {
  order_id: {
    type: DataTypes.INTEGER,
    primaryKey: true
  },
  product_id: {
    type: DataTypes.INTEGER,
    primaryKey: true
  }
}, {
  timestamps: false
});

module.exports.Product = sequelize.define('product', {
  name: DataTypes.STRING,
  price: DataTypes.DECIMAL(18, 2)
}, {
  instanceMethods: {
    // jsonObject returns an object that JSON.stringify can trivially
    // convert into the JSON response the test driver expects.
    jsonObject: function() {
      return {
        id: parseInt(this.id),
        name: this.name,
        price: this.price,
      };
    }
  },
  timestamps: false
});

module.exports.sequelize = sequelize;
module.exports.Sequelize = Sequelize;
