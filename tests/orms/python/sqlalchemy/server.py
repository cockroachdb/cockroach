#! /usr/bin/env python

# Copyright 2017 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.

from __future__ import print_function

# Import Flask code.
from flask import Flask, Response, request

# Import SQLAlchemy code.
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy.orm

# This import isn't actually referenced directly below but is included to
# clearly indicate whether the CockroachDB dialect is missing.
import cockroachdb

from models import db, Customer, Order, Product

import argparse
from decimal import Decimal
import json
import logging
import os

DEFAULT_URL = "cockroachdb://root@localhost:26257/company_sqlalchemy?sslmode=disable"

# Setup and return Flask app.
def setup_app():
    # Setup Flask app.
    app = Flask(__name__)
    app.config.from_pyfile('server.cfg')
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("ADDR", DEFAULT_URL)
    app.debug = False

    # Disable log messages, which are outputted to stderr and treated as errors
    # by the test driver.
    if not app.debug:
        logging.getLogger('werkzeug').setLevel(logging.ERROR)

    # Initialize flask-sqlachemy.
    db.init_app(app)
    with app.test_request_context():
        db.create_all()

    return app

app = setup_app()


@app.route('/ping')
def ping():
    return 'python/sqlalchemy'


# The following functions respond to various HTTP routes, as described in the
# top-level README.md.

@app.route('/customer', methods=['GET'])
def get_customers():
    customers = [c.as_dict() for c in Customer.query.all()]
    return Response(json.dumps(customers), 200,
                    mimetype="application/json; charset=UTF-8")


@app.route('/customer', methods=['POST'])
def create_customer():
    try:
        body = request.stream.read().decode('utf-8')
        data = json.loads(body)
        customer = Customer(id=data.get('id'), name=data['name'])
        db.session.add(customer)
        db.session.commit()
    except (ValueError, KeyError) as e:
        return Response(str(e), 400)
    return body


@app.route('/customer/<id>', methods=['GET'])
def get_customer(id=None):
    if id is None:
        return Response('no ID specified', 400)
    customer = db.session.query(Customer).get(int(id))
    return Response(json.dumps(customer.as_dict()), 200,
                    mimetype="application/json; charset=UTF-8")


@app.route('/order', methods=['GET'])
def get_orders():
    orders = [o.as_dict() for o in Order.query.all()]
    return Response(json.dumps(orders), 200,
                    mimetype="application/json; charset=UTF-8")


@app.route('/order', methods=['POST'])
def create_order():
    try:
        body = request.stream.read().decode('utf-8')
        data = json.loads(body)
        order = Order(
            id=data.get('id'),
            subtotal=Decimal(data['subtotal']),
            customer_id=data['customer']['id'])
        db.session.add(order)
        db.session.commit()
    except (ValueError, KeyError) as e:
        return Response(str(e), 400)
    return body


@app.route('/order/<id>', methods=['GET'])
def get_order(id=None):
    if id is None:
        return Response('no ID specified', 400)
    order = db.session.query(Order).get(int(id))
    return Response(json.dumps(order.as_dict()), 200,
                    mimetype="application/json; charset=UTF-8")


@app.route('/product', methods=['GET'])
def get_products():
    products = [p.as_dict() for p in Product.query.all()]
    return Response(json.dumps(products), 200,
                    mimetype="application/json; charset=UTF-8")


@app.route('/product', methods=['POST'])
def create_product():
    try:
        body = request.stream.read().decode('utf-8')
        data = json.loads(body)
        product = Product(id=data.get('id'), name=data['name'], price=Decimal(data['price']))
        db.session.add(product)
        db.session.commit()
    except (ValueError, KeyError) as e:
        return Response(str(e), 400)
    return body


@app.route('/product/<id>', methods=['GET'])
def get_product(id=None):
    if id is None:
        return Response('no ID specified', 400)
    product = db.session.query(Product).get(int(id))
    return Response(json.dumps(product.as_dict()), 200,
                    mimetype="application/json; charset=UTF-8")


def main():
    parser = argparse.ArgumentParser(
        description='Test SQLAlchemy compatibility with CockroachDB.')
    parser.add_argument('--port', dest='port', type=int,
                        help='server listen port', default=6543)
    args = parser.parse_args()
    print('listening on port %d' % args.port)
    app.run(host='localhost', port=args.port)


if __name__ == '__main__':
    main()
