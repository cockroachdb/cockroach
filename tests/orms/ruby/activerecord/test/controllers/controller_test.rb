require 'test_helper'

class ControllerTest < ActionDispatch::IntegrationTest
  test "should get index" do
    get customers_url
    assert_response :success
  end

  test "customer" do
    post customers_url, params: { name: "hodor" }, as: :json
    assert_response :redirect
    follow_redirect!
    assert_response :success

    c = Customer.find_by(name: "hodor")
    assert_not_nil(c)

    get customer_url, params: { id: c.id }
    assert_response :success
    assert_equal(c.id, json_response["id"])
    assert_equal(c.name, json_response["name"])

    get customers_url
    assert_response :success
    assert_equal(1, json_response.length)
    assert_equal(c.id, json_response[0]["id"])
    assert_equal(c.name, json_response[0]["name"])
  end

  test "product" do
    post products_url, params: { name: "derp", price: 3.4 }, as: :json
    assert_response :redirect
    follow_redirect!
    assert_response :success

    p = Product.find_by(name: "derp")
    assert_not_nil(p)

    get product_url, params: { id: p.id }
    assert_response :success
    assert_equal(p.id, json_response["id"])
    assert_equal(p.name, json_response["name"])

    get products_url
    assert_response :success
    assert_equal(1, json_response.length)
    assert_equal(p.id, json_response[0]["id"])
    assert_equal(p.name, json_response[0]["name"])
  end

  test "order" do
    c = Customer.create!(name: "hodor")
    p = Product.create!(name: "derp", price: 3.4)

    post orders_url, params: { subtotal: 3.4, customer: { id: c.id }, products: [{id: p.id}]}, as: :json
    assert_response :redirect
    follow_redirect!
    assert_response :success

    o = p.orders[0]
    get order_url, params: { id: o.id }
    assert_response :success
    assert_equal(o.id, json_response["id"])
    assert_equal(o.subtotal, json_response["subtotal"].to_f)

    get orders_url
    assert_response :success
    assert_equal(1, json_response.length)
    assert_equal(o.id, json_response[0]["id"])
    assert_equal(o.subtotal, json_response[0]["subtotal"].to_f)
  end

  test "add_product_to_order" do
    c = Customer.create!(name: "hodor")
    p = Product.create!(name: "derp", price: 3.4)
    o = Order.create!(customer: c, products: [p], subtotal: 3.4)

    post order_product_url(o.id), params: { product_id: p.id }, as: :json
    assert_response :success

    o.reload
    assert_equal(2, o.products.length)
    assert_equal(3.4 * 2, o.subtotal)
    assert_equal(o.products[1], p)
  end

  def json_response
    ActiveSupport::JSON.decode @response.body
  end
end
