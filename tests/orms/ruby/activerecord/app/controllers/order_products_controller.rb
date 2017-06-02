class OrderProductsController < ApplicationController
  def create
    order = Order.find(params[:order_id])
    product = Product.find(params[:product_id])
    order.products.push(product)
    order.subtotal += p.price
    o.save!

    render plain: "ok"
  end
end
