class OrdersController < ApplicationController
  def index
    render json: Order.all
  end

  def show
    render json: Order.find(params[:id])
  end

  def create
    p = order_params
    p[:customer_id] = params[:customer][:id]
    p[:product_ids] = params[:products].map{|p| p[:id]}

    redirect_to Order.create!(p)
  end

  def update
    Order.find(params[:id]).update(order_params)
  end

  def destroy
    Order.find(params[:id]).destroy
    render plain: ok
  end

  private
    def order_params
      params.require(:order).permit(:subtotal, :customer_id, :product_ids)
    end
end
