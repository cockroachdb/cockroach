class ProductsController < ApplicationController
  def index
    render json: Product.all
  end

  def show
    render json: Product.find(params[:id])
  end

  def create
    redirect_to Product.create!(product_params)
  end

  def update
    Product.find(params[:id]).update(product_params)
  end

  def destroy
    Product.find(params[:id]).destroy
    render plain: "ok"
  end

  private
    def product_params
      params.require(:product).permit(:name, :price)
    end
end
