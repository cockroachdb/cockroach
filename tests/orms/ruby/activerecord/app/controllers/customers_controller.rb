class CustomersController < ActionController::API
  def index
    render json: Customer.all
  end

  def show
    render json: Customer.find(params[:id])
  end

  def create
    redirect_to Customer.create!(customer_params)
  end

  def update
    Customer.find(params[:id]).update(customer_params)
  end

  def destroy
    Customer.find(params[:id]).destroy
    render plain: "ok"
  end

  private
    def customer_params
      params.require(:customer).permit(:name)
    end
end
