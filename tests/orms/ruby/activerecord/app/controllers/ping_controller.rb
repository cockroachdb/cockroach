class PingController < ActionController::API
  def ping
    render plain: "ruby/activerecord"
  end
end
