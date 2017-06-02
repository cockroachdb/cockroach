class Product < ApplicationRecord
  validates :name, presence: true
  validates :price, presence: true

  has_many :order_products
  has_many :orders, through: :order_products
end
