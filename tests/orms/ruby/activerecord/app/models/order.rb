class Order < ApplicationRecord
  # TODO(jordan): remove when default values on decimal columns are supported.
  # See https://github.com/cockroachdb/cockroach/issues/13993.
  validates :subtotal, presence: true

  belongs_to :customer

  has_many :order_products
  has_many :products, through: :order_products
end
