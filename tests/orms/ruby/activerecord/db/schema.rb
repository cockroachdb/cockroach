# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 20170207160810) do

  create_table "customers", id: :bigint, default: -> { "unique_rowid()" }, force: :cascade do |t|
    t.text "name", null: false
  end

  create_table "order_products", id: false, force: :cascade do |t|
    t.bigint "order_id",   null: false
    t.bigint "product_id", null: false
    t.index ["order_id"], name: "index_order_products_on_order_id"
    t.index ["product_id"], name: "index_order_products_on_product_id"
  end

  create_table "orders", id: :bigint, default: -> { "unique_rowid()" }, force: :cascade do |t|
    t.decimal "subtotal",    null: false
    t.bigint  "customer_id", null: false
    t.index ["customer_id"], name: "index_orders_on_customer_id"
  end

  create_table "products", id: :bigint, default: -> { "unique_rowid()" }, force: :cascade do |t|
    t.text    "name",  null: false
    t.decimal "price", null: false
  end

  add_foreign_key "order_products", "orders"
  add_foreign_key "order_products", "products"
  add_foreign_key "orders", "customers"
end
