package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cockroachdb/cockroach/tests/orms/go/gorm/model"
	"github.com/jinzhu/gorm"
	"github.com/julienschmidt/httprouter"
)

// Server is an http server that handles REST requests.
type Server struct {
	db *gorm.DB
}

// NewServer creates a new instance of a Server.
func NewServer(db *gorm.DB) *Server {
	return &Server{db: db}
}

// RegisterRouter registers a router onto the Server.
func (s *Server) RegisterRouter(router *httprouter.Router) {
	router.GET("/ping", s.ping)

	router.GET("/customer", s.getCustomers)
	router.POST("/customer", s.createCustomer)
	router.GET("/customer/:customerID", s.getCustomer)
	router.PUT("/customer/:customerID", s.updateCustomer)
	router.DELETE("/customer/:customerID", s.deleteCustomer)

	router.GET("/product", s.getProducts)
	router.POST("/product", s.createProduct)
	router.GET("/product/:productID", s.getProduct)
	router.PUT("/product/:productID", s.updateProduct)
	router.DELETE("/product/:productID", s.deleteProduct)

	router.GET("/order", s.getOrders)
	router.POST("/order", s.createOrder)
	router.GET("/order/:orderID", s.getOrder)
	router.PUT("/order/:orderID", s.updateOrder)
	router.DELETE("/order/:orderID", s.deleteOrder)
	router.POST("/order/:orderID/product", s.addProductToOrder)
}

func (s *Server) ping(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	writeTextResult(w, "go/gorm")
}

func (s *Server) getCustomers(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var customers []model.Customer
	if err := s.db.Find(&customers).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, customers)
	}
}

func (s *Server) createCustomer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var customer model.Customer
	if err := json.NewDecoder(r.Body).Decode(&customer); err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	if err := s.db.Create(&customer).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, customer)
	}
}

func (s *Server) getCustomer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var customer model.Customer
	if err := s.db.Find(&customer, ps.ByName("customerID")).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, customer)
	}
}

func (s *Server) updateCustomer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var customer model.Customer
	if err := json.NewDecoder(r.Body).Decode(&customer); err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	customerID := ps.ByName("customerID")
	if err := s.db.Model(&customer).Where("ID = ?", customerID).Update(customer).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, customer)
	}
}

func (s *Server) deleteCustomer(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	customerID := ps.ByName("customerID")
	req := s.db.Delete(model.Customer{}, "ID = ?", customerID)
	if err := req.Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else if req.RowsAffected == 0 {
		http.Error(w, "", http.StatusNotFound)
	} else {
		writeTextResult(w, "ok")
	}
}

func (s *Server) getProducts(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var products []model.Product
	if err := s.db.Find(&products).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, products)
	}
}

func (s *Server) createProduct(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var product model.Product
	if err := json.NewDecoder(r.Body).Decode(&product); err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	if err := s.db.Create(&product).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, product)
	}
}

func (s *Server) getProduct(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var product model.Product
	if err := s.db.Find(&product, ps.ByName("productID")).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, product)
	}
}

func (s *Server) updateProduct(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var product model.Product
	if err := json.NewDecoder(r.Body).Decode(&product); err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	productID := ps.ByName("productID")
	if err := s.db.Model(&product).Where("ID = ?", productID).Update(product).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, product)
	}
}

func (s *Server) deleteProduct(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	productID := ps.ByName("productID")
	req := s.db.Delete(model.Product{}, "ID = ?", productID)
	if err := req.Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else if req.RowsAffected == 0 {
		http.Error(w, "", http.StatusNotFound)
	} else {
		writeTextResult(w, "ok")
	}
}

func (s *Server) getOrders(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var orders []model.Order
	if err := s.db.Preload("Customer").Preload("Products").Find(&orders).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, orders)
	}
}

func (s *Server) createOrder(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var order model.Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	if order.Customer.ID == 0 {
		http.Error(w, "must specify user", http.StatusBadRequest)
		return
	}
	if err := s.db.Find(&order.Customer, order.Customer.ID).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	for i, product := range order.Products {
		if product.ID == 0 {
			http.Error(w, "must specify a product ID", http.StatusBadRequest)
			return
		}
		if err := s.db.Find(&order.Products[i], product.ID).Error; err != nil {
			http.Error(w, err.Error(), errToStatusCode(err))
			return
		}
	}

	if err := s.db.Create(&order).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, order)
	}
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var order model.Order
	if err := s.db.Preload("Customer").Preload("Products").Find(&order, ps.ByName("orderID")).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, order)
	}
}

func (s *Server) updateOrder(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var order model.Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	orderID := ps.ByName("orderID")
	if err := s.db.Model(&order).Where("ID = ?", orderID).Update(order).Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, order)
	}
}

func (s *Server) deleteOrder(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	orderID := ps.ByName("orderID")
	req := s.db.Delete(model.Order{}, "ID = ?", orderID)
	if err := req.Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else if req.RowsAffected == 0 {
		http.Error(w, "", http.StatusNotFound)
	} else {
		writeTextResult(w, "ok")
	}
}

func (s *Server) addProductToOrder(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tx := s.db.Begin()

	var order model.Order
	orderID := ps.ByName("orderID")
	if err := tx.Preload("Products").First(&order, orderID).Error; err != nil {
		tx.Rollback()
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	const productIDParam = "productID"
	productID := r.URL.Query().Get(productIDParam)
	if productID == "" {
		tx.Rollback()
		writeMissingParamError(w, productIDParam)
		return
	}

	var addedProduct model.Product
	if err := tx.First(&addedProduct, productID).Error; err != nil {
		tx.Rollback()
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	order.Products = append(order.Products, addedProduct)
	if err := tx.Save(&order).Error; err != nil {
		tx.Rollback()
		http.Error(w, err.Error(), errToStatusCode(err))
		return
	}

	if err := tx.Commit().Error; err != nil {
		http.Error(w, err.Error(), errToStatusCode(err))
	} else {
		writeJSONResult(w, order)
	}
}

func writeTextResult(w http.ResponseWriter, res string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, res)
}

func writeJSONResult(w http.ResponseWriter, res interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		panic(err)
	}
}

func writeMissingParamError(w http.ResponseWriter, paramName string) {
	http.Error(w, fmt.Sprintf("missing query param %q", paramName), http.StatusBadRequest)
}

func errToStatusCode(err error) int {
	switch err {
	case gorm.ErrRecordNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
