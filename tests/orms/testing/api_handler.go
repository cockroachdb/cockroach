package testing

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/tests/orms/go/gorm/model"
)

const (
	applicationAddr = "localhost:6543"
	applicationURL  = "http://" + applicationAddr

	pingPath      = applicationURL + "/ping"
	customersPath = applicationURL + "/customer"
	ordersPath    = applicationURL + "/order"
	productsPath  = applicationURL + "/product"

	jsonContentType = "application/json"
)

// apiHandler takes care of communicating with the application api. It uses GORM's models
// for convenient JSON marshalling/unmarshalling, but this format should be the same
// across all ORMs.
type apiHandler struct{}

func (apiHandler) canDial() bool {
	conn, err := net.Dial("tcp", applicationAddr)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (apiHandler) ping(expected string) error {
	resp, err := http.Get(pingPath)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("HTTP error %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if body := strings.TrimSpace(string(b)); body != expected {
		return errors.Errorf("ping response %s != expected %s", body, expected)
	}
	return err
}

func (apiHandler) queryCustomers() ([]model.Customer, error) {
	var customers []model.Customer
	if err := getJSON(customersPath, &customers); err != nil {
		return nil, err
	}
	return customers, nil
}
func (apiHandler) queryProducts() ([]model.Product, error) {
	var products []model.Product
	if err := getJSON(productsPath, &products); err != nil {
		return nil, err
	}
	return products, nil
}
func (apiHandler) queryOrders() ([]model.Order, error) {
	var orders []model.Order
	if err := getJSON(ordersPath, &orders); err != nil {
		return nil, err
	}
	return orders, nil
}

func (apiHandler) createCustomer(name string) error {
	customer := model.Customer{Name: &name}
	return postJSONData(customersPath, customer)
}
func (apiHandler) createProduct(name string, price float64) error {
	product := model.Product{Name: &name, Price: price}
	return postJSONData(productsPath, product)
}
func (apiHandler) createOrder(customerID, productID int, subtotal float64) error {
	order := model.Order{
		Customer: model.Customer{ID: customerID},
		Products: []model.Product{{ID: productID}},
		Subtotal: subtotal,
	}
	return postJSONData(ordersPath, order)
}

func getJSON(path string, result interface{}) error {
	resp, err := http.Get(path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}
	bbytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	body := string(bbytes)
	return errors.Wrapf(json.Unmarshal(bbytes, result), "getJSON(%s) : %s", path, body)
}

func postJSONData(path string, body interface{}) error {
	var bodyBuf bytes.Buffer
	if err := json.NewEncoder(&bodyBuf).Encode(body); err != nil {
		return err
	}

	resp, err := http.Post(path, jsonContentType, &bodyBuf)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("HTTP error %d: %s", resp.StatusCode, resp.Status)
	}
	return nil
}

// These functions clean any non-deterministic fields, such as IDs that are
// generated upon row creation.
func cleanCustomers(customers []model.Customer) []model.Customer {
	for i := range customers {
		customers[i].ID = 0
	}
	return customers
}
func cleanProducts(products []model.Product) []model.Product {
	for i := range products {
		products[i].ID = 0
	}
	return products
}
func cleanOrders(orders []model.Order) []model.Order {
	for i := range orders {
		orders[i].ID = 0
		orders[i].Customer = model.Customer{}
		orders[i].CustomerID = 0
		orders[i].Products = nil
	}
	return orders
}
