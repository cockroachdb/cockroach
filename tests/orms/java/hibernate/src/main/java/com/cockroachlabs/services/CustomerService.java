package com.cockroachlabs.services;

import com.cockroachlabs.model.Customer;
import com.cockroachlabs.util.SessionUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import javax.ws.rs.*;
import java.io.IOException;
import java.util.List;

@Path("/customer")
public class CustomerService {

    private final ObjectMapper mapper = new ObjectMapper();

    @GET
    @Produces("application/json")
    public String getCustomers() {
        try (Session session = SessionUtil.getSession()) {
            Query query = session.createQuery("from Customer");
            List customers = query.list();
            return mapper.writeValueAsString(customers);
        } catch (JsonProcessingException e) {
            return e.toString();
        }
    }

    @POST
    @Produces("application/json")
    public String createCustomer(String body) {
        try (Session session = SessionUtil.getSession()) {
            Customer newCustomer = mapper.readValue(body, Customer.class);
            session.save(newCustomer);

            return mapper.writeValueAsString(newCustomer);
        } catch (IOException e) {
            return e.toString();
        }
    }

    @GET
    @Path("/{customerID}")
    @Produces("application/json")
    public String getCustomer(@PathParam("customerID") long customerID) {
        try (Session session = SessionUtil.getSession()) {
            Customer customer = session.get(Customer.class, customerID);
            if (customer == null) {
                throw new NotFoundException();
            }

            return mapper.writeValueAsString(customer);
        } catch (JsonProcessingException e) {
            return e.toString();
        }
    }

    @PUT
    @Path("/{customerID}")
    @Produces("application/json")
    public String updateCustomer(@PathParam("customerID") long customerID, String body) {
        try (Session session = SessionUtil.getSession()) {
            Customer updateInfo = mapper.readValue(body, Customer.class);
            updateInfo.setId(customerID);

            Customer updatedCustomer = (Customer) session.merge(updateInfo);
            return mapper.writeValueAsString(updatedCustomer);
        } catch (IOException e) {
            return e.toString();
        }
    }

    @DELETE
    @Path("/{customerID}")
    @Produces("text/plain")
    public String deleteCustomer(@PathParam("customerID") long customerID) {
        try (Session session = SessionUtil.getSession()) {
            Transaction tx = session.beginTransaction();

            Query deleteReferencing = session.createQuery("delete from Order where customer_id = :id");
            deleteReferencing.setParameter("id", customerID);
            deleteReferencing.executeUpdate();

            Query deleteCustomer = session.createQuery("delete from Customer where id = :id");
            deleteCustomer.setParameter("id", customerID);

            int rowCount = deleteCustomer.executeUpdate();
            if (rowCount == 0) {
                tx.rollback();
                throw new NotFoundException();
            }
            tx.commit();
            return "ok";
        }
    }

}
