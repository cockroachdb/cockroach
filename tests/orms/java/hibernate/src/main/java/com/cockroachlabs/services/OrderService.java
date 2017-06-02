package com.cockroachlabs.services;

import com.cockroachlabs.model.Order;
import com.cockroachlabs.util.SessionUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import javax.ws.rs.*;
import java.io.IOException;
import java.util.List;

@Path("/order")
public class OrderService {

    private final ObjectMapper mapper = new ObjectMapper();

    @GET
    @Produces("application/json")
    public String getOrders() {
        try (Session session = SessionUtil.getSession()) {
            Query query = session.createQuery("from Order");
            List orders = query.list();
            return mapper.writeValueAsString(orders);
        } catch (JsonProcessingException e) {
            return e.toString();
        }
    }

    @POST
    @Produces("application/json")
    public String createOrder(String body) {
        try (Session session = SessionUtil.getSession()) {
            Order newOrder = mapper.readValue(body, Order.class);
            session.save(newOrder);

            return mapper.writeValueAsString(newOrder);
        } catch (IOException e) {
            return e.toString();
        }
    }

    @GET
    @Path("/{orderID}")
    @Produces("application/json")
    public String getOrder(@PathParam("orderID") long orderID) {
        try (Session session = SessionUtil.getSession()) {
            Order order = session.get(Order.class, orderID);
            if (order == null) {
                throw new NotFoundException();
            }

            return mapper.writeValueAsString(order);
        } catch (JsonProcessingException e) {
            return e.toString();
        }
    }

    @PUT
    @Path("/{orderID}")
    @Produces("application/json")
    public String updateOrder(@PathParam("orderID") long orderID, String body) {
        try (Session session = SessionUtil.getSession()) {
            Order updateInfo = mapper.readValue(body, Order.class);
            updateInfo.setId(orderID);

            Order updatedOrder = (Order) session.merge(updateInfo);
            return mapper.writeValueAsString(updatedOrder);
        } catch (IOException e) {
            return e.toString();
        }
    }

    @DELETE
    @Path("/{orderID}")
    @Produces("text/plain")
    public String deleteOrder(@PathParam("orderID") long orderID) {
        try (Session session = SessionUtil.getSession()) {
            Transaction tx = session.beginTransaction();

            Query deleteReferencing = session.createQuery("delete from Order where order_id = :id");
            deleteReferencing.setParameter("id", orderID);
            deleteReferencing.executeUpdate();

            Query deleteOrder = session.createQuery("delete from Order where id = :id");
            deleteOrder.setParameter("id", orderID);

            int rowCount = deleteOrder.executeUpdate();
            if (rowCount == 0) {
                tx.rollback();
                throw new NotFoundException();
            }
            tx.commit();
            return "ok";
        }
    }

}
