package com.cockroachlabs.services;

import com.cockroachlabs.model.Product;
import com.cockroachlabs.util.SessionUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import javax.ws.rs.*;
import java.io.IOException;
import java.util.List;

@Path("/product")
public class ProductService {

    private final ObjectMapper mapper = new ObjectMapper();

    @GET
    @Produces("application/json")
    public String getProducts() {
        try (Session session = SessionUtil.getSession()) {
            Query query = session.createQuery("from Product");
            List products = query.list();
            return mapper.writeValueAsString(products);
        } catch (JsonProcessingException e) {
            return e.toString();
        }
    }

    @POST
    @Produces("application/json")
    public String createProduct(String body) {
        try (Session session = SessionUtil.getSession()) {
            Product newProduct = mapper.readValue(body, Product.class);
            session.save(newProduct);

            return mapper.writeValueAsString(newProduct);
        } catch (IOException e) {
            return e.toString();
        }
    }

    @GET
    @Path("/{productID}")
    @Produces("application/json")
    public String getProduct(@PathParam("productID") long productID) {
        try (Session session = SessionUtil.getSession()) {
            Product product = session.get(Product.class, productID);
            if (product == null) {
                throw new NotFoundException();
            }

            return mapper.writeValueAsString(product);
        } catch (JsonProcessingException e) {
            return e.toString();
        }
    }

    @PUT
    @Path("/{productID}")
    @Produces("application/json")
    public String updateProduct(@PathParam("productID") long productID, String body) {
        try (Session session = SessionUtil.getSession()) {
            Product updateInfo = mapper.readValue(body, Product.class);
            updateInfo.setId(productID);

            Product updatedProduct = (Product) session.merge(updateInfo);
            return mapper.writeValueAsString(updatedProduct);
        } catch (IOException e) {
            return e.toString();
        }
    }

    @DELETE
    @Path("/{productID}")
    @Produces("text/plain")
    public String deleteProduct(@PathParam("productID") long productID) {
        try (Session session = SessionUtil.getSession()) {
            Transaction tx = session.beginTransaction();

            Query deleteReferencing = session.createQuery("delete from Order where product_id = :id");
            deleteReferencing.setParameter("id", productID);
            deleteReferencing.executeUpdate();

            Query deleteProduct = session.createQuery("delete from Product where id = :id");
            deleteProduct.setParameter("id", productID);

            int rowCount = deleteProduct.executeUpdate();
            if (rowCount == 0) {
                tx.rollback();
                throw new NotFoundException();
            }
            tx.commit();
            return "ok";
        }
    }

}
