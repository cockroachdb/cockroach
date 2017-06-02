package com.cockroachlabs.services;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/ping")
public class PingService {

    @GET
    @Produces("text/plain")
    public String ping() {
        return "java/hibernate";
    }

}
