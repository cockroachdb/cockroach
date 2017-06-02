package com.cockroachlabs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.cockroachlabs.services.CustomerService;
import com.cockroachlabs.services.OrderService;
import com.cockroachlabs.services.PingService;
import com.cockroachlabs.services.ProductService;
import com.cockroachlabs.util.SessionUtil;
import org.glassfish.jersey.netty.httpserver.NettyHttpContainerProvider;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class Application {

    @Parameter(names = "-addr", description = "the address of the database")
    private String dbAddr;

    public static void main(String[] args) {
        Application app = new Application();
        new JCommander(app, args);
        app.run();
    }

    private void run() {
        initHibernate();
        initHTTPServer();
    }

    private void initHibernate() {
        SessionUtil.init(dbAddr);
    }

    private void initHTTPServer() {
        URI baseUri = UriBuilder.fromUri("http://localhost/").port(6543).build();
        ResourceConfig resourceConfig = new ResourceConfig(
                PingService.class,
                CustomerService.class,
                ProductService.class,
                OrderService.class
        );
        NettyHttpContainerProvider.createServer(baseUri, resourceConfig, true);
    }

}
