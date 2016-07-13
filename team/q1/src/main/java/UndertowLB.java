import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.proxy.LoadBalancingProxyClient;
import io.undertow.server.handlers.proxy.ProxyHandler;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Zhangwei on 2/27/16.
 */

public class UndertowLB {

    public static void main(final String[] args) {
        try {
            LoadBalancingProxyClient loadBalancer = new LoadBalancingProxyClient()
                    .addHost(new URI("http://localhost:8081"))
                    .addHost(new URI("http://localhost:8082"))
                    .setConnectionsPerThread(5);

            Undertow reverseProxy = Undertow.builder()
                    .addHttpListener(8080, "localhost")
                    .setIoThreads(3)
                    .setHandler(new ProxyHandler(loadBalancer, 10000, new HttpHandler() {
                        @Override
                        public void handleRequest(HttpServerExchange httpServerExchange) throws Exception {
                            if (httpServerExchange.getResponseCode() == 503) {

                            }
                        }
                    }))
                    .build();
            reverseProxy.start();

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}
