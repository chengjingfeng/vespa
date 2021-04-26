// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package ai.vespa.hosted.client;

import org.apache.hc.client5.http.classic.methods.ClassicHttpRequests;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

import static java.util.Objects.requireNonNull;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.WARNING;

/**
 * @author jonmv
 */
public abstract class AbstractConfigServerClient implements ConfigServerClient {

    private static final Logger log = Logger.getLogger(AbstractConfigServerClient.class.getName());

    /** Executes the request with the given context. The caller must close the response. */
    abstract ClassicHttpResponse execute(ClassicHttpRequest request, HttpClientContext context) throws IOException;

    /** Executes the given request with response/error handling and retries. */
    private <T> T execute(RequestBuilder builder,
                          BiFunction<ClassicHttpResponse, ClassicHttpRequest, T> handler,
                          Consumer<IOException> catcher) {
        HttpClientContext context = HttpClientContext.create();
        context.setRequestConfig(builder.config);

        Throwable thrown = null;
        for (URI host : builder.hosts) {
            ClassicHttpRequest request = ClassicHttpRequests.create(builder.method, concat(host, builder.uriBuilder));
            request.setEntity(builder.entity);
            try {
                try {
                    return handler.apply(execute(request, context), request);
                }
                catch (IOException e) {
                    catcher.accept(e);
                    throw new UncheckedIOException(e); // Throw unchecked if catcher doesn't throw.
                }
            }
            catch (RetryException e) {
                if (thrown == null)
                    thrown = e.getCause();
                else
                    thrown.addSuppressed(e.getCause());

                if (builder.entity != null && ! builder.entity.isRepeatable()) {
                    log.log(WARNING, "Cannot retry " + request + " as entity is not repeatable");
                    break;
                }
                log.log(FINE, request + " failed; will retry", e.getCause());
            }
        }
        if (thrown != null) {
            if (thrown instanceof IOException)
                throw new UncheckedIOException((IOException) thrown);
            else if (thrown instanceof RuntimeException)
                throw (RuntimeException) thrown;
            else
                throw new IllegalStateException("Illegal retry cause: " + thrown.getClass(), thrown);
        }

        throw new IllegalStateException("No hosts to perform the request against");
    }

    /** Append path to the given host, which may already contain a root path. */
    static URI concat(URI host, URIBuilder pathAndQuery) {
        URIBuilder builder = new URIBuilder(host);
        List<String> pathSegments = new ArrayList<>(builder.getPathSegments());
        if ( ! pathSegments.isEmpty() && pathSegments.get(pathSegments.size() - 1).isEmpty())
            pathSegments.remove(pathSegments.size() - 1);
        pathSegments.addAll(pathAndQuery.getPathSegments());
        try {
            return builder.setPathSegments(pathSegments)
                          .setParameters(pathAndQuery.getQueryParams())
                          .build();
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("URISyntaxException should not be possible here", e);
        }
    }

    @Override
    public RequestBuilder send(HostStrategy hosts, Method method) {
        return new RequestBuilder(hosts, method);
    }

    /** Builder for a request against a given set of hosts. */
    class RequestBuilder implements ConfigServerClient.RequestBuilder {

        private final Method method;
        private final HostStrategy hosts;
        private final URIBuilder uriBuilder = new URIBuilder();
        private HttpEntity entity;
        private RequestConfig config = ConfigServerClient.defaultRequestConfig;
        private BiConsumer<ClassicHttpResponse, ClassicHttpRequest> handler = ConfigServerClient::throwOnError;
        private Consumer<IOException> catcher = ConfigServerClient::retryAll;

        private RequestBuilder(HostStrategy hosts, Method method) {
            if ( ! hosts.iterator().hasNext())
                throw new IllegalArgumentException("Host strategy cannot be empty");

            this.hosts = hosts;
            this.method = requireNonNull(method);
        }

        @Override
        public RequestBuilder at(List<String> pathSegments) {
            uriBuilder.setPathSegments(requireNonNull(pathSegments));
            return this;
        }

        @Override
        public ConfigServerClient.RequestBuilder body(byte[] json) {
            return body(HttpEntities.create(json, ContentType.APPLICATION_JSON));
        }

        @Override
        public RequestBuilder body(HttpEntity entity) {
            this.entity = requireNonNull(entity);
            return this;
        }

        @Override
        public RequestBuilder parameters(List<String> pairs) {
            if (pairs.size() % 2 != 0)
                throw new IllegalArgumentException("Must supply parameter key/values in pairs");

            for (int i = 0; i < pairs.size(); )
                uriBuilder.setParameter(pairs.get(i++), pairs.get(i++));

            return this;
        }

        @Override
        public RequestBuilder timeout(Duration timeout) {
            return config(RequestConfig.copy(config)
                                       .setResponseTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                                       .build());
        }

        @Override
        public RequestBuilder config(RequestConfig config) {
            this.config = requireNonNull(config);
            return this;
        }

        @Override
        public RequestBuilder catching(Consumer<IOException> catcher) {
            this.catcher = requireNonNull(catcher);
            return this;
        }

        @Override
        public RequestBuilder handling(BiConsumer<ClassicHttpResponse, ClassicHttpRequest> handler) {
            this.handler = requireNonNull(handler);
            return this;
        }

        @Override
        public <T> T read(Function<byte[], T> mapper) {
            return mapIfSuccess(input -> {
                try (input) {
                    return mapper.apply(input.readAllBytes());
                }
                catch (IOException e) {
                    throw new RetryException(e);
                }
            });
        }

        @Override
        public void discard() throws UncheckedIOException, ResponseException {
            mapIfSuccess(input -> {
                try (input) {
                    return null;
                }
                catch (IOException e) {
                    throw new RetryException(e);
                }
            });
        }

        @Override
        public InputStream stream() throws UncheckedIOException, ResponseException {
            return mapIfSuccess(input -> input);
        }

        /** Returns the mapped body, if successful, retrying any IOException. The caller must close the body stream. */
        private <T> T mapIfSuccess(Function<InputStream, T> mapper) {
            return execute(this,
                           (response, request) -> {
                               try {
                                   handler.accept(response, request); // This throws on unacceptable responses.

                                   InputStream body = response.getEntity() != null ? response.getEntity().getContent()
                                                                                   : InputStream.nullInputStream();
                                   return mapper.apply(new ForwardingInputStream(body) {
                                       @Override
                                       public void close() throws IOException {
                                           super.close();
                                           response.close();
                                       }
                                   });
                               }
                               catch (IOException | RuntimeException | Error e) {
                                   try {
                                       response.close();
                                   }
                                   catch (IOException f) {
                                       e.addSuppressed(f);
                                   }
                                   if (e instanceof IOException) {
                                       catcher.accept((IOException) e);
                                       throw new UncheckedIOException((IOException) e);
                                   }
                                   else
                                       sneakyThrow(e); // e is a runtime exception or an error, so this is fine.
                                   throw new AssertionError("Should not happen");
                               }
                           },
                           catcher);
        }

    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

}