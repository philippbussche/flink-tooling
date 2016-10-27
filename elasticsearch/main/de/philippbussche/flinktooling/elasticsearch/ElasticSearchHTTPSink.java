package de.philippbussche.flinktooling.elasticsearch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;

import com.google.common.base.Supplier;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;

import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.core.Update;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * Created by philipp on 09/09/16.
 */
public class ElasticSearchHTTPSink<T> extends RichSinkFunction<T> {

    private final Map<String, String> userConfig;
    private final String index;
    private final String type;
    private transient JestClient client;

    public ElasticSearchHTTPSink(Map<String, String> userConfig) {
        this.userConfig = userConfig;
        this.index = userConfig.get("index");
        this.type = userConfig.get("type");
    }

    public SSLContext getSSLContext() {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                    return true;
                }
            }).build();
            return sslContext;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void open(Configuration configuration) {
        if(userConfig.get("location").equals("aws")) {
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(userConfig.get("awsAccessKey"), userConfig.get("awsSecurityKey"));
            AWSStaticCredentialsProvider awsProvider = new AWSStaticCredentialsProvider(awsCreds);
            final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
            final AWSSigner awsSigner = new AWSSigner(awsProvider, userConfig.get("awsRegion"), userConfig.get("awsService"), clock);
            AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);

            JestClientFactory factory = new JestClientFactory() {
                @Override
                protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
                    builder.addInterceptorLast(requestInterceptor);
                    return builder;
                }

                @Override
                protected HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder builder) {
                    builder.addInterceptorLast(requestInterceptor);
                    return builder;
                }
            };

            // this is bad.
            HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;

            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(this.getSSLContext(), hostnameVerifier);
            SchemeIOSessionStrategy httpsIOSessionStrategy = new SSLIOSessionStrategy(this.getSSLContext(), hostnameVerifier);

            factory.setHttpClientConfig(new HttpClientConfig.Builder(userConfig.get("awsEndpoint"))
                            .defaultSchemeForDiscoveredNodes("https") // required, otherwise uses http
                            .sslSocketFactory(sslSocketFactory) // this only affects sync calls
                            .httpsIOSessionStrategy(httpsIOSessionStrategy) // this only affects async calls
                            .build()
            );
            client = factory.getObject();
        } else if(userConfig.get("location").equals("local")) {
            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(new HttpClientConfig
                    .Builder("http://" + userConfig.get("esNode") + ":" + userConfig.get("esPort"))
                    .multiThreaded(true)
                    .build());
            client = factory.getObject();
        }

    }

    @Override
    public void invoke(T t) throws Exception {
        if(t != null) {
            /*
             * This is where the data will be sinked into ES. In my use case I actually had a document already so I first had to retrieve that
             Get get = new Get.Builder(index, myObject.myIdentifyingProperty).type(type).build();
             JestResult result = client.execute(get);
             * I then had to construct an updated document, some counter was incremented (these counters are used for giving more weight to certain documents in a search)
             String updateDoc = "{ \"doc\": { \"" + myObject.someProperty + "Counter\": \"" + Integer.toString(counter) + "\" } }";
             * I then got the id of the document retrieved
             String id = result.getJsonObject().get("_id").getAsString();
             * And finally pushed the updated document with the id back into ES
             client.execute(new Update.Builder(updateDoc).index(index).type(type).id(id).build());
             * Obviously in a perfect world one would want to somehow bulk insert and even bulk update things in the invoke method instead.
            */
        }
    }

    @Override
    public void close() {
        client.shutdownClient();
    }
}
