package es.poc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportOptions;
import co.elastic.clients.transport.rest_client.RestClientOptions;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import static org.elasticsearch.client.RequestOptions.DEFAULT;

public class CreateIndexExample {
    private static final Logger LOGGER = Logger.getLogger(CreateIndexExample.class.getName());

    public static void main(String[] args) throws IOException {
        // Create the low-level client
        var DEFAULT_HEADERS = new BasicHeader[] {new BasicHeader("X-Elastic-Product", "elasticsearch")};
        RestClient restClient = RestClient.builder(
                new HttpHost("192.168.1.229", 9200)).setDefaultHeaders(DEFAULT_HEADERS).build();

// Create the transport with a Jackson mapper
        final RequestOptions.Builder options = DEFAULT.toBuilder();
        options.addHeader("X-Elastic-Product", "Elasticsearch");
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()).withRequestOptions(new RestClientOptions(options.build()));

// And create the API client
        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        createIndex(esClient);
        deleteIndex(esClient);
        System.exit(0);
    }

    private static void createIndex(ElasticsearchClient esClient) throws IOException {
        Product product = new Product();
        product.setId(11);
        product.setName("test");

        try {
            esClient.indices().create(i -> i.index("ew-index-poc"));
        } catch (Exception e){
            //ignore
        }


        IndexResponse response = esClient.index(i -> i
                .index("ew-index-poc")
                .id(UUID.randomUUID().toString())
                .document(product)
        );

        LOGGER.info(response.toString());
    }

    private static void deleteIndex(ElasticsearchClient esClient) throws IOException {
        try {
            DeleteIndexResponse response = esClient
                    .indices().delete(builder -> builder.index(List.of("ew-index-poc")));
            LOGGER.info(response.toString());
        } catch (Exception e) {
            //ignore
        }

    }
}
