package es.poc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOptionsBuilders;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.BucketSelectorAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.termvectors.Term;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.json.Json;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

public class EsExample {
    private static final Logger LOGGER = Logger.getLogger(EsExample.class.getName());
    private static final String INDEX_NAME = "products";


    public static void main(String[] args) throws IOException {
        // Create the low-level client
        RestClient restClient = RestClient.builder(
                new HttpHost("elastic01.shop.liv", 9200)).build();

// Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

// And create the API client
        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        SearchResponse<JsonNode> search = esClient.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q
                                .term(t -> t
                                        .field("OPContainerID")
                                        .value(v -> v.stringValue("1654291993"))
                                )),
                JsonNode.class);

        for (Hit<JsonNode> hit: search.hits().hits()) {
            System.out.println(hit.source());
        }

        getById(esClient);

        for (int i = 0; i < 10; i++) {
            System.out.println("========================request page {" + i + "}==========================");
            searchAndPagination(esClient, i, 20);
        }

        System.exit(0);
    }

    private static void getById(ElasticsearchClient esClient) throws IOException {
        GetResponse<JsonNode> response = esClient.get(g -> g
                        .index(INDEX_NAME)
                        .id("1654291993"),
                JsonNode.class
        );

        if (response.found())
            System.out.println(response.source());
    }

    private static void searchAndPagination(ElasticsearchClient esClient, int currentPage, int size) throws IOException {
        Query categoryTerm = TermQuery.of(m -> m
                .boost(2.0f)
                .field("Level2Category")
                .value("Women")
        )._toQuery();

        Query freeShippingTerm = TermQuery.of(m -> m
                .field("FreeShipping")
                .value(true)
        )._toQuery();

        Query level1CategoryTerm = TermQuery.of(m -> m
                .field("ProductCategory.keyword")
                .value("Home Store")
        )._toQuery();

        FunctionScore randomFunctionScore = FunctionScore.of(fs -> fs.filter(categoryTerm)
                .randomScore(RandomScoreFunction.of(rsf -> rsf.field("Level2Category"))));

        Query storeNameTerm = TermQuery.of(m ->
                m.field("StoreName").value("金石堂網路書店"))._toQuery();

        BoolQuery boolQuery = BoolQuery.of(bq ->
                bq.must(List.of(FunctionScoreQuery.of(b -> b.functions(List.of(randomFunctionScore)))._toQuery(), freeShippingTerm))
                        .mustNot(storeNameTerm));

        SortOptions sortOptions = SortOptionsBuilders.field(
                builder -> builder.field("Price").order(SortOrder.Desc));

        SortOptions localPriceSort = SortOptionsBuilders.field(
                builder -> builder.field("offers.LocalPrice").order(SortOrder.Desc));

        SearchResponse<JsonNode> search = esClient.search(s -> s
                        .index(INDEX_NAME)
                        .from(currentPage)
                        .size(size)
                        .sort(List.of(localPriceSort, sortOptions))
                        .query(boolQuery._toQuery())
                        .aggregations("byProductCategory",
                                builder -> builder.terms(TermsAggregation.of(b -> b.field("ProductCategory.keyword")))),
                JsonNode.class);

        System.out.println("total size " + search.hits().total().toString());
        for (Hit<JsonNode> hit : search.hits().hits()) {
            System.out.println("product {" + hit.source().get("Description_en").asText() + "}" +
                    " is free shipping? " + hit.source().get("FreeShipping"));
        }

        Map<String, Aggregate> aggregateMap = search.aggregations();
        for (String key : aggregateMap.keySet()) {
            System.out.println("====================aggregation by {" + key + "}======================");
            System.out.println(aggregateMap.get(key)._get().toString());
        }
    }
}
