package com.usian.test;

import com.usian.App;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = App.class)
public class ElTest {


    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest searchRequest;

    private SearchResponse searchResponse;

    @Before
    public void searchRequest(){
        searchRequest = new SearchRequest("h1906b");
        searchRequest.types("course");
    }

    //添加文档
    @Test
    public void testAddIndex() throws IOException {
        //创建“添加文档”的对象
        IndexRequest indexRequest = new IndexRequest("h1906b","course","1");
        indexRequest.source("{\n" +
                "  \"name\": \"spring cloud实战\",\n" +
                "  \"description\": \"4.注册中心eureka\",\n" +
                "  \"studymodel\": \"201001\",\n" +
                "  \"price\": 5.6,\n" +
                "  \"timestamp\":\"2019-09-09\"\n" +
                "}",XContentType.JSON);

        //创建响应对象
        IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(index);
    }

    //批量添加
    @Test
    public void testBulkAddDoc() throws IOException {

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("h1906b","course").source("{\"name\":\"年后\",\"description\":\"谁都不服\",\"studymodel\":\"209083\",\"price\":23.89,\"timestamp\":\"2019-09-08\"}",XContentType.JSON));
        bulkRequest.add(new IndexRequest("h1906b","course").source("{\"name\":\"你好\",\"description\":\"在见不见\",\"studymodel\":\"209574\",\"price\":67.9,\"timestamp\":\"2017-09-08\"}",XContentType.JSON));

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.toString());
    }

    //修改文档
    @Test
    public void testUpdateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("h1906b", "course", "1");
        updateRequest.doc("{\n" +
                "  \"name\":\"你好世界\",\n" +
                "  \"description\":\"人生苦短\",\n" +
                "  \"studymodel\":\"10059\",\n" +
                "  \"timestamp\":\"2019-09-09\",\n" +
                "  \"price\":200.0\n" +
                "}",XContentType.JSON);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.toString());
    }

    //删除文档
    @Test
    public void testDeleteDoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("h1906b", "course", "1");
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult());
    }


    //查询文档数据
    @Test
    public void testGetDoc() throws IOException {
        GetRequest getRequest = new GetRequest("h1906b", "course", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(getResponse.getSourceAsString());
    }

    //查询所有文档
    @Test
    public void textGetAllDoc() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //分页查询
    @Test
    public void pageInfoDoc() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("price", SortOrder.ASC);
        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

    }


    //条件查询
    @Test
    public void textGetAllDocMatch() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发"));

        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //条件查询至少匹配两个词才返回
    @Test
    public void textGetAllDocMatch2() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));

        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //多条件查询
    @Test
    public void textGetAllDocMultiMatch() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发","name","description"));

        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //bool查询
    @Test
    public void textGetAllDocBool() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","java"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("description","开发"));

        searchSourceBuilder.query(boolQueryBuilder);

        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //filter查询
    @Test
    public void textGetAllDocBool2() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","java"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(100));

        searchSourceBuilder.query(boolQueryBuilder);

        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //highlight查询
    @Test
    public void textGetAllDocHighlight() throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发","name","description"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<red>");
        highlightBuilder.postTags("</red>");
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);

        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }



    @After
    public void displayHis() throws IOException {

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("一共搜索到："+totalHits+"条数据");
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());


            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(highlightFields!=null){
                HighlightField highlightField = highlightFields.get("name");
                Text[] fragments = highlightField.getFragments();
                System.out.println("高亮字段：" + fragments[0].toString());
            }
        }
    }

}
