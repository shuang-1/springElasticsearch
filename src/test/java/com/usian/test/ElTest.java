package com.usian.test;

import com.usian.App;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = App.class)
public class ElTest {


    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test  //创建索引库
    public void testCreateIndex() throws IOException {
        //创建“新建索引”的对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("h1906b");
        //设置索引参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards",2)
                .put("number_of_replicas",0));
        createIndexRequest.mapping("course", "{\r\n" +
                "  \"_source\": {\r\n" +
                "    \"excludes\":[\"description\"]\r\n" +
                "  }, \r\n" +
                " 	\"properties\": {\r\n" +
                "           \"name\": {\r\n" +
                "              \"type\": \"text\",\r\n" +
                "              \"analyzer\":\"ik_max_word\",\r\n" +
                "              \"search_analyzer\":\"ik_smart\"\r\n" +
                "           },\r\n" +
                "           \"description\": {\r\n" +
                "              \"type\": \"text\",\r\n" +
                "              \"analyzer\":\"ik_max_word\",\r\n" +
                "              \"search_analyzer\":\"ik_smart\"\r\n" +
                "           },\r\n" +
                "           \"studymodel\": {\r\n" +
                "              \"type\": \"keyword\"\r\n" +
                "           },\r\n" +
                "           \"price\": {\r\n" +
                "              \"type\": \"float\"\r\n" +
                "           },\r\n" +
                "           \"timestamp\": {\r\n" +
                "          		\"type\":   \"date\",\r\n" +
                "          		\"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\r\n" +
                "        	}\r\n" +
                "  }\r\n" +
                "}", XContentType.JSON);
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
        System.out.println(createIndexResponse.isAcknowledged());
    }

    @Test  //删除索引
    public void testDeleteIndex() throws IOException {
        //创建“删除索引请求”的对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("h1906b");
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        DeleteIndexResponse deleteIndexResponse = indices.delete(deleteIndexRequest,
                RequestOptions.DEFAULT);
        //得到响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }
}
