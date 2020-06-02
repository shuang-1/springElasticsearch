package com.usian.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig extends ElasticsearchProperties {

    @Bean
    public RestHighLevelClient getRestHighLevelClient(){
        String[] hosts = getClusterNodes().split(",");
        HttpHost[] httpHost = new HttpHost[hosts.length];

        for (int i = 0; i < httpHost.length; i++) {
            String h = hosts[i];
            httpHost[i] = new HttpHost(h.split(":")[0],Integer.parseInt(h.split(":")[1]));
        }
        return new RestHighLevelClient(RestClient.builder(httpHost));
    }
}
