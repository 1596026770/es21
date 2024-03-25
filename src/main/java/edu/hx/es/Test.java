package edu.hx.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Test {
    public static void main(String[] args) throws Exception{
        //创建es客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.5.30",9200,"http"))
        );
        ClusterHealthRequest request = new ClusterHealthRequest();
        //查看健康状态
        ClusterHealthResponse response = esClient.cluster().health(request, RequestOptions.DEFAULT);

        System.out.println(response.getStatus());
        //关闭es客户端
        esClient.close();
    }
}
