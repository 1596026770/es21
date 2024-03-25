//package edu.hx.es;
//
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.RestClient;
//
//public class Demo {
//    public static void main(String[] args) {
//// 创建低级客户端
//        RestClient restClient = RestClient.builder(
//                new HttpHost("192.168.5.30", 9200)).build();
//
//// 使用Jackson映射器创建传输层
//        ElasticsearchTransport transport = new RestClientTransport(
//                restClient, new JacksonJsonpMapper());
//
//// 创建API客户端
//        ElasticsearchClient client = new ElasticsearchClient(transport);
//
//
//        GetIndexResponse createIndexResponse = client.indices().get(e->e.index("newapi"));
//        System.out.println(String.join(",", createIndexResponse.result().keySet()));
//        transport.close();
//        restClient.close();
//
//    }
//
//}
