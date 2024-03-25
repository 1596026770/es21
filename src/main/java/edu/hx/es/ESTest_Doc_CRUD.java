package edu.hx.es;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hx.es.empty.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;

public class ESTest_Doc_CRUD {
    //创建es客户端
    static RestHighLevelClient esClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost("192.168.5.30",9200,"http"))
    );

    /**
     * 插入文档
     * @throws IOException
     */
    @Test
    public void insert() throws IOException {
        //插入数据
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");

        User user = new User();
        user.setName("zhangsan");
        user.setAge(30);
        user.setSex("男");

        //向ES插入数据必须将数据转为json格式
        ObjectMapper mapper = new ObjectMapper();
        String userJson = mapper.writeValueAsString(user);
        request.source(userJson, XContentType.JSON);

        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);

        System.out.println(response.getResult());

        esClient.close();
    }

    /**
     * 修改文档
     * @throws IOException
     */
    @Test
    public void update() throws IOException {
        //修改数据
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");//对哪个进行修改
        request.doc(XContentType.JSON,"sex","女");//修改内容为

        UpdateResponse response = esClient.update(request, RequestOptions.DEFAULT);

        System.out.println(response.getResult());

        //关闭es客户端
        esClient.close();
    }

    /**
     * 查询文档
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        //查询数据
        GetRequest request = new GetRequest();
        request.index("user").id("1001");

        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);

        System.out.println(response.getSourceAsString());

        //关闭es客户端
        esClient.close();
    }

    /**
     * 删除文档
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        //删除文档
        DeleteRequest request = new DeleteRequest();
        request.index("user").id("1001");

        DeleteResponse response = esClient.delete(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());

        //关闭es客户端
        esClient.close();
    }










}
