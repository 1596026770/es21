package edu.hx.es;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import edu.hx.es.empty.User;
import org.apache.http.HttpHost;
//import org.apache.lucene.index.Terms;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;

import org.elasticsearch.action.index.IndexRequest;

import org.elasticsearch.action.search.SearchRequest;

import org.elasticsearch.action.search.SearchResponse;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ESTest_Doc_CRUD_Batch {
    //创建es客户端
    static RestHighLevelClient esClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost("192.168.5.30",9200,"http"))
    );

    /**
     * 批量插入文档
     * @throws IOException
     */
    @Test
    public void insert() throws IOException {
        //批量插入数据
        BulkRequest request = new BulkRequest();

        request.add(new IndexRequest().index("user").id("1001").source(XContentType.JSON,"name","echo","sex","女","age",30));
        request.add(new IndexRequest().index("user").id("1002").source(XContentType.JSON,"name","lingshuanghua","sex","女","age",18));
        request.add(new IndexRequest().index("user").id("1003").source(XContentType.JSON,"name","huashan","sex","男","age",22));
        request.add(new IndexRequest().index("user").id("1004").source(XContentType.JSON,"name","yunmeng","sex","女","age",16));
        request.add(new IndexRequest().index("user").id("1005").source(XContentType.JSON,"name","anxiang","sex","男","age",22));
        request.add(new IndexRequest().index("user").id("1006").source(XContentType.JSON,"name","wudang","sex","男","age",20));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);

        System.out.println(response.getTook());//时间
        System.out.println(response.getItems());

        esClient.close();
    }

    /**
     * 批量删除文档
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        //批量删除数据
        BulkRequest request = new BulkRequest();

        request.add(new DeleteRequest().index("user").id("1001"));
        request.add(new DeleteRequest().index("user").id("1002"));
        request.add(new DeleteRequest().index("user").id("1003"));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);

        System.out.println(response.getTook());//时间
        System.out.println(response.getItems());

        //关闭es客户端
        esClient.close();
    }

    /**
     * 全量查询文档
     * @throws IOException
     */
    @Test
    public void get() throws IOException {
        //全量查询文档
        SearchRequest request = new SearchRequest();
        request.indices("user");

        //查询条件
        //1. 查询索引中全部的数据
//        new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
        System.out.println(response.getTook());

        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 条件查询文档
     * @throws IOException
     */
    @Test
    public void getTiaojian() throws IOException {
        //条件查询文档
        SearchRequest request = new SearchRequest();
        request.indices("customer");

        //查询条件
        //2. 条件查询:termQuery
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.termQuery("sex", 0));
        builder.from(0);
        builder.size(5);

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
//        System.out.println(response.getTook());

        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
            System.out.print("customCode:" + sourceAsMap.get("customCode"));
            System.out.print("\t customerName:" + sourceAsMap.get("customerName"));
            System.out.print("\t sex:" + sourceAsMap.get("sex"));
            System.out.println("\t tel:" + sourceAsMap.get("tel"));
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 分页查询文档
     * @throws IOException
     */
    @Test
    public void getPages() throws IOException {
        //分页查询文档
        SearchRequest request = new SearchRequest();
        request.indices("user");

        //查询条件
        //3. 分页查询
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        //(当前页码-1)*每页显示的数据条数
        builder.from(0);
        builder.size(2);
        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
        System.out.println(response.getTook());

        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 查询排序文档
     * @throws IOException
     */
    @Test
    public void getSort() throws IOException {
        //查询排序文档
        SearchRequest request = new SearchRequest();
        request.indices("user");

        //查询条件
        //4. 查询排序：sort
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        builder.sort("age", SortOrder.DESC);
        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
//        System.out.println(response.getTook());//时间

        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 查询过滤字段
     * @throws IOException
     */
    @Test
    public void getFetchSource() throws IOException {
        //查询过滤字段
        SearchRequest request = new SearchRequest();
        request.indices("user");

        //查询条件
        //5. 查询过滤字段:fetchSource
        SearchSourceBuilder builder = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] excludes = {"age"};//排除
        String[] includes = {"name"};//包含
        builder.fetchSource(includes,excludes);
        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
//        System.out.println(response.getTook());//时间

        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 组合查询
     * @throws IOException
     */
    @Test
    public void getBool() throws IOException {
        //查询过滤字段
        SearchRequest request = new SearchRequest();
        request.indices("user");

        //查询条件
        //6. 组合查询:
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.must(QueryBuilders.matchQuery("age",22));
        boolQueryBuilder.must(QueryBuilders.matchQuery("sex","男"));

        builder.query(boolQueryBuilder);

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();


//        System.out.println(response.getTook());//时间

//        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println(hits.getTotalHits());//查询总条数
        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
            System.out.print("name:" + sourceAsMap.get("name"));
            System.out.print("\t sex:" + sourceAsMap.get("sex"));
            System.out.print("\t age:" + sourceAsMap.get("age"));
            System.out.println();
        }

        //关闭es客户端
        esClient.close();
    }

    //-------------------------------------------------------------------------
    /**
     * 组合查询
     * @throws IOException
     */
    @Test
    public void getyangli1() throws IOException {
        //查询过滤字段
        SearchRequest request = new SearchRequest();
        request.indices("customer");

        //查询条件
        //样例
        SearchSourceBuilder builder = new SearchSourceBuilder();
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        builder.query(QueryBuilders.termQuery("sex",0));

        String[] excludes = {"sex"};//排除
        String[] includes = {"customCode","customerName"};//包含
        builder.fetchSource(includes,excludes);

        builder.sort("customCode.keyword",SortOrder.DESC);

//        boolQueryBuilder.must(QueryBuilders.matchQuery("age",22));
//        boolQueryBuilder.must(QueryBuilders.matchQuery("sex",0));

//        builder.query(boolQueryBuilder);

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();


//        System.out.println(response.getTook());//时间

//        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println(hits.getTotalHits());//查询总条数
        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
            System.out.print("customCode:" + sourceAsMap.get("customCode"));
            System.out.print("\t customerName:" + sourceAsMap.get("customerName"));
            System.out.println();
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 模糊查询
     * @throws IOException
     */
    @Test
    public void getfuzzy() throws IOException {
        //模糊查询
        SearchRequest request = new SearchRequest();
        request.indices("user");

        //查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
//        QueryBuilders.fuzzyQuery("name", "an").fuzziness(Fuzziness.ONE);
        builder.query(QueryBuilders.fuzzyQuery("name", "anxiang").fuzziness(Fuzziness.ONE));

        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();


//        System.out.println(response.getTook());//时间

//        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
//        }

        System.out.println(hits.getTotalHits());//查询总条数
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
//            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
//            System.out.print("customCode:" + sourceAsMap.get("customCode"));
//            System.out.print("\t customerName:" + sourceAsMap.get("customerName"));
//            System.out.println();
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 高亮查询
     * @throws IOException
     */
    @Test
    public void getgaoliang() throws IOException {
        //查询过滤字段
        SearchRequest request = new SearchRequest();
        request.indices("customer");

        //查询条件
        //6. 高亮查询:
        SearchSourceBuilder builder = new SearchSourceBuilder();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("addr", "万科");

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color=\"red\">");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("addr");

        builder.highlighter(highlightBuilder);
        builder.query(matchQueryBuilder);


        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());
//            HighlightField highlightField = hit.getHighlightFields().get("addr");

            //解析高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField= highlightFields.get("addr");
            Text[] fragments = highlightField.fragments();

          if(highlightField!=null){
              System.out.println(fragments[0].toString());
          }
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 聚合查询 aggregation
     * @throws IOException
     */
    @Test
    public void getjuhe() throws IOException {
        //查询过滤字段
        SearchRequest request = new SearchRequest();
        request.indices("customer");

        //查询条件
        // 聚合查询:
        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
//        new ObjectMapper().writeValueAsString(SearchResponse.getAggregations());
        builder.aggregation(aggregationBuilder);


        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数

        for(SearchHit hit : hits){
            System.out.println(hit.getSourceAsString());

        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 分组查询 aggregation
     * Terms类导包导错了
     * @throws IOException
     */
    @Test
    public void getfenzu() throws IOException {
        //查询过滤字段
        SearchRequest request = new SearchRequest();
        request.indices("customer");

        //查询条件
        // 聚合查询:
        SearchSourceBuilder builder = new SearchSourceBuilder();

        AggregationBuilder aggregation = AggregationBuilders.terms("sex_aggs").field("sex");
        aggregation.subAggregation(AggregationBuilders.avg("age_avg").field("age"));

        builder.aggregation(aggregation);
        request.source(builder);
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        Aggregations aggregations = response.getAggregations();
        Terms sexAggregation = aggregations.get("sex_aggs");

        //获取key=1的平均值
//        Terms.Bucket elasticBucket = sexAggregation.getBucketByKey("1");
//        System.out.println(elasticBucket.getDocCount());//获取分组sex=1的数量

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数

        Terms terms = response.getAggregations().get("sex_aggs");

        for (Terms.Bucket bucket : terms.getBuckets()) {
            System.out.println("key=" + bucket.getKeyAsString() + ",count=" + bucket.getDocCount());
        }


//        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
//
//        }

        //关闭es客户端
        esClient.close();
    }







}
