package edu.hx.es.finish_work;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class ESFinishWork {
    //创建es客户端
    static RestHighLevelClient esClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost("192.168.5.30",9200,"http"))
    );

    @Test
    public void Create_Index() throws IOException {
        //创建es客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.5.30",9200,"http"))
        );
        //创建索引
        CreateIndexRequest request = new CreateIndexRequest("stu");
        //设置分片、副本数
        Settings.Builder builder = Settings.builder();
        builder.put("index.number_of_shards", 3);
        builder.put("index.number_of_replicas", 1);
        //设置别名
        request.alias(new Alias("stu_rest"));

        String source = "{\n"
                    +"\"properties\": {\n"
                    +"\"stu_no\":{\"type\": \"keyword\"},\n"
                    +"\"name\":{\"type\": \"text\",\"fields\": {\"keyword\":{\"type\":\"keyword\"}}},\n"
                    +"\"sex\":{\"type\": \"long\"},\n"
                    +"\"age\":{\"type\": \"integer\"},\n"
                    +"\"birthday\":{\"type\": \"date\"},\n"
                    +"\"height\":{\"type\": \"double\"},\n"
                    +"\"addr\":{\"type\": \"text\",\"fields\": {\"keyword\":{\"type\":\"keyword\"}}},\n"
                    +"\"interest\":{\"type\": \"text\",\"fields\": {\"keyword\":{\"type\":\"keyword\"}}}\n"
                    +"}\n"
                    +"}";
        request.mapping(source,XContentType.JSON);
        request.settings(builder);
        CreateIndexResponse response =
                esClient.indices().create(request, RequestOptions.DEFAULT);
        //响应状态
        boolean acknowledged = response.isAcknowledged();
        System.out.println("索引操作："+acknowledged);

        //关闭es客户端
        esClient.close();
    }

    /**
     * 批量插入文档
     * @throws IOException
     */
    @Test
    public void insert() throws IOException {

//         单次插入
//        IndexRequest request = new IndexRequest("stu").id("1").source("stu_no", "1001", "name", "张三","sex",0,"age",18,"birthday","2003-01-01","height",163.5,"addr","硚口区万松园小区","interest","changge,tiaowu");
//        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);

        //批量插入数据
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest().index("stu").id("1").source(XContentType.JSON,
                "stu_no", "1001", "name", "张三","sex",0,"age",18,"birthday","2003-01-01","height",163.5,"addr","硚口区万松园小区","interest","changge,tiaowu"));
        request.add(new IndexRequest().index("stu").id("2").source(XContentType.JSON,
                "stu_no","1002","name","李四","sex",0,"age",19,"birthday","2002-02-03","height",165,"addr","硚口区万达小区","interest","changge,dengshan"));
        request.add(new IndexRequest().index("stu").id("3").source(XContentType.JSON,
                "stu_no","1003","name","王五","sex",1,"age",20,"birthday","2001-03-06","height",176.5,"addr","江岸区春天里小区","interest","youyong,tiaowu"));
        request.add(new IndexRequest().index("stu").id("4").source(XContentType.JSON,
                "stu_no","1004","name","赵六","sex",1,"age",21,"birthday","1999-04-09","height",173,"addr","江岸区幸福里小区","interest","changge,qima"));
        request.add(new IndexRequest().index("stu").id("5").source(XContentType.JSON,
                "stu_no","1005","name","刘小明","sex",1,"age",22,"birthday","1998-05-02","height",170,"addr","江汉区解放大道1008号","interest","changge,taiquandao"));
        request.add(new IndexRequest().index("stu").id("6").source(XContentType.JSON,
                "stu_no","1006","name","jack","sex",1,"age",23,"birthday","1997-06-16","height",168.5,"addr","江汉区建设大道1010号","interest","dengshan,youyong"));
        request.add(new IndexRequest().index("stu").id("7").source(XContentType.JSON,
                "stu_no","1007","name","jim","sex",1,"age",25,"birthday","1995-07-19","height",169,"addr","黄陂区盘龙城小区","interest","paobu,lanqiu"));
        request.add(new IndexRequest().index("stu").id("8").source(XContentType.JSON,
                "stu_no","1008","name","lily","sex",0,"age",18,"birthday","2003-08-20","height",161.5,"addr","江夏区月亮湾小区1008号","interest","changge,tiaowu,youyong"));
        request.add(new IndexRequest().index("stu").id("9").source(XContentType.JSON,
                "stu_no","1009","name","lucy","sex",0,"age",18,"birthday","2003-08-20","height",161,"addr","江夏区月亮湾小区1010号","interest","changge,tiaowu,youyong"));
        request.add(new IndexRequest().index("stu").id("10").source(XContentType.JSON,
                "stu_no","1010","name","lilei","sex",1,"age",26,"birthday","1994-09-25","height",180,"addr","江夏区月亮湾小区","interest","paobu,zuqiu"));

        BulkResponse response = esClient.bulk(request, RequestOptions.DEFAULT);

        System.out.println(response);

        esClient.close();
    }

    /**
     * 组合查询
     * @throws IOException
     */
    @Test
    public void getZuHe() throws IOException {
        //全量查询文档
        SearchRequest request = new SearchRequest();
        request.indices("stu");

        //查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //年龄range
        builder.query(QueryBuilders.rangeQuery("age").from(18).to(25));
        //学号排序
        builder.sort("stu_no", SortOrder.ASC);

        //爱好高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color=\"red\">");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("interest");

        boolQueryBuilder.should(QueryBuilders.matchQuery("interest","changge"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("interest","tiaowu"));
        boolQueryBuilder.mustNot(QueryBuilders.termQuery("addr","1008"));

        builder.query(boolQueryBuilder);
        builder.highlighter(highlightBuilder);
        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数

        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
            System.out.print("stu_no:" + sourceAsMap.get("stu_no"));
            System.out.print("\t name:" + sourceAsMap.get("name"));
            System.out.print("\t sex:" + sourceAsMap.get("sex"));
            System.out.print("\t age:" + sourceAsMap.get("age"));
            System.out.print("\t birthday:" + sourceAsMap.get("birthday"));
            System.out.print("\t height:" + sourceAsMap.get("height"));
            System.out.print("\t addr:" + sourceAsMap.get("addr"));

            //解析高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlightField= highlightFields.get("interest");
            Text[] fragments = highlightField.fragments();
            if(highlightField!=null){
                System.out.println("\t interest:" +fragments[0].toString());
            }

//            System.out.print("\t interest:" + sourceAsMap.get("interest"));
//            System.out.println();
        }

        //关闭es客户端
        esClient.close();
    }

    /**
     * 聚合查询
     * @throws IOException
     */
    @Test
    public void getJuHe() throws IOException {
        SearchRequest request = new SearchRequest();
        request.indices("stu");

        //查询条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //只查询一条详细数据
        builder.from(0);
        builder.size(1);

        //聚合查询
        AggregationBuilder aggregation = AggregationBuilders.terms("sex_group").field("sex");
//        aggregation.subAggregation(AggregationBuilders.stats("age_status").field("age"));
        aggregation.subAggregation(AggregationBuilders.max("age_max").field("age"));
        aggregation.subAggregation(AggregationBuilders.min("age_min").field("age"));
        aggregation.subAggregation(AggregationBuilders.avg("age_avg").field("age"));
        aggregation.subAggregation(AggregationBuilders.sum("age_sum").field("age"));

        //布尔查询
        boolQueryBuilder.should(QueryBuilders.matchQuery("interest","changge"));
        boolQueryBuilder.should(QueryBuilders.matchQuery("interest","tiaowu"));

        builder.aggregation(aggregation);
        builder.query(boolQueryBuilder);
        request.source(builder);

        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        System.out.println(hits.getTotalHits());//查询总条数
//        System.out.println(response);

        for(SearchHit hit : hits){
//            System.out.println(hit.getSourceAsString());
            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
            System.out.print("stu_no:" + sourceAsMap.get("stu_no"));
            System.out.print("\t name:" + sourceAsMap.get("name"));
            System.out.print("\t sex:" + sourceAsMap.get("sex"));
            System.out.print("\t age:" + sourceAsMap.get("age"));
            System.out.print("\t birthday:" + sourceAsMap.get("birthday"));
            System.out.print("\t height:" + sourceAsMap.get("height"));
            System.out.print("\t addr:" + sourceAsMap.get("addr"));
            System.out.print("\t interest:" + sourceAsMap.get("interest"));
            System.out.println();

        }

        Terms terms = response.getAggregations().get("sex_group");

        for (Terms.Bucket bucket : terms.getBuckets()) {
            System.out.println("sex="+bucket.getKeyAsString()+"的数据如下所示：");
//            System.out.println("key=" + bucket.getKeyAsString() + ",count=" + bucket.getDocCount());
            Max age_max = bucket.getAggregations().get("age_max");
            Min age_min = bucket.getAggregations().get("age_min");
            Avg age_avg = bucket.getAggregations().get("age_avg");
            Sum age_sum = bucket.getAggregations().get("age_sum");
            System.out.println("文档数量:"+bucket.getDocCount()+"\t"+"age_max:"+age_max.getValue()+"\t"+"age_min:"+age_min.getValue()+"\t"+"age_avg:"+age_avg.getValue()+"\t"+"age_sum:"+age_sum.getValue());
        }

        //关闭es客户端
        esClient.close();
    }



}
