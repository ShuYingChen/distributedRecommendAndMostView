package jobCore.ec1web.orderTest;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * 測試抓order uid 和 uid map
 * key ->
 * Created by Joan on 2014/10/13.
 */
public class OrderShipInfo {
    final String product_urlString = "http://ecshsolr.global.mypchome.com.tw:8080/solr/product";    // 線上 solr (slave)
    final String order_urlString = "http://ecsolr2.global.mypchome.com.tw:8080/order/order_v1";    // 線上 solr (slave)
    final String test_urlString = "http://192.168.200.161:8080/solr-4.4.0/product"; // 外網測試用
    SolrServer product_solr = new HttpSolrServer(product_urlString);
    SolrServer order_solr = new HttpSolrServer(order_urlString);

    void test(String time) {
        // 2014-10-14T17:57:08Z
        String start_time = time + "T00:00:00Z";
        String end_time = time + "T23:59:59Z";
        String q = "TRANS_DTM:[" + start_time + " TO " + end_time;
        SolrQuery query = new SolrQuery();
        query.set("q", q);
        query.set("fq", "IS_SHIP:1 AND IS_CANCEL:0 AND IS_REFUND"); //出貨 且 沒有取消和退貨
    }
}
