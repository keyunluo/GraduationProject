package org.streamer.dislab.HBaseElasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;



/**
 * Created by keyun on 2016/5/7.
 */
public class ESTest {

    private static Client client=null;

    private static void readConfiguration(){
        Configure.clusterName="Elasticsearch";
        Configure.nodeHost = "slave01";
        Configure.nodePort = 9300;
        Configure.indexName = "gps_data";
        Configure.esnodes= "slave01:9300,slave07:9300,slave10:9300"
                .split(",");
    }

    public static void init()  {
        readConfiguration();
        Settings settings = Settings.settingsBuilder().put("cluster.name",Configure.clusterName).build();
        TransportClient tclient  = TransportClient.builder().settings(settings).build();
        try {
            for (String node : Configure.esnodes) {
                String[] host = node.split(":");
                tclient.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(host[0]), Integer.valueOf(host[1])));
            }
        } catch (UnknownHostException uh){
            uh.printStackTrace();
        }

        client = tclient;
    }

    public static String Query(String zdbh){
        QueryBuilder qb= QueryBuilders.termQuery("ZDBH",zdbh);
        SearchResponse response = client.prepareSearch()
                .setIndices("bus_info")
                .setTypes("bus")
                .setQuery(qb)
                .execute()
                .actionGet();
        for (SearchHit hit : response.getHits().getHits()){
            //if (hit.getFields().containsKey("XLMC"))
                //return hit.getFields().get("XLMC").getValue();
                return hit.getSource().get("XLMC").toString();
        }
        return null;

    }
    public static void main(String[] args){

        init();
        String zdbh=new String("3201100064");
        String xlmc=Query(zdbh);
        if (xlmc != null)
            System.out.println("线路名称为："+xlmc);
        else
            System.out.println("查询失败!");
        client.close();
    }
}
