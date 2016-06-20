package org.streamer.dislab.HBaseElasticsearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;


/**
 * Created by streamer on 16-4-5.
 */
public class HBaseObserver extends BaseRegionObserver{

    private static Client client=null;
    private static Log log= LogFactory.getLog(HBaseObserver.class);
    private static final String GPSSJ = new String("GPSSJ");
    private static final String ZDBH = new String("ZDBH");
    private static final String ROWKEY = new String("ROWKEY");
    private static final String XLMC = new String("XLMC");

    //读取HBase Shell指令

    private void readConfiguration(CoprocessorEnvironment env){
        Configuration conf = env.getConfiguration();
        Configure.clusterName=conf.get("es_cluster","Elasticsearch");
        Configure.nodeHost = conf.get("es_host","slave01");
        Configure.nodePort = conf.getInt("es_port", 9300);
        Configure.indexName = conf.get("es_index","gps_data");
        Configure.typeName = conf.get("es_type","gps_type");
        Configure.esnodes= "slave01:9300,slave07:9300,slave10:9300"
                .split(",");

       /* Configure.clusterName=conf.get("es_cluster","Elasticsearch");
        Configure.nodeHost = conf.get("es_host","ubuntu");
        Configure.nodePort = conf.getInt("es_port", 9300);
        Configure.indexName = conf.get("es_index","gps_data");
        Configure.typeName = conf.get("es_type","gps_type");
        Configure.esnodes= "ubuntu:9300"
                .split(",");*/

        log.info("observer -- started with config: " + Configure.getInfo());
    }

    @Override
    //Elastic启动
    public void start(CoprocessorEnvironment env) throws UnknownHostException{
        readConfiguration(env);

        Settings settings = Settings.settingsBuilder().put("cluster.name",Configure.clusterName).build();
        TransportClient tclient  = TransportClient.builder().settings(settings).build();
                    //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Configure.nodeHost), Configure.nodePort));
        for (String node : Configure.esnodes) {
            String[] host = node.split(":");
            tclient.addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName(host[0]), Integer.valueOf(host[1])));
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
            /*if (hit.getFields().containsKey("XLMC"))
                return hit.getFields().get("XLMC").getValue();
              */
            return hit.getSource().get("XLMC").toString();
        }
        return null;

    }

    public void shutdown() {
        client.close();
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException{
        super.preGetOp(e, get, results);
    }

    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit  edit,final  Durability durability)  {
        try {
            String indexId = new String(put.getRow());
            String typeName=null;
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            Map<String, Object> json = new HashMap<String, Object>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    if(key.equals(GPSSJ)){
                        String zdbh=indexId.substring(0,10).toString();
                        json.put(ZDBH,zdbh);
                        json.put(ROWKEY,indexId );
                        typeName=value.substring(0,10).replace("-","");
                        json.put(key, value);
                        String xlmc=Query(zdbh);
                        if (xlmc != null)
                            json.put(XLMC,xlmc);
                    }

                }
            }
            if (typeName != null)
            ESOperator.addUpdateBuilderToBulk(client.prepareUpdate(Configure.indexName, typeName, indexId).setDoc(json).setUpsert(json));
            //log.info("observer -- add new doc: " + indexId + " to type: " + Configure.typeName);

        } catch (Exception ex) {
            log.error(ex);
        }
    }

    @Override
    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete, final WALEdit edit, final Durability durability)  {
        try {
            String indexId = new String(delete.getRow());

            String typeName=null;
            RegionCoprocessorEnvironment env = e.getEnvironment();
            typeName = Bytes.toString(env.getRegion().getTableDesc().getTableName().getName());
            if (typeName != null)
                ESOperator.addDeleteBuilderToBulk(client.prepareDelete(Configure.indexName, typeName, indexId));
            //log.info("observer -- delete a doc: " + indexId);
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    private static void testGetPutData(String rowKey, String columnFamily, String column, String value) {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
        System.out.println(Bytes.toString(put.getRow()));
        for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
            Cell cell = entry.getValue().get(0);
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) {
        //testGetPutData("row008", "f1", "c1", "hello world");
        String zdbh=new String("3201100064");
        String xlmc=Query(zdbh);
        if (xlmc != null)
            System.out.println("线路名称为："+xlmc);
        else
            System.out.println("查询失败!");
    }


}
