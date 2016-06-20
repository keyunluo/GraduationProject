package org.streamer.dislab.HBaseElasticsearch;



import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by streamer on 16-4-5.
 */
public class Configure {

    // ElasticSearch的集群名称
    static String clusterName;
    // ElasticSearch的host
    static String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    static int nodePort;

    //多个节点
    static String[] esnodes;
    // ElasticSearch的索引名称
    static String indexName;
    // ElasticSearch的类型名称
    static String typeName;

    public static String getInfo(){
        List<String> fields = new ArrayList<String>();

        try {
            for (Field f : Configure.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
                }
            } catch (IllegalAccessException ex){
                ex.printStackTrace();
            }
        return StringUtils.join(fields,",");
        }

  /*  public static void main(String[] args){
        Configure.clusterName="Elasticsearch";
        Configure.nodeHost="ubuntu";
        Configure.nodePort=9300;
        Configure.indexName="gps_data";
        Configure.typeName="gps_type";
        System.out.println(Configure.getInfo());
    }*/


     public static void main(String[] args){
        Configure.clusterName="Elasticsearch";
        Configure.nodeHost="slave01";
        Configure.nodePort=9300;
        Configure.indexName="demo";
        Configure.typeName="doc";
        System.out.println(Configure.getInfo());
    }

}
