package org.streamer.dislab.HBaseElasticsearch;

/**
 * Created by streamer on 16-4-5.
 */

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;


public class ESOperator {

    // 缓冲池容量为5000,每一千个数据提交一次
    private static final int MAX_BULK_COUNT = 5000;
    // 最大提交间隔（秒）
    private static final int MAX_COMMIT_INTERVAL = 60 * 5;

    private static BulkRequestBuilder bulkRequestBuilder =null;

    private static Client client =null;
    private static Lock commitLock = new ReentrantLock();


   static {
        Settings settings = Settings.settingsBuilder().put("cluster.name", Configure.clusterName).build();

       try {
           client = TransportClient.builder().settings(settings).build()
                   .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(Configure.nodeHost), Configure.nodePort));
       }catch (UnknownHostException ue){
           ue.printStackTrace();
       }

        bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefresh(true);

        Timer timer = new Timer();
        timer.schedule(new CommitTimer(),10*1000,MAX_COMMIT_INTERVAL*1000);
    }



    /**
     * 判断缓存池是否已满，批量提交
     *
     * @param threshold
     */
    private static void bulkRequest(int threshold) {
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                bulkRequestBuilder = client.prepareBulk();
            }
        }
    }

    /**
     * 加入索引请求到缓冲池
     *
     * @param builder
     */
    public static void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        bulkRequestBuilder.add(builder);
        bulkRequest(MAX_BULK_COUNT);
        commitLock.unlock();
    }

    /**
     * 加入删除请求到缓冲池
     *
     * @param builder
     */
    public static void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        bulkRequestBuilder.add(builder);
        bulkRequest(MAX_BULK_COUNT);
        commitLock.unlock();

    }


    /**
     * 定时任务，避免RegionServer迟迟无数据更新，导致ElasticSearch没有与HBase同步
     */
    static class CommitTimer extends TimerTask {
        @Override
        public void run() {
            commitLock.lock();
            try {
                bulkRequest(0);
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                commitLock.unlock();
            }
        }
    }


    private static void test() {
        for (int i = 0; i < 10; i++) {
            Map<String, Object> json = new HashMap<String, Object>();
            json.put("field", "test");
            addUpdateBuilderToBulk(client.prepareUpdate(Configure.indexName, Configure.typeName, String.valueOf(i)).setUpsert(json));
        }
        System.out.println(bulkRequestBuilder.numberOfActions());
    }

    public static void main(String[] args) {
        test();
    }
}
