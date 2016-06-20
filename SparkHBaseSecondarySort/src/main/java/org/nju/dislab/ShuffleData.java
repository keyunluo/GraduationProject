package org.nju.dislab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

class SparkTupleComparator  implements Comparator<Tuple3<String, String,String>>, java.io.Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 8383342958459774342L;
    static final SparkTupleComparator INSTANCE = new SparkTupleComparator();

    //自定义比较函数
    public int compare(Tuple3<String, String,String> DATE1,Tuple3<String, String,String> DATE2){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        try {
            Date dt1 = df.parse(DATE1._1());
            Date dt2 = df.parse(DATE2._1());
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }
}


public class ShuffleData implements Serializable{

    /**
     *
     */
    private static final long serialVersionUID = 6876861194159418771L;
    private static final String master = "spark://slave01:7077";


    //迭代变量转成列表
    List<Tuple3<String, String, String>> iterableToList(Iterable<Tuple3<String, String, String>> s) {
        List<Tuple3<String, String, String>> list = new ArrayList<Tuple3<String, String, String>>();
        for (Tuple3<String, String, String> item : s) {
            list.add(item);
        }
        return list;
    }

    public void start() {

        //设置Spark配置文件

        SparkConf sparkConf = new SparkConf().
                setAppName("Spark二次排序").
                setMaster(master).
                setSparkHome(System.getenv("SPARK_HOME"));
        sparkConf.set("spark.kryoserializer.buffer.max","2000");
        sparkConf.set("spark.driver.maxResultSize","2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        List<String> tables = new ArrayList<String>();


        try {
            //HDFS配置
            Configuration hadoopconf=sc.hadoopConfiguration();
            hadoopconf.set("fs.defaultFS", "hdfs://slave01:9000/");
            FileSystem fileSystem = FileSystem.get(hadoopconf);
            Path basePath= new Path("/user/hadoop/traffic/rawdata/");
            FileStatus[] fs = fileSystem.listStatus(basePath);
            //获得数据表
            for (FileStatus f:fs){
                if(f.isDirectory())
                    tables.add(f.getPath().getName());
            }

            //过滤掉无效数据，即终端编号不是3201开头的数据
            final String starts = new String("3201");

            for (String table:tables){
                //读取文件
                JavaRDD<String> lines = sc.textFile(basePath.toString()+"/"+table);
                //转换RDD
                JavaPairRDD<String,Tuple3<String,String,String>> levels = lines.mapToPair(
                        new PairFunction<String, String, Tuple3<String, String, String>>() {
                            @Override
                            public Tuple2<String, Tuple3<String, String, String>> call(String s) throws Exception {
                                String[] tokens = s.split(",");

                                Tuple3<String,String,String> timevalue = new Tuple3<String, String, String>(tokens[2].substring(0,19),tokens[3],tokens[4]);
                                String rowkey= new String(tokens[1]+" "+tokens[2].substring(11,19));
                                return new Tuple2<String, Tuple3<String, String, String>>(rowkey,timevalue);
                            }
                        }
                );

                //按Key来聚集
                JavaPairRDD<String,Iterable<Tuple3<String,String, String>>> count = levels.groupByKey();

                //二次排序，根据记录时间来排序
                JavaPairRDD<String,Iterable<Tuple3<String,String, String>>> sorted = count.mapValues(
                        new Function<Iterable<Tuple3<String,String, String>>, Iterable<Tuple3<String,String, String>>>() {

                            private static final long serialVersionUID = 1L;

                            public Iterable<Tuple3<String,String, String>> call (Iterable<Tuple3<String,String, String>> s){
                                List<Tuple3<String,String, String>> newlist = new ArrayList<Tuple3<String,String, String>>(iterableToList(s));
                                Collections.sort(newlist, SparkTupleComparator.INSTANCE);
                                return newlist;
                            }
                        }
                ).sortByKey();

                //写结果到HDFS中
                //进行转换操作
                List<Tuple2<String, Iterable<Tuple3<String,String, String>>>> output= sorted.collect();
                Path path= new Path("/user/hadoop/traffic/hbasedata/"+table.substring(12));
                //判断文件是否存在，若存在则删除,第二个参数表示是否递归删除
                if(fileSystem.exists(path))
                    fileSystem.delete(path,true);
                FSDataOutputStream outputStream = fileSystem.create(path);
                for (Tuple2<String, Iterable<Tuple3<String,String, String>>> tuple : output) {
                    if (! tuple._1.startsWith(starts))
                        continue;
                    Iterable<Tuple3<String,String, String>> list = tuple._2;
                    for (Tuple3<String,String, String> s : list)
                        outputStream.writeBytes(tuple._1+","+s._1()+","+s._2()+","+s._3()+ "\n");
                }
                outputStream.close();
            }
            System.out.println("文件创建成功！");
            //关闭SparkContext
            sc.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws InterruptedException {
        new ShuffleData().start();
        System.exit(0);
    }

}
