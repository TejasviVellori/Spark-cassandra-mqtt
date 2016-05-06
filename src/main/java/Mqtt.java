import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.*;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.base.Objects;

import scala.Tuple2;
 
public class Mqtt {
 
  private static int sum=0;
 
  public static class Count implements Serializable {
      
		private static final long serialVersionUID = 858977299556976470L;
		private String word;
        private int count;
        private int id;

        public static Count newInstance(int id, int count,String name) {
            Count Count = new Count();
            
            Count.setWord(name);
            Count.setCount(count);
            Count.setid(id);
            return Count;
        }

       
       
        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
        public int getid() {
            return id;
        }

        public void setid(int eventTime) {
            this.id = id;
        }
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("id", id).add("count", count).add("word", word).toString();
        }
    }
  @SuppressWarnings({ "serial", "unchecked" })
  public static void main(String[] args) throws Exception {
 
 //1. Create the spark streaming context with a 10 second batch size
SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingMqttTest").setMaster("spark://master:7077").set("spark.driver.allowMultipleContexts", "true").set("spark.cassandra.connection.host", "52.90.114.29");
final JavaSparkContext sc=new JavaSparkContext(sparkConf);
JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));
CassandraConnector connector = CassandraConnector.apply(sc.getConf());
try (Session session = connector.openSession()) {
session.execute("CREATE KEYSPACE testkeyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
session.execute("CREATE TABLE testkeyspace.test_table (id INT PRIMARY KEY,count INT, word TEXT)");
}
//2. MQTTUtils to collect MQTT messages
String brokerUrl = "tcp://52.73.161.142:1883";
String topic = "Spark/wordcount";
JavaReceiverInputDStream<String> lines = MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevels.MEMORY_AND_DISK_SER);
//lines.print();
JavaDStream<String> words = lines.flatMap(
		  new FlatMapFunction<String, String>() {
		 public Iterable<String> call(String x) {
		      return Arrays.asList(x.split(" "));
		    }
		  });     
//words.print();
JavaPairDStream<String, Integer> pairs = words.mapToPair(
		  new PairFunction<String, String, Integer>() {
		      public Tuple2<String, Integer> call(String s) {
		      return new Tuple2<String, Integer>(s, 1);
		    }
		  });
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
		  new Function2<Integer, Integer, Integer>() {
		      public Integer call(Integer i1, Integer i2) {
		      return i1 + i2;
		    }
		  });

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		JavaDStream wordStream=wordCounts.toJavaDStream();
		
		wordStream.foreachRDD(new Function<JavaRDD<String>, Void>() { 
			 public Void call(JavaRDD<String> wordStream) throws Exception { 
		            if(wordStream!=null) 
		        {   
		            	List result = wordStream.collect(); 
		            	ArrayList al=new ArrayList();
		            	
		           	 
		            	for(int i=0;i<result.size();i++){
		            		String[] sa=result.get(i).toString().split(",");
		            		
		            		al.add(Count.newInstance(sum,Integer.parseInt(sa[1].substring(0, sa[1].length()-1)), sa[0].substring(1)));
		            		sum++;
		            	}
		            	JavaRDD rdd = sc.parallelize(al);
		            	javaFunctions(rdd).writerBuilder("testkeyspace", "test_table", mapToRow(Count.class)).saveToCassandra();
		            	
		        }
					return null;
			 }
		});
		
		
		
		wordCounts.dstream().saveAsTextFiles("hdfs://master:9000/wcout11/wc", "txt");
		
		
		
ssc.start();
ssc.awaitTermination();
ssc.stop();
}
 

 
}