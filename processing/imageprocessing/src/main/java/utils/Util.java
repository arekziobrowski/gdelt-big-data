package utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Util {

    private static JedisPool jedis;
    private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf()
            .setMaster("local[2]")
            .setAppName("model.Image processing"));

    public static JavaSparkContext getSc() {
        return sc;
    }

    public static JedisPool getJedis() {
        if (jedis == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxWaitMillis(10000);
            jedisPoolConfig.setMaxTotal(128);
            jedis = new JedisPool(jedisPoolConfig, "redis-tasks", 6379);
        }


        return jedis;
    }

    public static void setJedis(JedisPool jedis) {
        Util.jedis = jedis;
    }
}
