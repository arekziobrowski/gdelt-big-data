import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Util {

    private static JedisPool jedis;

    public static JedisPool getJedis() {
        if (jedis == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxWaitMillis(10000);
            jedisPoolConfig.setMaxTotal(128);
            jedis = new JedisPool(jedisPoolConfig, "redis-tasks", 6379);
        }


        return jedis;
    }
}
