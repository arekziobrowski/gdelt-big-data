import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class Util {

    private static JedisPool jedis;

    public static JedisPool getJedis() {
        if (jedis == null)
            jedis = new JedisPool("redis-tasks", 6379);


        return jedis;
    }
}
