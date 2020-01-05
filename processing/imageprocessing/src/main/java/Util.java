import redis.clients.jedis.Jedis;

public class Util {

    private static Jedis jedis;

    public static Jedis getJedis() {
        if (jedis == null)
            jedis = new Jedis("localhost", 6379);

        return jedis;
    }
}
