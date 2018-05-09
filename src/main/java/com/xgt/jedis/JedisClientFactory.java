package com.xgt.jedis;

import redis.clients.jedis.Jedis;

public class JedisClientFactory {
    private static final String HOST = "172.17.50.8";
    private static final int PORT = 6379;

    private JedisClientFactory() {}

    public static Jedis create() {
        return new Jedis(HOST, PORT);
    }
}
