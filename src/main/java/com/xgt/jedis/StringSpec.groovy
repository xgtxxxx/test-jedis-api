package com.xgt.jedis

import redis.clients.jedis.Jedis
import spock.lang.Specification

//https://www.cnblogs.com/Laymen/p/6119515.html
//https://www.cnblogs.com/liuling/p/2014-4-19-04.html
class StringSpec extends Specification {
    private Jedis jedis

    void setup() {
        jedis = JedisClientFactory.create()
        jedis.flushAll() //清空所有数据
    }

    /**
     * SET key value [EX seconds] [PX milliseconds] [NX|XX]
     * 将字符串值 value 关联到 key 。
     * 如果 key 已经持有其他值， SET 就覆写旧值，无视类型。
     * 对于某个原本带有生存时间（TTL）的键来说， 当 SET 命令成功在这个键上执行时， 这个键原有的 TTL 将被清除。
     * 可选参数
     * 从 Redis 2.6.12 版本开始， SET 命令的行为可以通过一系列参数来修改：
     * EX second ：设置键的过期时间为 second 秒。 SET key value EX second 效果等同于 SETEX key second value 。
     * PX millisecond ：设置键的过期时间为 millisecond 毫秒。 SET key value PX millisecond 效果等同于 PSETEX key millisecond value 。
     * NX ：只在键不存在时，才对键进行设置操作。 SET key value NX 效果等同于 SETNX key value 。
     * XX ：只在键已经存在时，才对键进行设置操作。
     */
    def "test set()"() {
        when:
        jedis.set("name", "Gavin")

        then:
        jedis.get("name") == "Gavin"

        when:"如果 key 已经持有其他值， SET 就覆写旧值，无视类型"
        jedis.set("name", "Kevin")

        then:
        jedis.get("name") == "Kevin"

        when:"NX: 如果key存在，则不执行set方法，如果不存在，则set"
        jedis.set("name", "Tony", "NX")//等同于jedis.setnx("name", "Tony")
        jedis.set("age", "30")

        then:
        jedis.get("name") == "Kevin"
        jedis.get("age") == "30"

        when:"设置key的过期时间"
        jedis.set("temp", "this will expire in 5 seconds!", "NX", "EX", 5)//等同于jedis.setex("temp", 5, "this will expire in 5 seconds!")

        then:
        jedis.get("temp") == "this will expire in 5 seconds!"

        when:"当6秒过后查看temp，temp的值为null"
        Thread.sleep(6000)
        then:
        !jedis.get("temp")
    }

    /**
     * MSET key value [key value ...]  MGET key [key ...]返回所有(一个或多个)给定 key 的值。如果给定的key里面,有某个key不存在，那么这个key返回特殊值nil 。因此，该命令永不失败。
     * 同时设置一个或多个 key-value 对。
     * 如果某个给定 key 已经存在，那么 MSET 会用新值覆盖原来的旧值，如果这不是你所希望的效果，请考虑使用 MSETNX 命令：它只会在所有给定 key 都不存在的情况下进行设置操作。
     * MSET 是一个原子性(atomic)操作，所有给定 key 都会在同一时间内被设置，某些给定 key 被更新而另一些给定 key 没有改变的情况，不可能发生。
     */
    def "mult set -- mset()"() {
        when:
        jedis.mset("name", "Gavin", "age", "28")
        then:
        jedis.mget("name", "age") == ["Gavin", "28"]

        when:"当key存在的情况下，不覆盖原来的旧值"
        jedis.msetnx("name", "Kevin", "age", "30")
        then:
        jedis.mget("name", "age") == ["Gavin", "28"]

        when:"当key存在的情况下，覆盖原来的旧值"
        jedis.mset("name", "Kevin", "age", "30")
        then:
        jedis.mget("name", "age") == ["Kevin", "30"]
    }

    /**
     * GETSET key value
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
     * 当 key 存在但不是字符串类型时，返回一个错误。
     */
    def "test getSet()"() {
        when:
        def name = jedis.getSet("name", "new name")
        then:
        name == null
        jedis.get("name") == "new name"
    }

    /**
     * APPEND key value
     * 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。
     * 如果 key 不存在， APPEND 就简单地将给定 key 设为 value ，就像执行 SET key value 一样。
     */
    def "test append()" () {
        when:
        jedis.append("name", "Gavin")
        then:
        jedis.get("name") == "Gavin"

        when:
        jedis.append("name", " Xi")
        then:
        jedis.get("name") == "Gavin Xi"
    }

    /**
     * SETBIT key offset value
     * 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
     * 位的设置或清除取决于 value 参数，可以是 0 也可以是 1 。
     * 当 key 不存在时，自动生成一个新的字符串值。
     * 字符串会进行伸展(grown)以确保它可以将 value 保存在指定的偏移量上。当字符串值进行伸展时，空白位置以 0 填充。
     * offset 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)。
     */
    def "test setBit()" () {
        when:
        jedis.setbit("id", 0, "1");
        jedis.setbit("id", 1, true);
        then:
        jedis.bitcount("id") == 2
        jedis.getbit("id", 0) == true
        jedis.getbit("id", 1) == true
    }

    /**
     * SETRANGE key offset value
     * 用 value 参数覆写(overwrite)给定 key 所储存的字符串值，从偏移量 offset 开始。
     * 不存在的 key 当作空白字符串处理。
     * SETRANGE 命令会确保字符串足够长以便将 value 设置在指定的偏移量上，
     * 如果给定key原来储存的字符串长度比偏移量小(比如字符串只有 5 个字符长，但你设置的 offset 是 10 )，
     * 那么原字符和偏移量之间的空白将用零字节(zerobytes, "\x00" )来填充。
     */
    def "test setRange()"() {
        given:
        jedis.set("name", "Gavin")
        when:
        jedis.setrange("name", 0, "Ke")
        then:
        jedis.get("name") == "Kevin"
    }

    def "test getRange()"() {
        given:
        jedis.set("name", "Gavin")
        when:
        def result = jedis.getrange("name", 1, -2)
        then:
        result == "avi"
    }

    def "test strlen()"() {
        given:
        jedis.set("name", "Gavin Xi")

        when:
        def len = jedis.strlen("name")

        then:
        len == 8
    }

    def "test incr()" (){
        given:
        jedis.set("age", "29")
        when:
        jedis.incr("age")
        then:
        jedis.get("age") == "30"

        when:
        jedis.set("age", "29Y")
        jedis.incr("age")
        then:
        thrown(Exception)
    }

    def "test incrBy()" () {
        given:
        jedis.set("age", "30")
        when:
        jedis.incrBy("age", 5)
        then:
        jedis.get("age") == "35"

        when:
        jedis.incrByFloat("age", 0.35);
        then:
        jedis.get("age") == "35.35"
    }
}
