package com.xgt.jedis

import redis.clients.jedis.Jedis
import spock.lang.Specification

class HashSpec extends Specification {
    private Jedis jedis

    void setup() {
        jedis = JedisClientFactory.create()
        jedis.flushAll()
    }

    /**
     * HSET key field value
     * 将哈希表 key 中的域 field 的值设为 value 。
     * 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。
     * 如果域 field 已经存在于哈希表中，旧值将被覆盖。
     * <p/>
     * HMSET key field value [field value ...]
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。
     * 此命令会覆盖哈希表中已存在的域。
     * 如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
     * <p/>
     * HSETNX key field value
     * 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在。
     * 若域 field 已经存在，该操作无效。
     * 如果 key 不存在，一个新哈希表被创建并执行 HSETNX 命令。
     */
    def "test hset"() {
        when:
        jedis.hset("hash", "name", "Gavin")
        then:
        jedis.hgetAll("hash") == ["name": "Gavin"]

        when:
        jedis.hsetnx("hash", "name", "Kevin")
        then:
        jedis.hgetAll("hash") == ["name": "Gavin"]

        when:
        jedis.hsetnx("hash", "age", "30")
        then:
        jedis.hgetAll("hash") == ["name": "Gavin", "age": "30"]
    }

    /**
     * HKEYS key
     * 返回哈希表 key 中的所有域。
     */
    def "test hkeys"() {
        given:
        jedis.hset("hash", "name", "Gavin")
        jedis.hset("hash", "age", "30")

        when:
        def result = jedis.hkeys("hash")

        then:
        result.contains("name")
        result.contains("age")
        result.size() == 2
    }

    /**
     * HVALS key
     * 返回哈希表 key 中所有域的值。
     */
    def "test hvals"() {
        given:
        jedis.hset("hash", "name", "Gavin")
        jedis.hset("hash", "age", "30")

        when:
        def result = jedis.hvals("hash")

        then:
        result == ["Gavin", "30"]
    }

    /**
     * HEXISTS key field
     * 查看哈希表 key 中，给定域 field 是否存在。
     */
    def "test hexists"() {
        given:
        jedis.hset("hash", "name", "Gavin")
        jedis.hset("hash", "age", "30")

        when:
        def isExistName = jedis.hexists("hash", "name")
        def isExistGender = jedis.hexists("hash", "gender")

        then:
        isExistName
        !isExistGender
    }

    /**
     * HDEL key field [field ...]
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
     */
    def "test hdel"() {
        given:
        jedis.hset("hash", "name", "Gavin")
        jedis.hset("hash", "age", "30")

        when:
        jedis.hdel("hash", "name", "age")

        then:
        jedis.hkeys("hash").size() == 0
    }

    /**
     * HINCRBY key field increment
     * 为哈希表 key 中的域 field 的值加上增量 increment 。
     * 增量也可以为负数，相当于对给定域进行减法操作。
     * 如果 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
     * 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。
     * 本操作的值被限制在 64 位(bit)有符号数字表示之内。
     * <p/>
     * HINCRBYFLOAT key field increment
     * 为哈希表 key 中的域 field 加上浮点数增量 increment 。
     * 如果哈希表中没有域 field ，那么 HINCRBYFLOAT 会先将域 field 的值设为 0 ，然后再执行加法操作。
     * 如果键 key 不存在，那么 HINCRBYFLOAT 会先创建一个哈希表，再创建域 field ，最后再执行加法操作。
     * 当以下任意一个条件发生时，返回一个错误：
     * 域 field 的值不是字符串类型(因为 redis 中的数字和浮点数都以字符串的形式保存，所以它们都属于字符串类型）
     * 域 field 当前的值或给定的增量 increment 不能解释(parse)为双精度浮点数(double precision floating point number)
     */
    def "test hincrby"() {
        given:
        jedis.hset("hash", "name", "Gavin")
        jedis.hset("hash", "age", "30")

        when:
        jedis.hincrBy("hash", "age", 5)
        then:
        jedis.hget("hash", "age") == "35"

        when:
        jedis.hincrBy("hash", "name", 5)
        then:
        thrown(Exception)
    }

    /**
     * HLEN key
     * 返回哈希表 key 中域的数量。
     */
    def "test hlen"() {
        given:
        jedis.hset("hash", "name", "Gavin")
        jedis.hset("hash", "age", "30")

        when:
        def len = jedis.hlen("hash")

        then:
        len == 2
    }
}
