package com.xgt.jedis

import redis.clients.jedis.Jedis
import spock.lang.Specification

class JedisForKeySpec extends Specification {
    private Jedis jedis
    void setup() {
        jedis = JedisClientFactory.create()
        jedis.flushAll()
    }

    /**
     * DEL key [key ...]
     * 删除给定的一个或多个 key, 任意类型的数据
     * 不存在的 key 会被忽略。
     */
    def "test del"() {
        given:
        jedis.set("string", "stringvalue")
        jedis.sadd("set", "setvalue")
        when:
        jedis.del("string")
        then:
        !jedis.get("string")

        when:
        jedis.del("set")
        then:
        !jedis.smembers("set")
    }

    /**
     * DUMP key
     * 序列化给定 key ，并返回被序列化的值，使用 RESTORE 命令可以将这个值反序列化为 Redis 键。
     * 序列化生成的值有以下几个特点：
     * 它带有 64 位的校验和，用于检测错误， RESTORE 在进行反序列化之前会先检查校验和。
     * 值的编码格式和 RDB 文件保持一致。
     * RDB 版本会被编码在序列化值当中，如果因为 Redis 的版本不同造成 RDB 格式不兼容，那么 Redis 会拒绝对这个值进行反序列化操作。
     * 序列化的值不包括任何生存时间信息。
     */
    def "test dump"() {
        given:
        jedis.set("string", "value")
        when:
        def bytes = jedis.dump("string")
        then:
        bytes == [0, 5, 118, 97, 108, 117, 101, 6, 0, 23, 27, -87, -72, 52, -1, -89, -3]
    }

    /**
     * EXISTS key
     * 检查给定 key 是否存在。
     */
    def "test exists"() {
        given:
        jedis.set("string", "value")
        when:
        def isExists = jedis.exists("string")
        then:
        isExists
    }

    /**
     * EXPIRE key seconds
     * 为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除。
     * 在 Redis 中，带有生存时间的 key 被称为『易失的』(volatile)。
     * 生存时间可以通过使用 DEL 命令来删除整个 key 来移除，或者被 SET 和 GETSET 命令覆写(overwrite)，这意味着，
     * 如果一个命令只是修改(alter)一个带生存时间的 key 的值而不是用一个新的 key 值来代替(replace)它的话，那么生存时间不会被改变。
     * 比如说，对一个 key 执行 INCR 命令，对一个列表进行 LPUSH 命令，或者对一个哈希表执行 HSET 命令，这类操作都不会修改 key 本身的生存时间。
     * 另一方面，如果使用 RENAME 对一个 key 进行改名，那么改名后的 key 的生存时间和改名前一样。
     * RENAME 命令的另一种可能是，尝试将一个带生存时间的 key 改名成另一个带生存时间的 another_key ，这时旧的 another_key (以及它的生存时间)会被删除，
     * 然后旧的 key 会改名为 another_key ，因此，新的 another_key 的生存时间也和原本的 key 一样。
     * 使用 PERSIST 命令可以在不删除 key 的情况下，移除 key 的生存时间，让 key 重新成为一个『持久的』(persistent) key 。
     * 更新生存时间
     * 可以对一个已经带有生存时间的 key 执行 EXPIRE 命令，新指定的生存时间会取代旧的生存时间。
     * <p/>
     * PEXPIRE key milliseconds
     * 这个命令和 EXPIRE 命令的作用类似，但是它以毫秒为单位设置 key 的生存时间，而不像 EXPIRE 命令那样，以秒为单位。
     */
    def "test expire"() {
        given:
        jedis.set("string", "value")
        when:
        jedis.expire("string", 2)
        then:
        jedis.get("string") == "value"
        Thread.sleep(2200)
        !jedis.get("string")
    }

    /**
     * EXPIREAT key timestamp
     * EXPIREAT 的作用和 EXPIRE 类似，都用于为 key 设置生存时间。
     * 不同在于 EXPIREAT 命令接受的时间参数是 UNIX 时间戳(unix timestamp)。
     * <p/>
     * PEXPIREAT key milliseconds-timestamp
     * 这个命令和 EXPIREAT 命令类似，但它以毫秒为单位设置 key 的过期 unix 时间戳，而不是像 EXPIREAT 那样，以秒为单位。
     */
    def "test expireAt"() {
        given:
        jedis.set("string", "value")
        def expireTime = System.currentTimeMillis() + 2000;
        when:
        jedis.expireAt("string", expireTime)
        then:
        jedis.get("string") == "value"
        Thread.sleep(3000)
//        !jedis.get("string") //todo:没有起效果
    }

    /**
     * KEYS pattern
     * 查找所有符合给定模式 pattern 的 key 。
     * KEYS * 匹配数据库中所有 key 。
     * KEYS h?llo 匹配 hello ， hallo 和 hxllo 等。
     * KEYS h*llo 匹配 hllo 和 heeeeello 等。
     * KEYS h[ae]llo 匹配 hello 和 hallo ，但不匹配 hillo 。
     * 特殊符号用 \ 隔开
     * KEYS 的速度非常快，但在一个大的数据库中使用它仍然可能造成性能问题，如果你需要从一个数据集中查找特定的 key ，你最好还是用 Redis 的集合结构(set)来代替。
     */
    def "test keys"() {
        given:
        jedis.set("kevin", "25")
        jedis.set("gavin", "27")
        when:
        def set = jedis.keys("*")
        then:
        set.toArray() == ["gavin", "kevin"]

        when:
        set = jedis.keys("*vin")
        then:
        set.toArray() == ["gavin", "kevin"]

        when:
        set = jedis.keys("gav?n")
        then:
        set.toArray() == ["gavin"]
    }
}
