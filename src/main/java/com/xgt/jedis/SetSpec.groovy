package com.xgt.jedis

import redis.clients.jedis.Jedis
import spock.lang.Specification

class SetSpec extends Specification {
    private Jedis jedis

    void setup() {
        jedis = JedisClientFactory.create()
        jedis.flushAll()
    }

    /**
     * SADD key member [member ...]
     * 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
     * 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
     * 当 key 不是集合类型时，返回一个错误。
     */
    def "test sadd"() {
        when:
        jedis.sadd("set", "a", "b", "c")
        then:
        containsAll(jedis.smembers("set"), "a", "b", "c")
    }

    /**
     * SCARD key
     * 返回集合 key 的基数(集合中元素的数量)。
     */
    def "test scard"() {
        given:
        jedis.sadd("set", "a", "b", "c")
        when:
        def result = jedis.scard("set")
        then:
        result == 3
    }

    /**
     * SDIFF key [key ...]
     * 返回一个集合的全部成员，该集合是所有给定集合之间的差集。
     * 不存在的 key 被视为空集。
     * <p/>
     * SDIFFSTORE destination key [key ...]
     * 这个命令的作用和 SDIFF 类似，但它将结果保存到 destination 集合，而不是简单地返回结果集。
     * 如果 destination 集合已经存在，则将其覆盖。
     * destination 可以是 key 本身。
     */
    def "test sdiff & sdiffstore"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        jedis.sadd("set2", "b", "c", "e")

        when: "set1中不同于set2的集合"
        def result1 = jedis.sdiff("set1", "set2")
        then:
        containsAll(result1, "a", "d")

        when: "set2中不同于set1的集合"
        def result2 = jedis.sdiff("set2", "set1")
        then:
        containsAll(result2, "e")

        when:
        def count = jedis.sdiffstore("destination", "set1", "set2")
        def dest = jedis.smembers("destination")
        then:
        count == 2
        containsAll(result1, "a", "d")
    }

    /**
     * SINTER key [key ...]
     * 返回一个集合的全部成员，该集合是所有给定集合的交集。
     * 不存在的 key 被视为空集。
     * 当给定集合当中有一个空集时，结果也为空集(根据集合运算定律)。
     * <p/>
     * SINTERSTORE destination key [key ...]
     * 这个命令类似于 SINTER 命令，但它将结果保存到 destination 集合，而不是简单地返回结果集。
     * 如果 destination 集合已经存在，则将其覆盖。
     * destination 可以是 key 本身。
     */
    def "test sinter"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        jedis.sadd("set2", "b", "c", "e")

        when:
        def s = jedis.sinter("set1", "set2")
        then:
        containsAll(s, "b", "c")

        when:
        def count = jedis.sinterstore("destination", "set1", "set2")
        then:
        count == 2
        containsAll(jedis.smembers("destination"), "b", "c")
    }

    /**
     * SISMEMBER key member
     * 判断 member 元素是否集合 key 的成员。
     */
    def "test sismember"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        when:
        def isExistA = jedis.sismember("set1", "a");
        def isExistE = jedis.sismember("set1", "e");
        then:
        isExistA
        !isExistE
    }

    /**
     * SMOVE source destination member
     * 将 member 元素从 source 集合移动到 destination 集合。
     * SMOVE 是原子性操作。
     * 如果source集合不存在或不包含指定的member元素，则SMOVE命令不执行任何操作，仅返回0。否则,member元素从source集合中被移除，并添加到destination 集合中去。
     * 当 destination 集合已经包含 member 元素时， SMOVE 命令只是简单地将 source 集合中的 member 元素删除。
     * 当 source 或 destination 不是集合类型时，返回一个错误。
     */
    def "test smove"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        jedis.sadd("set2", "b", "c", "e")
        when:
        jedis.smove("set1", "set2", "a");
        then:
        !jedis.sismember("set1", "a")
        jedis.sismember("set2", "a")
    }

    /**
    * SPOP key
    * 移除并返回集合中的一个随机元素。
    * 如果只想获取一个随机元素，但不想该元素从集合中被移除的话，可以使用 SRANDMEMBER 命令。
    */
    def "test spop"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        when:
        def r = jedis.spop("set1")
        then:
        println(r)
        jedis.smembers("set1").size() == 3

//        when:
//        def rs = jedis.spop("set1", 2) //逆天API，大于1的都不行
//        then:
//        println(rs)
//        jedis.smembers("set1") == 1
    }

    /**
     * SRANDMEMBER key [count]
     * 如果命令执行时，只提供了 key 参数，那么返回集合中的一个随机元素。
     * 从 Redis 2.6 版本开始， SRANDMEMBER 命令接受可选的 count 参数：
     * 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
     * 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
     * 该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而 SRANDMEMBER 则仅仅返回随机元素，而不对集合进行任何改动。
     */
    def "test srandmember"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        when:
        def r = jedis.srandmember("set1")
        then:
        println(r)
        jedis.smembers("set1").size() == 4

        when:
        def rs = jedis.srandmember("set1", 2)
        then:
        println(rs)
        jedis.smembers("set1").size() == 4
    }

    /**
     * SREM key member [member ...]
     * 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
     * 当 key 不是集合类型，返回一个错误。
     */
    def "test srem"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        when:
        def count = jedis.srem("set1", "a", "d", "e")
        then:
        count == 2
        containsAll(jedis.smembers("set1"), "b", "c")
    }

    /**
     * SUNION key [key ...]
     * 返回一个集合的全部成员，该集合是所有给定集合的并集。
     * 不存在的 key 被视为空集。
     * <p/>
     * SUNIONSTORE destination key [key ...]
     * 这个命令类似于 SUNION 命令，但它将结果保存到 destination 集合，而不是简单地返回结果集。
     * 如果 destination 已经存在，则将其覆盖。
     * destination 可以是 key 本身。
     */
    def "test sunion"() {
        given:
        jedis.sadd("set1", "a", "b", "c", "d")
        jedis.sadd("set2", "b", "c", "e")
        when:
        def res = jedis.sunion("set1", "set2")
        then:
        containsAll(res, "a", "b", "c", "d", "e")

        when:
        def count = jedis.sunionstore("dest", "set1", "set2")
        then:
        count == 5
        containsAll(jedis.smembers("dest"), "a", "b", "c", "d", "e")
    }

    private boolean containsAll(Collection set, String ...values) {
        def flag = true;
        for(String v: values) {
            flag = flag && set.contains(v)
        }

        return flag && set.size() == values.size()
    }
}
