package com.xgt.jedis

import redis.clients.jedis.Jedis
import redis.clients.jedis.ZParams
import spock.lang.Specification
import redis.clients.jedis.Tuple
import spock.lang.Unroll

class SortedSetSpec extends Specification {
    private Jedis jedis

    void setup() {
        jedis = JedisClientFactory.create()
        jedis.flushAll()
    }

    /**
     * ZADD key score member [[score member] [score member] ...]
     * 将一个或多个 member 元素及其 score 值加入到有序集 key 当中。
     * 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该 member 在正确的位置上。
     * score 值可以是整数值或双精度浮点数。
     * 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。
     * 当 key 存在但不是有序集类型时，返回一个错误。
     */
    def "test zadd"() {
        when:
        jedis.zadd("sset", 1, "a")
        jedis.zadd("sset", ["b": 2d, "c": 3d])
        then:
        jedis.zrange("sset", 0, -1).toArray() == ["a", "b", "c"]

        when:
        jedis.zadd("sset", 1.5, "x")
        then:
        jedis.zrange("sset", 0, -1).toArray() == ["a", "x", "b", "c"]

        when:
        jedis.zadd("sset", 1.6, "a")
        then:
        jedis.zrange("sset", 0, -1).toArray() == ["x", "a", "b", "c"]
    }

    /**
     * 获取member总数
     */
    def "test zcard"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d])
        when:
        def count = jedis.zcard("sset")
        then:
        count == 3
    }

    /**
     * ZCOUNT key min max
     * 返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量。
     * 关于参数 min 和 max 的详细使用方法，请参考 ZRANGEBYSCORE 命令。
     */
    def "test zcount"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def count1 = jedis.zcount("sset", "2", "3")
        then:
        count1 == 2

        when:
        def count2 = jedis.zcount("sset", 2, 3)
        then:
        count2 == 2
    }

    /**
     * ZINCRBY key increment member
     * 为有序集 key 的成员 member 的 score 值加上增量 increment 。
     * 可以通过传递一个负数值 increment ，让 score 减去相应的值，比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。
     * 当 key 不存在，或 member 不是 key 的成员时， ZINCRBY key increment member 等同于 ZADD key increment member 。
     * 当 key 不是有序集类型时，返回一个错误。
     * score 值可以是整数值或双精度浮点数。
     */
    def "test zincrby"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        jedis.zincrby("sset", 2.1, "a")
        then:
        getSortedArray() == ["b", "c", "a", "d"]

        when:
        jedis.zincrby("sset", -2.1, "a")
        then:
        getSortedArray() == ["a", "b", "c", "d"]

        when:
        jedis.zincrby("sset", 1.5, "x")
        then:
        getSortedArray() == ["a", "x", "b", "c", "d"]
    }

    /**
     * ZRANGE key start stop [WITHSCORES]
     * 返回有序集 key 中，指定区间内的成员。
     * 其中成员的位置按 score 值递增(从小到大)来排序。
     * 具有相同 score 值的成员按字典序(lexicographical order )来排列。
     * 如果你需要成员按 score 值递减(从大到小)来排列，请使用 ZREVRANGE 命令。
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
     * 超出范围的下标并不会引起错误。
     * 比如说，当 start 的值比有序集的最大下标还要大，或是 start > stop 时， ZRANGE 命令只是简单地返回一个空列表。
     * 另一方面，假如 stop 参数的值比有序集的最大下标还要大，那么 Redis 将 stop 当作最大下标来处理。
     * 可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回，返回列表以 value1,score1, ..., valueN,scoreN 的格式表示。
     * 客户端库可能会返回一些更复杂的数据类型，比如数组、元组等。
     * <p/>
     * ZREVRANGE key start stop [WITHSCORES]
     * 返回有序集 key 中，指定区间内的成员。
     * 其中成员的位置按 score 值递减(从大到小)来排列。
     * 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order)排列。
     * 除了成员按 score 值递减的次序排列这一点外， ZREVRANGE 命令的其他方面和 ZRANGE 命令一样。
     */
    def "test zrange"() {
        when:
        jedis.zadd("sset", ["abcdf": 1d, "abcdef": 1d, "bac": 1d])
        then:
        getSortedArray() == ["abcdef", "abcdf", "bac"]

        when:
        def arr = jedis.zrevrange("sset", 0, -1).toArray()
        then:
        arr == ["bac", "abcdf", "abcdef"]
    }

    /**
     * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score 值递增(从小到大)次序排列。
     * 具有相同 score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     * 可选的 WITHSCORES 参数决定结果集是单单返回有序集的成员，还是将有序集成员及其 score 值一起返回。
     * 该选项自 Redis 2.0 版本起可用。
     * 区间及无限
     * min 和 max 可以是 -inf 和 +inf ，这样一来，你就可以在不知道有序集的最低和最高 score 值的情况下，使用 ZRANGEBYSCORE 这类命令。
     * 默认情况下，区间的取值使用闭区间 (小于等于或大于等于)，你也可以通过给参数前增加 ( 符号来使用可选的开区间 (小于或大于)。
     * <p/>
     * ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
     * 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score 值递减(从大到小)的次序排列。
     * 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order )排列。
     * 除了成员按 score 值递减的次序排列这一点外， ZREVRANGEBYSCORE 命令的其他方面和 ZRANGEBYSCORE 命令一样。
     */
    def "test zrangebyscore"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def set1 = jedis.zrangeByScore("sset", 2d, 3d)
        then:
        set1.toArray() == ["b", "c"]

        when:
        def set2 = jedis.zrangeByScore("sset", 2d, 4d, 0, 2)
        then:
        set2.toArray() == ["b", "c"]

        when:
        def set3 = jedis.zrangeByScore("sset", "-inf", "+inf", 1, 2)
        then:
        set3.toArray() == ["b", "c"]

        when:
        LinkedHashSet<Tuple> set4 = jedis.zrangeByScoreWithScores("sset", "-inf", "+inf", 1, 2)
        then:
        ((Tuple) (set4.toArray()[0])).getElement() == "b"
        ((Tuple) (set4.toArray()[0])).getScore() == 2d
        ((Tuple) (set4.toArray()[1])).getElement() == "c"
        ((Tuple) (set4.toArray()[1])).getScore() == 3d
    }

    /**
     * ZRANK key member
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。
     * 排名以 0 为底，也就是说， score 值最小的成员排名为 0 。
     * 使用 ZREVRANK 命令可以获得成员按 score 值递减(从大到小)排列的排名。
     * <p/>
     * ZREVRANK key member
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。
     * 排名以 0 为底，也就是说， score 值最大的成员排名为 0 。
     * 使用 ZRANK 命令可以获得成员按 score 值递增(从小到大)排列的排名。
     */
    def "test zrank"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def index = jedis.zrank("sset", "b")
        then:
        index == 1

        when:
        def revIndex = jedis.zrevrank("sset", "b")
        then:
        revIndex == 2
    }

    /**
     * remove member(s) by key(s)
     */
    def "test zrem"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def removedCount = jedis.zrem("sset", "a", "e")
        then:
        removedCount == 1
        getSortedArray() == ["b", "c", "d"]
    }

    /**
     * ZREMRANGEBYRANK key start stop
     * 移除有序集 key 中，指定排名(rank)区间内的所有成员。
     * 区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内。
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
     */
    def "test zremrangebyrank"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def removedCount = jedis.zremrangeByRank("sset", 1, 3)
        then:
        removedCount == 3
        getSortedArray() == ["a"]
    }

    /**
     * ZREMRANGEBYSCORE key min max
     * 移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
     * 自版本2.1.6开始， score 值等于 min 或 max 的成员也可以不包括在内，详情请参见 ZRANGEBYSCORE 命令。
     */
    def "test zremrangebyscore"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def removedCount = jedis.zremrangeByScore("sset", 2d, 4d)
        then:
        removedCount == 3
        getSortedArray() == ["a"]
    }

    /**
     * ZSCORE key member
     * 返回有序集 key 中，成员 member 的 score 值。
     * 如果 member 元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
     */
    def "test zscore"() {
        given:
        jedis.zadd("sset", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        when:
        def score = jedis.zscore("sset", "b")
        then:
        score == 2d
    }

    /**
     * ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 计算给定的一个或多个有序集的并集，其中给定 key 的数量必须以 numkeys 参数指定，并将该并集(结果集)储存到 destination 。
     * 默认情况下，结果集中某个成员的 score 值是所有给定集下该成员 score 值之 和 。
     * WEIGHTS
     * 使用 WEIGHTS 选项，你可以为 每个 给定有序集 分别 指定一个乘法因子(multiplication factor)，
     * 每个给定有序集的所有成员的 score 值在传递给聚合函数(aggregation function)之前都要先乘以该有序集的因子。
     * 如果没有指定 WEIGHTS 选项，乘法因子默认设置为 1 。
     * AGGREGATE
     * 使用 AGGREGATE 选项，你可以指定并集的结果集的聚合方式。
     * 默认使用的参数 SUM ，可以将所有集合中某个成员的 score 值之 和 作为结果集中该成员的 score 值；使用参数 MIN ，
     * 可以将所有集合中某个成员的最小score值作为结果集中该成员的score值；而参数MAX则是将所有集合中某个成员的最大score值作为结果集中该成员的score值。
     */
    @Unroll
    def "test zunionscore"() {
        given:
        jedis.zadd("sset1", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        jedis.zadd("sset2", ["e": 4d, "b": 3d, "c": 2d, "d": 1d])
        def zparams = new ZParams()
        zparams.aggregate(aggregate)
        zparams.weightsByDouble(weightsForSet1, weightsForSet2)
        when:
        def count = jedis.zunionstore("dst", zparams, "sset1", "sset2")
        then:
        count == 5
        scoreA == jedis.zscore("dst", "a")
        scoreB == jedis.zscore("dst", "b")
        scoreC == jedis.zscore("dst", "c")
        scoreD == jedis.zscore("dst", "d")
        scoreE == jedis.zscore("dst", "e")
        where:
        aggregate             | weightsForSet1 | weightsForSet2 | scoreA | scoreB | scoreC | scoreD | scoreE
        ZParams.Aggregate.MAX | 1              | 1              | 1d     | 3d     | 3d     | 4d     | 4d
        ZParams.Aggregate.MIN | 1              | 1              | 1d     | 2d     | 2d     | 1d     | 4d
        ZParams.Aggregate.SUM | 1              | 1              | 1d     | 5d     | 5d     | 5d     | 4d
        ZParams.Aggregate.MAX | 1              | 2              | 1d     | 6d     | 4d     | 4d     | 8d
        ZParams.Aggregate.MIN | 1              | 2              | 1d     | 2d     | 3d     | 2d     | 8d
        ZParams.Aggregate.SUM | 1              | 2              | 1d     | 8d     | 7d     | 6d     | 8d
    }

    /**
     * ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
     * 计算给定的一个或多个有序集的交集，其中给定 key 的数量必须以 numkeys 参数指定，并将该交集(结果集)储存到 destination 。
     * 默认情况下，结果集中某个成员的 score 值是所有给定集下该成员 score 值之和.
     * 关于 WEIGHTS 和 AGGREGATE 选项的描述，参见 ZUNIONSTORE 命令。
     */
    def "test zinterstore"() {
        given:
        jedis.zadd("sset1", ["a": 1d, "b": 2d, "c": 3d, "d": 4d])
        jedis.zadd("sset2", ["e": 4d, "b": 3d, "c": 2d, "d": 1d])
        def zparams = new ZParams()
        zparams.aggregate(aggregate)
        zparams.weightsByDouble(weightsForSet1, weightsForSet2)
        when:
        def count = jedis.zinterstore("dst", zparams, "sset1", "sset2")
        then:
        count == 3
        scoreB == jedis.zscore("dst", "b")
        scoreC == jedis.zscore("dst", "c")
        scoreD == jedis.zscore("dst", "d")
        where:
        aggregate             | weightsForSet1 | weightsForSet2 | scoreB | scoreC | scoreD
        ZParams.Aggregate.MAX | 1              | 1              | 3d     | 3d     | 4d
        ZParams.Aggregate.MIN | 1              | 1              | 2d     | 2d     | 1d
        ZParams.Aggregate.SUM | 1              | 1              | 5d     | 5d     | 5d
        ZParams.Aggregate.MAX | 1              | 2              | 6d     | 4d     | 4d
        ZParams.Aggregate.MIN | 1              | 2              | 2d     | 3d     | 2d
        ZParams.Aggregate.SUM | 1              | 2              | 8d     | 7d     | 6d
    }

    /**
     * ZRANGEBYLEX key min max [LIMIT offset count]
     * 当有序集合的所有成员都具有相同的分值时， 有序集合的元素会根据成员的字典序（lexicographical ordering）来进行排序，
     * 而这个命令则可以返回给定的有序集合键 key 中， 值介于 min 和 max 之间的成员。
     * 如果有序集合里面的成员带有不同的分值， 那么命令返回的结果是未指定的（unspecified）。
     * 命令会使用 C 语言的 memcmp() 函数， 对集合中的每个成员进行逐个字节的对比（byte-by-byte compare）， 并按照从低到高的顺序， 返回排序后的集合成员。
     * 如果两个字符串有一部分内容是相同的话， 那么命令会认为较长的字符串比较短的字符串要大。
     * 可选的 LIMIT offset count 参数用于获取指定范围内的匹配元素 （就像 SQL 中的 SELECT LIMIT offset count 语句）。 需要注意的一点是，
     * 如果 offset 参数的值非常大的话， 那么命令在返回结果之前， 需要先遍历至 offset 所指定的位置， 这个操作会为命令加上最多 O(N) 复杂度。
     * 如何指定范围区间
     * 合法的 min 和 max 参数必须包含 ( 或者 [ ， 其中 ( 表示开区间（指定的值不会被包含在范围之内）， 而 [ 则表示闭区间（指定的值会被包含在范围之内）。
     * 特殊值 + 和 - 在 min 参数以及 max 参数中具有特殊的意义， 其中 + 表示正无限， 而 - 表示负无限。 因此， 向一个所有成员的分值都相同的有序集合发送命令
     * ZRANGEBYLEX <zset> - + ， 命令将返回有序集合中的所有元素。
     */
    def "test zrangebylex"() {
        given:
        jedis.zadd("sset", ["abcdf": 1d, "abcdef": 1d, "bac": 1d, "ccc": 3d, "sss": 5d])
        when:
        def result = jedis.zrangeByLex("sset", "[a", "[bac")
        then:
        result.toArray() == ["abcdef", "abcdf", "bac"]

        when:
        result = jedis.zrangeByLex("sset", "[a", "[bac", 0, 2)
        then:
        result.toArray() == ["abcdef", "abcdf"]

        when:
        result = jedis.zrangeByLex("sset", "[a", "(bac")
        then:
        result.toArray() == ["abcdef", "abcdf"]

        when:
        result = jedis.zrangeByLex("sset", "[a", "[ccc")
        then:
        result.toArray() == ["abcdef", "abcdf", "bac", "ccc"]

        when:
        jedis.zadd("sset", 2d, "eee")
        result = jedis.zrangeByLex("sset", "[a", "[ccc")
        then: "不包含ccc，因为ccc和bac之间还有一个eee，但是查询范围是a到ccc，不包含e，所以在此处断开了"
        result.toArray() == ["abcdef", "abcdf", "bac"]

        when:
        jedis.zadd("sset", 2d, "eee")
        result = jedis.zrangeByLex("sset", "[a", "(f")
        then: "包含ccc和eee"
        result.toArray() == ["abcdef", "abcdf", "bac", "eee", "ccc"]
    }

    /**
     * ZLEXCOUNT key min max
     * 对于一个所有成员的分值都相同的有序集合键 key 来说， 这个命令会返回该集合中， 成员介于 min 和 max 范围内的元素数量。
     * 这个命令的 min 参数和 max 参数的意义和 ZRANGEBYLEX 命令的 min 参数和 max 参数的意义一样。
     */
    def "test zlexcount"() {
        given:
        jedis.zadd("sset", ["abcdf": 1d, "abcdef": 1d, "bac": 1d, "ccc": 3d, "sss": 5d])
        when:
        def count = jedis.zlexcount("sset", "(a", "(c")
        then:
        count == 3
    }

    /**
     * ZREMRANGEBYLEX key min max
     * 对于一个所有成员的分值都相同的有序集合键 key 来说， 这个命令会移除该集合中， 成员介于 min 和 max 范围内的所有元素。
     * 这个命令的 min 参数和 max 参数的意义和 ZRANGEBYLEX 命令的 min 参数和 max 参数的意义一样。
     */
    def "test zremrangebylex"() {
        given:
        jedis.zadd("sset", ["abcdf": 1d, "abcdef": 1d, "bac": 1d, "ccc": 3d, "sss": 5d])
        when:
        def count = jedis.zremrangeByLex("sset", "(a", "(c")
        then:
        count == 3
        getSortedArray() == ["ccc", "sss"]
    }

    private String[] getSortedArray(String key) {
        return jedis.zrange(key, 0, -1).toArray();
    }

    private String[] getSortedArray() {
        return getSortedArray("sset")
    }
}
