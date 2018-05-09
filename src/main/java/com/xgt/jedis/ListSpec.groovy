package com.xgt.jedis

import redis.clients.jedis.BinaryClient
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException
import spock.lang.Specification

import java.time.LocalTime

class ListSpec extends Specification {
    private Jedis jedis

    void setup() {
        jedis = JedisClientFactory.create()
        jedis.flushAll()
    }

    /**
     * LPUSH key value [value ...]  将一个或多个值 value 插入到列表 key 的表头
     * 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表头： 比如说，对空列表 mylist 执行命令 LPUSH mylist a b c ，列表的值将是 c b a ，
     * 这等同于原子性地执行 LPUSH mylist a 、 LPUSH mylist b 和 LPUSH mylist c 三个命令。
     * 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。
     */
    def "test lpush()" (){
        when:
        def len = jedis.lpush("mylist", "gavin", "kevin", "lily")////返回的是列表的长度!!不是操作数
        then:
        jedis.lrange("mylist", 0, -1) == ["lily", "kevin", "gavin"]
        len == 3
    }

    /**
     * LPUSHX key value
     * 将值 value 插入到列表 key 的表头，当且仅当 key 存在并且是一个列表。
     * 和 LPUSH 命令相反，当 key 不存在时， LPUSHX 命令什么也不做。
     */
    def "test lpushx()" (){
        when:"当mylist不存在的时候，使用lpushx不会做任何操作"
        def len0 = jedis.lpushx("mylist", "a")
        then:
        notThrown(Exception)
        len0 == 0

        when:
        jedis.lpush("mylist", "a")
        jedis.lpushx("mylist", "b", "c")
        then:"lpushx只支持插入一个value，但是API时接受多个参数，搞不懂"
        thrown(JedisDataException)

        when:
        def len = jedis.lpushx("mylist", "b")
        then:
        jedis.lrange("mylist", 0, -1) == ["b", "a"]
        len == 2
    }

    /**
     * 类似于lpush，只是把value依次插入到列表最右边
     * @return
     */
    def "test rpush()" (){
        when:
        def len = jedis.rpush("mylist", "a", "b", "c")
        then:
        jedis.lrange("mylist", 0, -1) == ["a", "b", "c"]
        len == 3
    }
    /**
     * 类似lpushx，只是把新的value插入到列表最右边
     */
    def "test rpushx" () {
        when:
        jedis.rpush("mylist", "a")
        def len = jedis.rpushx("mylist", "b")
        then:
        jedis.lrange("mylist", 0, -1) == ["a", "b"]
        len == 2
    }

    /**
     * LPOP key
     * 移除并返回列表 key 的头元素。
     * <p/>
     * RPOP key
     * 移除并返回列表 key 的尾元素。
     */
    def "test lpop & rpop"() {
        given:
        jedis.rpush("mylist", "a", "b", "c", "d")
        when:
        def left = jedis.lpop("mylist")
        def right = jedis.rpop("mylist")
        then:
        left == "a"
        right == "d"
    }

    /**
     * BLPOP key [key ...] timeout
     * BLPOP 是列表的阻塞式(blocking)弹出原语。
     * 它是 LPOP 命令的阻塞版本，当给定列表内没有任何元素可供弹出的时候，连接将被 BLPOP 命令阻塞，直到等待超时或发现可弹出元素为止。
     * 当给定多个 key 参数时，按参数 key 的先后顺序依次检查各个列表，弹出第一个非空列表的头元素。
     * 非阻塞行为:
     * 当 BLPOP 被调用时，如果给定 key 内至少有一个非空列表，那么弹出遇到的第一个非空列表的头元素，并和被弹出元素所属的列表的名字一起，组成结果返回给调用者。
     * 当存在多个给定 key 时， BLPOP 按给定 key 参数排列的先后顺序，依次检查各个列表。
     * 假设现在有 job 、 command 和 request 三个列表，其中 job 不存在， command 和 request 都持有非空列表。考虑以下命令：BLPOP job command request 0
     * <p/>
     * 超时参数 timeout 接受一个以秒为单位的数字作为值。超时参数设为 0 表示阻塞时间可以无限期延长(block indefinitely) 。
     * <p/>
     * BRPOP key [key ...] timeout
     * 它是 RPOP 命令的阻塞版本，当给定列表内没有任何元素可供弹出的时候，连接将被 BRPOP 命令阻塞，直到等待超时或发现可弹出元素为止。
     * BRPOP 除了弹出元素的位置和 BLPOP 不同之外，其他表现一致。
     */
    def "test brpop()"() { //blpop类似
        when:
        def start1 = LocalTime.now();
        def list0 = jedis.brpop(5, "mylist")
        then:
        list0 == []
        LocalTime.now().minusSeconds(5) >= start1

        when:
        def start2 = LocalTime.now();
        jedis.rpush("mylist", "a")
        def list1 = jedis.brpop(5, "mylist")
        then:
        list1 == ["mylist", "a"]
        LocalTime.now().minusSeconds(5) < start2
    }

    /**
     * RPOPLPUSH source destination
     * 命令 RPOPLPUSH 在一个原子时间内，执行以下两个动作：
     * 1,将列表 source 中的最后一个元素(尾元素)弹出，并返回给客户端。
     * 2,将 source 弹出的元素插入到列表 destination ，作为 destination 列表的的头元素。
     * 举个例子，你有两个列表 source 和 destination ， source 列表有元素 a, b, c ， destination 列表有元素 x, y, z ，
     * 执行 RPOPLPUSH source destination之后，source列表包含元素a,b,destination列表包含元素 c, x, y, z,并且元素c会被返回给客户端。
     * 如果 source 不存在，值 nil 被返回，并且不执行其他动作。
     * 如果 source 和 destination 相同，则列表中的表尾元素被移动到表头，并返回该元素，可以把这种特殊情况视作列表的旋转(rotation)
     * <p/>
     * BRPOPLPUSH source destination timeout
     * BRPOPLPUSH 是 RPOPLPUSH 的阻塞版本，当给定列表 source 不为空时， BRPOPLPUSH 的表现和 RPOPLPUSH 一样。
     * 当列表 source 为空时， BRPOPLPUSH 命令将阻塞连接，直到等待超时，或有另一个客户端对 source 执行 LPUSH 或 RPUSH 命令为止。
     * 超时参数 timeout 接受一个以秒为单位的数字作为值。超时参数设为 0 表示阻塞时间可以无限期延长(block indefinitely) 。
     */
    def "test rpoplpush"() {
        given:
        jedis.rpush("list1", "a", "b", "c")
        jedis.rpush("list2", "x", "y", "z")

        when:
        def result = jedis.rpoplpush("list1", "list2")

        then:
        result == "c"
        jedis.lrange("list1", 0, -1) == ["a", "b"]
        jedis.lrange("list2", 0, -1) == ["c", "x", "y", "z"]
    }

    /**
    * LINDEX key index
    * 返回列表 key 中，下标为 index 的元素。
    * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
    * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
    * 如果 key 不是列表类型，返回一个错误。
    */
    def "test lindex"(){
        given:
        jedis.rpush("mylist", "a", "b", "c")
        when:
        def result1 = jedis.lindex("mylist", 0)
        def result2 = jedis.lindex("mylist", 1)
        def result3 = jedis.lindex("mylist", -1)
        then:
        result1 == "a"
        result2 == "b"
        result3 == "c"
    }

    /**
     * LSET key index value
     * 将列表 key 下标为 index 的元素的值设置为 value 。
     * 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
     * 关于列表下标的更多信息，请参考 LINDEX 命令。
     */
    def "test lset"() {
        given:
        jedis.rpush("mylist", "a", "b")

        when:
        jedis.lset("mylist", 2, "c")
        then: //index 越界
        thrown(Exception)

        when:
        def result = jedis.lset("mylist", 0, "c")
        then:
        result == "OK"
        jedis.lrange("mylist", 0, -1) == ["c", "b"]
    }

    /**
     * LINSERT key BEFORE|AFTER pivot value
     * 将值 value 插入到列表 key 当中，位于值 pivot 之前或之后。
     * 当 pivot 不存在于列表 key 时，不执行任何操作。
     * 当 key 不存在时， key 被视为空列表，不执行任何操作。
     * 如果 key 不是列表类型，返回一个错误。
     */
    def "test linsert"() {
        given:
        jedis.rpush("mylist", "a", "b", "c")
        when:
        jedis.linsert("mylist", BinaryClient.LIST_POSITION.AFTER, "d", "e")
        then: "不执行任何操作"
        jedis.lrange("mylist", 0, -1) == ["a", "b", "c"]

        when:
        jedis.linsert("mylist", BinaryClient.LIST_POSITION.AFTER, "b", "x")
        then:
        jedis.lrange("mylist", 0, -1) == ["a", "b", "x", "c"]
    }

    /**
     * LTRIM key start stop
     * 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
     * 举个例子，执行命令 LTRIM list 0 2 ，表示只保留列表 list 的前三个元素，其余元素全部删除。
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
     */
    def "test ltrim"() {
        given:
        jedis.rpush("mylist", "a", "b", "c")
        when: "去掉首尾"
        jedis.ltrim("mylist", 1, -2)
        then:
        jedis.lrange("mylist", 0, -1) == ["b"]
    }

    /**
     * LREM key count value
     * 根据参数 count 的值，移除列表中与参数 value 相等的元素。
     * count 的值可以是以下几种：
     * count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count 。
     * count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值。
     * count = 0 : 移除表中所有与 value 相等的值。
     */
    def "test lren"() {
        given:
        jedis.rpush("mylist", "a", "b", "a", "b", "c", "d", "d")
        when:
        jedis.lrem("mylist", 1, "a")
        then:
        jedis.lrange("mylist", 0, -1) == ["b", "a", "b", "c", "d", "d"]

        when:
        jedis.lrem("mylist", -1, "b")
        then:
        jedis.lrange("mylist", 0, -1) == ["b", "a", "c", "d", "d"]

        when:
        jedis.lrem("mylist", 0, "d")
        then:
        jedis.lrange("mylist", 0, -1) == ["b", "a", "c"]
    }

    /**
     * LLEN key
     * 返回列表 key 的长度。
     * 如果 key 不存在，则 key 被解释为一个空列表，返回 0 .
     * 如果 key 不是列表类型，返回一个错误。
     */
    def "test llen"() {
        given:
        jedis.rpush("mylist", "a", "b", "a", "b", "c", "d", "d")
        when:
        def l1 = jedis.llen("test")
        def l2 = jedis.llen("mylist")
        then:
        l1 == 0
        l2 == 7
    }
}
