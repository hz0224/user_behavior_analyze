import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Liu HangZhou on 2020/05/27
  * desc: 热门商品统计: 每隔5分钟输出最近一小时内点击量最多的前N个商品
  */
//数据源样例类
case class UserBehavior(userId: Long,itemId: Long,categoryId: Int,behavior: String,ts: Long)

//窗口聚合结果的样例类
case class ItemViewCount(itemId: Long, count: Long, windowEnd: Long)

object HotItems {
  def main(args: Array[String]): Unit = {

    //创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //为了使打印结果的顺序不乱，设置并行度为1
    env.setParallelism(1)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\UserBehavior.csv")
    val userBehaviorDStream = inputDStream.map { line =>
      //315321,942195,4339722,pv,1511658000
      val data = line.split(",")
      UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
    }//设置时间语义字段  由于数据源中的数据时间是升序的，即数据已经有序，因此这里只需要分配时间字段即可，不需要定义水位(延迟)
      .assignAscendingTimestamps(_.ts * 1000L)   //时间语义字段是毫秒

    //因为后面要对数据按照窗口进行排序，如果滑动的时间间隔特别小的话，滑动窗口计算后(按照itemId count后，即 itemId count)
    //你是没办法判断该条结果数据是属于当前窗口的还是属于下个窗口的，因为这些结果数据都在同一条流中，因此必须对每一条结果数据加上所属窗口的信息。(itemId count window)
    val aggDStream = userBehaviorDStream
      .filter { userBehavior =>"pv".equals(userBehavior.behavior)}
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new ItemCountWindowResult())   //aggregate就是一个增量聚合函数，因此它必须传入一个增量聚合函数。
    //CountAgg是增量聚合函数 ， ItemCountWindowResult是窗口函数,可以拿到增量聚合的结果和该窗口的信息，它的输入数据是增量聚合函数CountAgg的输出。
    //因此此时的窗口函数计算的就不是整个窗口内的所有数据了，它只是对预聚合的结果进行一个包装，加上了该窗口的信息。相当于将增量聚合和窗口函数进行了整合。



    aggDStream.print()

    env.execute("hot_items_job")
  }
}

//AggregateFunction<IN, ACC, OUT>   IN: 输入数据类型    ACC: 中间结果数据类型   OUT: 输出结果数据类型
//自定义预聚合函数，来一条数据加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{

  //来一条数据累加器 +1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //定义累加器初始值
  override def createAccumulator(): Long = 0L
  //返回结果
  override def getResult(acc: Long): Long = acc
  //合并
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//[IN, OUT, KEY, W <: Window]
//自定义窗口函数，结合window信息包装成样例类.
class ItemCountWindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  //此时的Iterable[Long]中只有一条数据，就是预聚合的结果.
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId= key
    val end = window.getEnd
    val count = input.iterator.next()
    //使用 out.collect(输出结果类型)输出数据
    out.collect(ItemViewCount(itemId,count,end))
  }
}