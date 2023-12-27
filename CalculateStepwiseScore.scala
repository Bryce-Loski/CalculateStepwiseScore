package com.anjuke.dw.flink.broker.log10.flinkUtil

import com.anjuke.dw.flink.common.JsonUtils

import scala.collection.immutable.TreeMap

/**
 * 工具名:阶梯式计算工具类.
 * 作  者:王紫阳
 * 工具介绍:
 * 1.0 版本只考虑左开右闭的版本,默认步长为1
 *
 * 传入参数为阶梯式(电费计算)的表达式例如:
 * {
 *"(0,10]": 0.2,
 *"(10,15]": 0.1,
 *"(15,": 0.05
 *}
 *
 * 该表达式表明了,步长为1,0-10区间,每个为0.2分,10-15区间时每个为0.1分,15个以上时,每个为0.05分.
 * 例如:
 *  n=20 -> result: 10*0.2+5*0.1+5*0.05 = 2.75
 *  n=5  -> result: 5*0.2 = 1
 *  n=13 -> result: 10*0.2+3*0.1
 * @param rules 需要计算的规则json
 * @param step 步长
 * @param value 计算值
 */
class CalculateStepwiseScore(rules:String,step:Double,value:Double) {

  /**
   * 检测表达式是否符合规范:
   *  1. 检查区间是否有重叠
   *  2. 区间的表达式使用的符号只能为 (] 或( 形成的封闭或者半封闭空间
   */
  private def checkExpression(stepWise:TreeMap[String,Double]):Boolean = {
    if(!checkWithOverlap(stepWise)) {throw new Exception("表达式有重叠区间,请检查")}
    if(!checkWithStructure(stepWise)) {throw new Exception("表达式目前必须为左开右闭或者左开右不限")}

    true // 表达式符合规范
  }


  /**
   * 表达式目前必须为左开右闭或者左开右不限
   */
  private def checkWithStructure(stepWise:TreeMap[String,Double]):Boolean = {
    stepWise.keySet.foreach{
      range => {
        val bytesWithRang = range.toCharArray
        if(!bytesWithRang(0).equals('(') || (!(bytesWithRang.last.equals(']') || bytesWithRang.last.equals(',')))) {
          return false
        }
      }
    }
    true
  }

  /**
   * 检测表达式是否重叠
   * @param stepWise 表达式
   * @return
   */
  private def checkWithOverlap(stepWise:TreeMap[String,Double]):Boolean = {
    var lastUpperBound: Double = Double.NaN
    for ((interval, _) <- stepWise) {
      val bounds = interval.substring(1, interval.length - 1).split(",")
      val currentLowerBound = bounds(0).toDouble
      if (!lastUpperBound.isNaN && currentLowerBound < lastUpperBound) {
        return false
      }
      lastUpperBound = if (bounds.length == 1) Double.NaN else bounds(1).toDouble
    }
    true
  }

  private def dealJson(rules:String) = {
    val map = JsonUtils.parseJson[Map[String, Double]](rules) match {
      case Some(x) => x
      case None => throw new ClassCastException(s"非标准Json,$rules")
    }
    TreeMap(map.toSeq: _*)
  }

  /**
   * 计算核心逻辑(只考虑左开右闭):
   *  对于给定的n.
   *    1. 如果n大于当前区间的最大值,则使用最大值*当前区间的值
   *    2. 如果n在当前区间内,则使用(n-当前区间的最小值)*当前区间的值
   *    3. 如果n比最小的区间的值还要小,则抛出error
   * @return
   */
  def calculate():Double = {
    import scala.util.control.Breaks._
    val n = this.value
    val rulesMap = dealJson(rules)
    checkExpression(rulesMap)

//    var result:Double = 0.0
    var result = BigDecimal(0)

    rulesMap.foreach(
      entry => breakable{
        val interval = entry._1
        val value = entry._2

        //解析区间的上下限
        val bounds = {
          //结构固定为左开右闭,所以只取中间部分进行计算
          interval.substring(1, interval.length - 1).split(",")
        }
        val lowerBound = bounds(0).toDouble

        //如果给定的值比区域下限要低,直接返回
        if(lowerBound >= n) break()

        //如果右边是无上限,直接给一个极大值
        val upperBound = if(bounds.length == 1) Double.PositiveInfinity else bounds(1).toDouble

        //计算在当前区间的分数,如果分数超过了上限,就使用(上限-下限)*区间值
        val intervalWidth = Math.min(upperBound,n) - lowerBound
        result += BigDecimal(intervalWidth) * BigDecimal(value)
      }
    )
    result.toDouble
  }
}

object CalculateStepwiseScore {
  def main(args: Array[String]): Unit = {
    val rules = """
                  |{
                  |    "(0,":0.01
                  |}
                   |""".stripMargin


    println(new CalculateStepwiseScore(rules, 1, 0).calculate())
    println(new CalculateStepwiseScore(rules, 1, 1).calculate())
    println(new CalculateStepwiseScore(rules, 1, 2).calculate())
    println(new CalculateStepwiseScore(rules, 1, 3).calculate())
    println(new CalculateStepwiseScore(rules, 1, 4).calculate())
    println(new CalculateStepwiseScore(rules, 1, 5).calculate())
    println(new CalculateStepwiseScore(rules, 1, 6).calculate())
    println(new CalculateStepwiseScore(rules, 1, 7).calculate())
    println(new CalculateStepwiseScore(rules, 1, 8).calculate())
    println(new CalculateStepwiseScore(rules, 1, 9).calculate())
  }
}
