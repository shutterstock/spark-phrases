package com.shutterstock

import org.scalatest.Suite
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait SparkContextSession extends BeforeAndAfterAll { self: Suite =>
  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll() {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    super.beforeAll()
  }

  override def afterAll() {
    if (sc != null)
      sc.stop()
    super.afterAll()
  }
}


class PhraserSuite extends FunSuite with SparkContextSession {

  test("Phrases single pass") {
    val schema = List(StructField("tokens", ArrayType(StringType)))

    val data = sc.parallelize(
      Seq(Row(Array("new", "york", "city")),
          Row(Array("new", "york", "state")),
          Row(Array("new", "york")))
    )

    val expected_data = sc.parallelize(
      Seq(Row(Array("new_york", "city")),
          Row(Array("new_york", "state")),
          Row(Array("new_york")))
    )

    val df = sqlContext.createDataFrame(data, StructType(schema))
    val phrase_df = new Phraser()
      .setInputCol("tokens")
      .setOutputCol("phrased_tokens")
      .transform(df)
      .select(col("phrased_tokens").alias("tokens"))

    val expected_df = sqlContext.createDataFrame(expected_data, StructType(schema))

    assert(phrase_df.collect().deep == expected_df.collect().deep)
  }

  test("Phrases double pass") {
    val schema = List(StructField("tokens", ArrayType(StringType)))

    val data = sc.parallelize(
      Seq(Row(Array("greater", "new", "york", "city")),
          Row(Array("northern", "new", "york", "state")),
          Row(Array("southern", "new", "york", "state")))
    )

    val expected_data = sc.parallelize(
      Seq(Row(Array("greater", "new_york", "city")),
          Row(Array("northern", "new_york_state")),
          Row(Array("southern", "new_york_state")))
    )

    val df = sqlContext.createDataFrame(data, StructType(schema))
    val phrase_df = new Phraser()
      .setInputCol("tokens")
      .setOutputCol("phrased_tokens")
      .transform(df)
    val second_phrase_df = new Phraser()
      .setInputCol("phrased_tokens")
      .setOutputCol("double_phrased_tokens")
      .transform(phrase_df)
      .select(col("double_phrased_tokens").alias("tokens"))

    val expected_df = sqlContext.createDataFrame(expected_data, StructType(schema))

    assert(second_phrase_df.collect().deep == expected_df.collect().deep)
  }

}
