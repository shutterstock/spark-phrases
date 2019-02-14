package com.shutterstock

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Row, DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.sketch.CountMinSketch
import scala.collection.mutable.ListBuffer


/** Phrase transformer for Spark ML pipeline */
class Phraser(override val uid: String) extends Transformer {
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  final val relativeError = new Param[Double](this, "relativeError", "Relative error for count min sketch")
  final val confidence = new Param[Double](this, "confidence", "Confidence for count min sketch")
  final val randomSeed = new Param[Integer](this, "randomSeed", "Random seed for count min sketch")
  final val minCount = new Param[Double](this, "minCount", "Minimum term count")
  final val threshold = new Param[Double](this, "threshold", "Score threshold for forming phrases")

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def setRelativeError(value: Double): this.type = set(relativeError, value)
  def setConfidence(value: Double): this.type = set(confidence, value)
  def setRandomSeed(value: Integer): this.type = set(randomSeed, value)
  def setMinCount(value:Double): this.type = set(minCount, value)
  def setThreshold(value:Double): this.type = set(threshold, value)

  setDefault(outputCol -> "phrases")
  setDefault(relativeError -> 0.01)
  setDefault(confidence -> 0.9)
  setDefault(randomSeed -> 42)
  setDefault(minCount -> 1)
  setDefault(threshold -> 1)

  def this() = this(Identifiable.randomUID("phraser"))

  /** Copy the current pipeline stage with additional parameters.  */
  def copy(addParams: ParamMap): Phraser = {
    defaultCopy(addParams)
  }

  /** Transform schema for pipeline stage output */
  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != ArrayType) {
      throw new Exception(s"Input type ${field.dataType} must be ArrayType")
    }
    schema.add(StructField($(outputCol), ArrayType(StringType), false))
  }

  /** Function generator for replacing sequences of co-occurring tokens with a phrase.
   *
   *  @param countMinSketchBroadcast A count min sketch object containing token and combinded token
   *                                 frequencies.
   *  @return A function which transforms a sequence of tokens to a sequence of tokens and phrases.
   */
  def addPhrases(countMinSketchBroadcast: Broadcast[CountMinSketch]) : Seq[String] => Seq[String] = { 
    tokens =>
      val listBuffer = new ListBuffer[String]()
      val countMinSketch = countMinSketchBroadcast.value

      var skipNext = false
      tokens.toIterator.sliding(2).foreach {
        tuple =>
          if (!skipNext) {
            val term = tuple.head
            val next_term = tuple.last
            val phrase = tuple.mkString("_")

            val score = 
              if (countMinSketch.estimateCount(term) < $(minCount) || 
                  countMinSketch.estimateCount(next_term) < $(minCount)) 
                0
              else 
                ((countMinSketch.estimateCount(phrase) - $(minCount)) / 
                  countMinSketch.estimateCount(term) / 
                  countMinSketch.estimateCount(next_term) * countMinSketch.totalCount())

            if (score >= $(threshold)) {
              listBuffer += phrase
              skipNext = true
            } else
              listBuffer += term
          } else
            skipNext = false
      }
      if (listBuffer.last != tokens.takeRight(2).mkString("_"))
        listBuffer += tokens.last
      listBuffer.toSeq
  }

  /** Calculates token and phrase frequencies and then replaces token sequences with significant phrases.
   *
   *  @param dataset Input dataset with a column to apply phrase transformation.
   *  @return A dataset with an additional column containing a sequence of tokens and phrases.
   */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val countMinSketch = CountMinSketch.create($(relativeError), $(confidence), $(randomSeed))

    dataset.select(col($(inputCol))).rdd.flatMap { 
      row : Row  => (
        row.get(0).asInstanceOf[TraversableOnce[String]].toIterator ++ 
        row.get(0).asInstanceOf[TraversableOnce[String]].toIterator.sliding(2).map(_.mkString("_"))
      )
    }.toLocalIterator.foreach(countMinSketch.addString)

    val phraseReplace = udf(addPhrases(
      dataset.rdd.context.broadcast(countMinSketch)
    ))
    dataset.withColumn($(outputCol), phraseReplace(dataset.col($(inputCol))))
  }
}
