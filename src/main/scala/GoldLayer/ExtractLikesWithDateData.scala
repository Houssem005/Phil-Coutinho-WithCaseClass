package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractLikesWithDateData {
  def ExtractLikesWithDate(spark: SparkSession, initialData: DataFrame): DataFrame = {
    initialData.createOrReplaceTempView("posts_view")
    val mostLikedPost = spark.sql("SELECT likes_count, taken_as_timestamp  FROM posts_view ")
    mostLikedPost
  }
}
