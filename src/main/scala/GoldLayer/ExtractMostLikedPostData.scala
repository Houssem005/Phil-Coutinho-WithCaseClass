package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}
object ExtractMostLikedPostData {
  def ExtractMostLikedPost(spark: SparkSession, initialData: DataFrame): DataFrame = {
    initialData.createOrReplaceTempView("posts_view")
    val mostLikedPost = spark.sql(
      """
        |SELECT PostId, MAX(likes_count) FROM posts_view""".stripMargin)
    mostLikedPost
  }
}
