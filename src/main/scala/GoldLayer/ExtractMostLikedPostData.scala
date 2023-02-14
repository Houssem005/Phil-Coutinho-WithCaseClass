package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}
object ExtractMostLikedPostData {
  def ExtractMostLikedPost(spark: SparkSession, initialData: DataFrame): DataFrame = {
    initialData.createOrReplaceTempView("posts_view")
    val mostLikedPost = spark.sql("SELECT PostId, likes_count  FROM posts_view WHERE likes_count = (SELECT MAX(likes_count) FROM posts_view)")
    mostLikedPost
  }
}
