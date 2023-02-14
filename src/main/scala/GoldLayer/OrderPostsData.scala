package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}
object OrderPostsData {
  def orderPosts(spark: SparkSession,initialData:DataFrame):DataFrame ={
    initialData.createOrReplaceTempView("posts_view")
    val orderedPosts = spark.sql("SELECT PostId, taken_as_timestamp  FROM posts_view ORDER BY taken_as_timestamp asc")
    orderedPosts
  }
}
