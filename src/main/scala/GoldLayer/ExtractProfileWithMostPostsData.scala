package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractProfileWithMostPostsData {
  def ExtractProfileWithMostPosts(spark: SparkSession, initialData: DataFrame): DataFrame = {
    initialData.createOrReplaceTempView("profile_view")
    val ProfileWithMostPosts = spark.sql("""SELECT full_name, MAX(posts_count) FROM profile_view""")
    ProfileWithMostPosts
  }
}
