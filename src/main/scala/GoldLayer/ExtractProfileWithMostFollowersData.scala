package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractProfileWithMostFollowersData {
  def ExtractProfileWithMostFollowers(spark: SparkSession, initialData: DataFrame): DataFrame = {

    initialData.createOrReplaceTempView("profile_view")
    val ProfileWithMostFollowers = spark.sql("SELECT full_name, followers_count FROM profile_view WHERE followers_count = (SELECT MAX(followers_count) FROM profile_view)")
    ProfileWithMostFollowers
  }
}
