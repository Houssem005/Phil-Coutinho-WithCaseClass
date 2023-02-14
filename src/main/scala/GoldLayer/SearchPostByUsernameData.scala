package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}

object SearchPostByUsernameData {
  def SearchPostByUsername(spark: SparkSession ,initialData : DataFrame , username : String) : DataFrame ={
    initialData.createOrReplaceTempView("posts_view")
    val searchResult = spark.sql(s"SELECT username, PostId FROM posts_view WHERE username = '$username'")
    searchResult
  }
}
