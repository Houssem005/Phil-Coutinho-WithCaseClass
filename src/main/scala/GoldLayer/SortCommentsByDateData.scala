package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}

object SortCommentsByDateData {
  def sortCommentsByDate(spark: SparkSession, initialData : DataFrame): DataFrame = {
    initialData.createOrReplaceTempView("comments_view")
    val sortedComments = spark.sql("""SELECT comment_id, created_at  FROM comments_view ORDER BY created_at asc""")
    sortedComments
  }
}
