package SilverLayer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

object ExtractCommentsData {
  def ExtractComments(inputData: DataFrame): DataFrame = {
    val CommentsAndPosts = inputData.select(
      explode(inputData("GraphImages")))

    val commentsData = CommentsAndPosts.select(
      explode(col("col.comments.data")), col("col.id").as("profile_id"),
      col("col.__typename").as("typename"))

    commentsData.select(commentsData.col("col.id").as("comment_id"),
      commentsData.col("col.text").as("comment_text"),
      commentsData.col("col.created_at").as("created_at"),
      commentsData.col("col.owner.id").as("owner_id"),
      commentsData.col("profile_id"),
      commentsData.col("typename"))
  }
}
