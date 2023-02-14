package SilverLayer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.explode

object ExtractPostsData {
  def ExtractPosts(initialData: DataFrame): DataFrame = {
    val PostsData = initialData.select(explode(initialData("GraphImages")))
    PostsData.select(PostsData.col("col.__typename").as("typename"),
      PostsData.col("col.comments_disabled").as("comments_disabled"),
      PostsData.col("col.dimensions").as("dimensions"),
      PostsData.col("col.display_url"),
      PostsData.col("col.edge_media_preview_like.count").as("likes_count"),
      PostsData.col("col.edge_media_to_caption.edges.node.text").as("post_text"),
      PostsData.col("col.edge_media_to_comment.count").as("number_of_comments"),
      PostsData.col("col.gating_info"),
      PostsData.col("col.id").as("PostId"),
      PostsData.col("col.is_video"),
      PostsData.col("col.location"),
      PostsData.col("col.media_preview"),
      PostsData.col("col.owner.id").as("ProfileId"),
      PostsData.col("col.shortcode").as("shortcode"),
      PostsData.col("col.tags").as("tags"),
      PostsData.col("col.taken_at_timestamp").as("taken_as_timestamp"),
      PostsData.col("col.thumbnail_resources").as("resources"),
      PostsData.col("col.thumbnail_src").as("thumbnail_src"),
      PostsData.col("col.urls").as("urls"),
      PostsData.col("col.username").as("username"))
  }
}
