package SilverLayer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object ExtractProfileData {
  def ExtractProfiles(initialData: DataFrame): DataFrame = {
    val profileData = initialData.select(col("GraphProfileInfo"))
    profileData.select(profileData.col("GraphProfileInfo.created_time") as ("createdTime"),
      profileData.col("GraphProfileInfo.username").as("username"),
      profileData.col("GraphProfileInfo.info.biography"), profileData.col("GraphProfileInfo.info.followers_count"),
      profileData.col("GraphProfileInfo.info.following_count"),
      profileData.col("GraphProfileInfo.info.full_name"),
      profileData.col("GraphProfileInfo.info.id"),
      profileData.col("GraphProfileInfo.info.is_business_account"),
      profileData.col("GraphProfileInfo.info.is_joined_recently"), profileData.col("GraphProfileInfo.info.is_private"),
      profileData.col("GraphProfileInfo.info.posts_count"), profileData.col("GraphProfileInfo.info.profile_pic_url"))
  }
}
