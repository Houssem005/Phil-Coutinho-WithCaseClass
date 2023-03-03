import GoldLayer.ExtractLikesWithDateData.ExtractLikesWithDate
import GoldLayer.ExtractMostCommentingUserData.ExtractMostCommentingUsers
import GoldLayer.ExtractMostLikedPostData.ExtractMostLikedPost
import GoldLayer.ExtractProfileWithMostFollowersData.ExtractProfileWithMostFollowers
import GoldLayer.ExtractProfileWithMostPostsData.ExtractProfileWithMostPosts
import GoldLayer.OrderPostsData.orderPosts
import GoldLayer.SearchPostByUsernameData.SearchPostByUsername
import GoldLayer.SortCommentsByDateData.sortCommentsByDate
import SilverLayer.ExtractCommentsData.ExtractComments
import SilverLayer.ExtractPostsData.ExtractPosts
import SilverLayer.ExtractProfileData.ExtractProfiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object PhilCoutinhoMain {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Main Class")
      .getOrCreate()
    import spark.implicits._

    val inputData = spark.read.option("multiline", value = true).json("phil.coutinho-1.json")
    val commentsData = ExtractComments(inputData)
    commentsData.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\SilverLayer\\Comments")
    val profilesData = ExtractProfiles(inputData)
    profilesData.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\SilverLayer\\Profiles")
    val postsData = ExtractPosts(inputData)
    postsData.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\SilverLayer\\Posts")
    val mostCommentingUsers = ExtractMostCommentingUsers(spark, commentsData)
    mostCommentingUsers.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\MostCommentingUsers")
    val mostLikedPost = ExtractMostLikedPost(spark,postsData)
    mostLikedPost.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\MostLikedPost")
    val mostFollowedProfile = ExtractProfileWithMostFollowers(spark,profilesData)
    mostFollowedProfile.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\MostFollowedProfile")
    val profileWithMostPosts = ExtractProfileWithMostPosts(spark, profilesData)
    profileWithMostPosts.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\ProfileWithMostPosts")
    val OrderedPosts = orderPosts(spark, postsData)
    OrderedPosts.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\OrderedPosts")
    val SortedComments = sortCommentsByDate(spark, commentsData)
    SortedComments.write.
      mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\SortedComments")
    val searchResults = SearchPostByUsername(spark, postsData, "phil.coutinho")
    searchResults.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\searchResults")
    val LikesByDates = ExtractLikesWithDate(spark, postsData)
    LikesByDates.write
      .mode("append")
      .parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\LikesByDates")
    //Working On Delta
    commentsData.write
      .mode("append")
      .option("mergeSchema", "true")
      .option("overwriteSchema", "true")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\SilverLayer\\Comments")
    profilesData.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\SilverLayer\\Profiles")
    postsData.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\SilverLayer\\Posts")
    mostCommentingUsers.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\MostCommentingUser")
    mostLikedPost.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\MostLikedPost")
    mostFollowedProfile.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\mostFollowedProfile")
    profileWithMostPosts.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\profileWithMostPosts")
    OrderedPosts.write
      .mode("append")
      .format("delta").save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\OrderedPosts")
    SortedComments.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\SortedComments")
    searchResults.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\searchResults")
    LikesByDates.write
      .mode("append")
      .format("delta")
      .save("E:\\dataset\\phil.Coutinho_Delta\\GoldLayer\\LikesByDates")
  }
}
