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

object PhilCoutinhoMain {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Main Class")
      .getOrCreate()
    import spark.implicits._
    val inputData = spark.read.option("multiline", value = true).json("phil.coutinho-1.json")
    //Extract Comments Data
    val commentsData = ExtractComments(inputData)
    commentsData.write.parquet("E:\\dataset\\phil.Coutinho\\SilverLayer\\Comments")
    //Extract Profile Data
    val profilesData = ExtractProfiles(inputData)
    profilesData.write.parquet("E:\\dataset\\phil.Coutinho\\SilverLayer\\Profiles")
    //Extract Posts Data
    val postsData = ExtractPosts(inputData)
    postsData.write.parquet("E:\\dataset\\phil.Coutinho\\SilverLayer\\Posts")
    //Extract Most Commenting User
    val mostCommentingUsers = ExtractMostCommentingUsers(spark, commentsData)
    mostCommentingUsers.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\MostCommentingUsers")
    //Extract Most Liked Post
    val mostLikedPost = ExtractMostLikedPost(spark,postsData)
    mostLikedPost.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\MostLikedPost")
    //Extract profile With Most Followers
    val mostFollowedProfile = ExtractProfileWithMostFollowers(spark,profilesData)
    mostFollowedProfile.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\MostFollowedProfile")
    //Extract Profile With Most Posts
    val profileWithMostPosts = ExtractProfileWithMostPosts(spark, profilesData)
    profileWithMostPosts.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\ProfileWithMostPosts")
    //Order Posts data ascending by timestamp
    val OrderedPosts = orderPosts(spark, postsData)
    OrderedPosts.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\OrderedPosts")
    //Sort The Comment by date(timestamp)
    val SortedComments = sortCommentsByDate(spark, commentsData)
    SortedComments.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\SortedComments")
    //Search Posts by username
    val searchResults = SearchPostByUsername(spark, postsData, "phil.coutinho")
    searchResults.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\searchResults")
    //Extract likes and dates data
    val LikesByDates = ExtractLikesWithDate(spark, postsData)
    LikesByDates.write.parquet("E:\\dataset\\phil.Coutinho\\GoldLayer\\LikesByDates")
  }
}
