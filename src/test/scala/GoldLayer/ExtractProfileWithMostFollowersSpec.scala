package GoldLayer

import GoldLayer.ExtractProfileWithMostFollowersData.ExtractProfileWithMostFollowers
import SilverLayer.ExtractProfileData.ExtractProfiles
import SilverLayer.{GraphProfileInfo, ProfileData, info}
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
case class profileWithMaxFollowers(full_name: String, followers_count: Long)
class ExtractProfileWithMostFollowersSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("SilverLayer.Profile with most followers test")
    .getOrCreate()

  import spark.implicits._

  val infoFirstProfile = new info("", 23156762, 1092, "Philippe Coutinho", "1382894360", false, false, false, 700,
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83")
  val infoSecondProfile = new info("", 13156762, 1092, "Cristiano Ronaldo", "1382894360", false, false, false, 618,
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83")
  val graphFistProfileInfo = new GraphProfileInfo(1286323200, infoFirstProfile, "phil.coutinho")
  val graphSecondProfileInfo = new GraphProfileInfo(1286323200, infoSecondProfile, "Cristiano Ronaldo")
  val initialData = Seq(ProfileData(graphFistProfileInfo), ProfileData(graphSecondProfileInfo)).toDF
  val expectedData = Seq(profileWithMaxFollowers("Philippe Coutinho", 23156762))
  val expectedResult = expectedData.toDF

  "ExtractProfileWithMostFollowers" should "Extract The SilverLayer.Profile With Most Followers Data from input data " in {
    Given("The input data")
    val profileData = ExtractProfiles(initialData)

    When("ExtractProfileWithMostFollowers is invoked")
    val profileWithMostFollowers = ExtractProfileWithMostFollowers(spark, profileData)

    Then("the extracted Data should be returned")
    profileWithMostFollowers.collect() should contain theSameElementsAs expectedResult.collect()
  }
}
