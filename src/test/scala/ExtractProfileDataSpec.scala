import SilverLayer.ExtractProfileData.ExtractProfiles
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class info(biography:String , followers_count : Long , following_count : Long , full_name : String,
                id:String,is_business_account:Boolean,is_joined_recently:Boolean,
                is_private:Boolean,posts_count:Long,profile_pic_url:String)
case class GraphProfileInfo(created_time:Long,info:info,username:String)
case class ProfileData(graphProfileInfo: GraphProfileInfo)
case class Profile ( createdTime :Long , username : String, biography : String, followers_count : Long ,
                     following_count : Long , full_name : String , id : String , is_business_account : Boolean ,
                     is_joined_recently : Boolean , is_private : Boolean , posts_count : Long , profile_pic_url : String)
class ExtractProfileDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Profile test")
    .getOrCreate()
  import spark.implicits._




  "ExtractProfiles" should "Extract Profile Data from input data " in {
    Given("The input file")
    val info = new info("", 23156762, 1092, "Philippe Coutinho", "1382894360", false, false, false, 618,
      "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83")
    val graphProfileInfo = new GraphProfileInfo(1286323200, info, "phil.coutinho")
    val initialData = Seq(ProfileData(graphProfileInfo))
    val profileData = initialData.toDF

    When("ExtractProfile is invoked")
    val profiles = ExtractProfiles(profileData)

    Then("the extracted Data should be returned")
    val expectedData = Seq(Profile(1286323200, "phil.coutinho", "", 23156762, 1092, "Philippe Coutinho", "1382894360", is_business_account = false, is_joined_recently = false, is_private = false, 618, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-19/s150x150/69437559_363974237877617_991135940606951424_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_ohc=uiYY_up9lLwAX8rG9wR&edm=ABfd0MgBAAAA&ccb=7-4&oh=c3a24d2609c83e4cf8d017318f3b034e&oe=60CBC5C0&_nc_sid=7bff83"))
    val expectedResult = expectedData.toDF
    profiles.as[Profile].collect() should contain theSameElementsAs expectedResult.as[Profile].collect()
  }
}
