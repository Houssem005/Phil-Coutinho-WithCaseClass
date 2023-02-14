import SilverLayer.ExtractCommentsData.ExtractComments
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Comments ( comment_id: String , comment_text : String , created_at : Long , owner_id : String , profile_id : String, typename: String)
class ExtractCommentsDataSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Comments test")
    .getOrCreate()
  import spark.implicits._

  val owner = new owner("20740995")
  val initialData = Seq(CommentsData(Array(GraphImage("GraphImage", Comment(
    Array(CommentData(1619023963, "18209883163069294", owner, "💪🏼💪🏼"))), "2556864304565671217"))))

  "ExtractComments" should "Extract Comments Data from input data" in {
    Given("The input data")
    val comments = initialData.toDF

    When("ExtractComments is invoked")
    val commentsData = ExtractComments(comments)

    Then("the extracted Data should be returned")
    val expectedData = Seq(Comments("18209883163069294","💪🏼💪🏼",1619023963, "20740995", "2556864304565671217","GraphImage"))
    val expectedResult = expectedData.toDF
    commentsData.as[Comments].collect() should contain theSameElementsAs expectedResult.as[Comments].collect()
  }
}



