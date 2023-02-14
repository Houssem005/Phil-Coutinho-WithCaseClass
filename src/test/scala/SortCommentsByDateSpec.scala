
import SilverLayer.ExtractCommentsData.ExtractComments
import GoldLayer.SortCommentsByDateData.sortCommentsByDate
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
case class SortedComments(comment_id:String, created_at:Long)
class SortCommentsByDateSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("sort comments test")
    .getOrCreate()
  import spark.implicits._

  val ownerComment = new owner("20740995")
  val CommentsAndPosts = Seq(CommentsData(Array(GraphImage("GraphImage", Comment(
    Array(CommentData(1619023963, "18209883163069294", ownerComment, "should be second"),
      CommentData(1620023963, "18209883163069290", ownerComment, "should be first"))), "2556864304565671217"))))
  val CommentsAndPostsData = CommentsAndPosts.toDF
  val expectedResult = Seq(SortedComments("18209883163069290",1620023963),
    SortedComments("18209883163069294",1619023963)).toDF

  "sortCommentsByDate" should "sort The comment descending by date(timestamp) from input data " in {
    Given("The input data")
    val commentsData = ExtractComments(CommentsAndPostsData)

    When("sortCommentsByDate is invoked")
    val sortedComments = sortCommentsByDate(spark, commentsData)

    Then("the sorted Data should be returned")
    sortedComments.collect() should contain theSameElementsAs expectedResult.collect()
  }
}
