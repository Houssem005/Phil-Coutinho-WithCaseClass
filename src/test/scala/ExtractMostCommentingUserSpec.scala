import SilverLayer.ExtractCommentsData.ExtractComments
import GoldLayer.ExtractMostCommentingUserData.ExtractMostCommentingUsers
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class CommentData(created_at: Long, id: String, owner: owner, text: String)
case class Comment(data: Array[CommentData])
case class GraphImage(__typename: String, comments: Comment, id: String)
case class CommentsData(GraphImages: Array[GraphImage])
case class mostCommentingUsers(owner_id : String, count : Long)

class ExtractMostCommentingUserSpec extends AnyFlatSpec with Matchers with GivenWhenThen{
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Profile with most commenting users test")
    .getOrCreate()
  import spark.implicits._

  val CommentsAndPosts = Seq(CommentsData(Array(GraphImage("GraphImage", Comment(Array(
    CommentData(1619023963, "18209883163069294", owner("20740995"), "💪🏼💪🏼"),
    CommentData(1619023981, "18114517408211027", owner("268668518"), "🙏🏻 Deus não erra, não falha, Ele sabe de todas as coisas! 🙌🏻 Deus está no comando da sua vida e logo vc estará de volta aos campos com força total 🦵🏻 ⚽️ 🥅"),
    CommentData(1619024010, "18220882246023146", owner("242792330"), "Deus no comando, você vai voltar ainda mais forte! 🙏🏽"),
    CommentData(1617213018, "17848025879550357", owner("268668518"), "Parabéns Ainê!! ❤️"),
    CommentData(1617213055, "18216489025016664", owner("268668518"), "Aiii vcs são tão perfeitos ❤️"))),
    "2556864304565671217"))))
  val CommentsAndPostsData = CommentsAndPosts.toDF
  val expectedData = new mostCommentingUsers("268668518",3)
  val CommentsAndPostsAlternative = Seq(CommentsData(Array(GraphImage("GraphImage", Comment(Array(
    CommentData(1619023963, "18209883163069294", owner("20740995"), "💪🏼💪🏼"),
    CommentData(1619023981, "18114517408211027", owner("268668515"), "🙏🏻 Deus não erra, não falha, Ele sabe de todas as coisas! 🙌🏻 Deus está no comando da sua vida e logo vc estará de volta aos campos com força total 🦵🏻 ⚽️ 🥅"),
    CommentData(1619024010, "18220882246023146", owner("242792330"), "Deus no comando, você vai voltar ainda mais forte! 🙏🏽"),
    CommentData(1617213018, "17848025879550357", owner("268668519"), "Parabéns Ainê!! ❤️"),
    CommentData(1617213055, "18216489025016664", owner("268668513"), "Aiii vcs são tão perfeitos ❤️"))),
    "2556864304565671217"))))
  val commentsAndPostsAlternativeData = CommentsAndPostsAlternative.toDF
  val expectedResult = Seq(expectedData).toDF

  "ExtractMostCommentingUsers" should "Extract The users that commented more than once on Posts " +
    "and how many times they commented from input data " in {
    Given("The input data")
    val commentsData = ExtractComments(CommentsAndPostsData)

    When("ExtractProfile is invoked")
    val mostCommentingUsers = ExtractMostCommentingUsers(spark, commentsData)

    Then("the extracted Data should be returned")
    mostCommentingUsers.as[mostCommentingUsers].collect() should contain theSameElementsAs expectedResult.as[mostCommentingUsers].collect()
  }

  "ExtractMostCommentingUsers" should "return empty if there's no users commented more than once from input data " in {
    Given("The input data")
    val commentsData = ExtractComments(commentsAndPostsAlternativeData)

    When("ExtractProfile is invoked")
    val mostCommentingUsers = ExtractMostCommentingUsers(spark, commentsData)

    Then("the extracted Data should be returned")
    mostCommentingUsers.as[mostCommentingUsers].collect() should be (empty)
  }
}
