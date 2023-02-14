import SilverLayer.ExtractPostsData.ExtractPosts
import GoldLayer.SearchPostByUsernameData.SearchPostByUsername
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
case class SearchPost(username: String, PostId: String)
class SearchPostByUsernameSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("searching test")
    .getOrCreate()

  import spark.implicits._

  val likesforfirstpost = new edge_media_preview_like(983475)
  val likesforsecondpost = new edge_media_preview_like(283475)
  val textforfirstpost = new GraphImageEdgeMediaToCaptionNode("Cada dia é uma nova batalha, que exige o meu máximo! \nA recuperação é lenta, requer paciência e dedicação. \nOs desafios sempre me motivaram. Estou trabalhando firme e estou convicto que voltarei melhor e mais forte a fazer o que mais amo.\nDEUS está comigo e tenho certeza que os que gostam de mim e do meu trabalho também! \nObrigado por toda positividade transmitida.\uD83D\uDCAA\uD83D\uDE4F\n\n‘’Tudo tem o seu tempo determinado, e há tempo para todo o propósito debaixo do céu.”\n\nEclesiastes 3:1\n\n#borapracima #gratidaoaDEUS\n#embrevetamodevolta #féemDEUS #focadoemotivado")
  val nodesforfirstpost = new GraphImageEdgeMediaToCaptionEdge(textforfirstpost)
  val edge_media_to_captionforfirstpost = new edge_media_to_caption(Seq(nodesforfirstpost))
  val edge_media_to_commentforfirstpost = new edge_media_to_comment(80)
  val owner = new owner("1382894360")
  val dimensionDataforfirstpost = dimensionStruct(720, 1080)
  val resourceData1 = resourcesStruct(150, 150, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/c240.0.960.960a/s150x150/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=b805155fe95e252c949c143d61083221&oe=60CBA556&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4")
  val resourceData2 = resourcesStruct(240, 240, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/c240.0.960.960a/s240x240/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=8a57c721e896d6517ecf80dd30672a5e&oe=60CA7354&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4")
  val resourceData3 = resourcesStruct(320, 320, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/c240.0.960.960a/s320x320/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=a6ae38a8ba1a666fd27fff0f3ed15efe&oe=60CAEB6E&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4")
  val resourceData4 = resourcesStruct(480, 480, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/c240.0.960.960a/s480x480/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=0d7731e7dbee346fc9ac665602d7f92d&oe=60CB12EB&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4")
  val resourceData5 = resourcesStruct(640, 640, "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=3620f2a89de0cc082a177e25655dfed8&oe=60CA6853&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4")

  val firstPost = new GraphImageData(
    "GraphImage",
    false,
    dimensionDataforfirstpost,
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=f6c899260225fe952028a55a7f5860a4&oe=60CA4DF3&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2-ccb7-4",
    likesforfirstpost, edge_media_to_captionforfirstpost, edge_media_to_commentforfirstpost,
    null, "2556864304565671217",
    false,
    null,
    "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX/Xj/AHap6wy7EU/eJyPpjn9cYqVuM52inEUlWSX/ALa4jCADjHPNQfaH9qVVzHkMCc8r3wO+aacZ6GkB0Urfv/8AgNLexCaIjgMoyCfbnHrzWG97IG3DGcY6f/XpG1OZgV4weOlSUVvKfbvwdp6H1+nrUdbM8hkt4yfb+YrOxvY59aokgFO3n1pH4J+tNpgf/9k=",
    owner, "CN7zonEg1Ux",
    Seq("embrevetamodevolta", "gratidaoaDEUS", "focadoemotivado", "borapracima", "féemDEUS"),
    1619021998,
    Seq(resourceData1, resourceData2, resourceData3, resourceData4, resourceData5),
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=3620f2a89de0cc082a177e25655dfed8&oe=60CA6853&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4",
    Seq("https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=f6c899260225fe952028a55a7f5860a4&oe=60CA4DF3&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2-ccb7-4"),
    "phil.coutinho")

  val secondPost = new GraphImageData(
    "GraphImage",
    false,
    dimensionDataforfirstpost,
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=f6c899260225fe952028a55a7f5860a4&oe=60CA4DF3&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2-ccb7-4",
    likesforsecondpost
    , edge_media_to_captionforfirstpost,
    edge_media_to_commentforfirstpost,
    null, "2556864304565671219",
    false, null,
    "ACocvTf68f7v9RWXrCybgST5eBgZ4z9PWtSX/Xj/AHap6wy7EU/eJyPpjn9cYqVuM52inEUlWSX/ALa4jCADjHPNQfaH9qVVzHkMCc8r3wO+aacZ6GkB0Urfv/8AgNLexCaIjgMoyCfbnHrzWG97IG3DGcY6f/XpG1OZgV4weOlSUVvKfbvwdp6H1+nrUdbM8hkt4yfb+YrOxvY59aokgFO3n1pH4J+tNpgf/9k=",
    owner, "CN7zonEg1Ux",
    Seq("embrevetamodevolta", "gratidaoaDEUS", "focadoemotivado", "borapracima", "féemDEUS"),
    1619021998,
    Seq(resourceData1, resourceData2, resourceData3, resourceData4, resourceData5),
    "https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c240.0.960.960a/s640x640/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=3620f2a89de0cc082a177e25655dfed8&oe=60CA6853&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2.c-ccb7-4",
    Seq("https://instagram.ftun9-1.fna.fbcdn.net/v/t51.2885-15/e35/s1080x1080/175638912_746496265891329_6399286025486428978_n.jpg?tp=1&_nc_ht=instagram.ftun9-1.fna.fbcdn.net&_nc_cat=1&_nc_ohc=6ye9cBZFVWEAX8MRaVy&edm=APU89FABAAAA&ccb=7-4&oh=f6c899260225fe952028a55a7f5860a4&oe=60CA4DF3&_nc_sid=86f79a&ig_cache_key=MjU1Njg2NDMwNDU2NTY3MTIxNw%3D%3D.2-ccb7-4"),
    "cristiano.ronaldo")
  val expectedData = Seq(SearchPost("phil.coutinho","2556864304565671217"))
  val initialData = Seq(PostData(Array(firstPost, secondPost))).toDF
  val expectedResult = expectedData.toDF

  "SearchPostByUsername" should "Search Posts data by username from input data" in {
    Given("The input data")
    val postsData = ExtractPosts(initialData)

    When("SearchPostByUsername is invoked")
    val searchResults = SearchPostByUsername(spark, postsData, "phil.coutinho")

    Then("the Data should be returned")
    searchResults.collect() should contain theSameElementsAs expectedResult.collect()
  }

  "SearchPostByUsername" should "give back an empty value when no post is found" in {
    Given("The input data")
    val postsData = ExtractPosts(initialData)

    When("SearchPostByUsername is invoked")
    val searchResults = SearchPostByUsername(spark, postsData, "no one")

    Then("the Data should be returned")
    searchResults.collect() should be(empty)
  }
}
