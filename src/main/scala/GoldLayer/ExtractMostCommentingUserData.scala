package GoldLayer

import org.apache.spark.sql.{DataFrame, SparkSession}
object ExtractMostCommentingUserData {
  def ExtractMostCommentingUsers(spark: SparkSession, initialData: DataFrame): DataFrame = {
    initialData.createOrReplaceTempView("comment_view")
    val mostCommentingUsers =spark.sql(
      """
        |SELECT owner_id, COUNT(owner_id) AS count
        |       FROM comment_view
        |      GROUP BY owner_id
        |      HAVING count(owner_id) > 1
        |""".stripMargin)
    mostCommentingUsers
  }
}
