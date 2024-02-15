import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DateType, TimestampType}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

object App {
    def main(args: Array[String]): Unit = {
        // Initialisation de Spark
        val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()


        // lecture du fichier tip.csv
        var tip = spark.read
        .option("header",true)
        .csv("data/yelp_academic_dataset_tip.csv")
        .select("user_id","date","text")

        tip = tip
            .withColumn("tip_id", monotonically_increasing_id())
        tip = tip
        .withColumn("date", col("date").cast(TimestampType))

        tip = tip
        .withColumnRenamed("user_id", "fk_user_id")
        // Dataframe Tip (tip_id, text, date, fk_user_id)
        tip = tip
        .select("tip_id","text","date","fk_user_id")

        // Affichage du dataframe Tip
        tip.printSchema()

        import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
        val dialect = new OracleDialect
        JdbcDialects.registerDialect(dialect)
        spark.stop()
    }


class OracleDialect extends JdbcDialect {
override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.INTEGER))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("VARCHAR2(4000)", java.sql.Types.VARCHAR))
    case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
    //Ligne ajoutée pour timestamp
    case TimestampType => Some(JdbcType("TIMESTAMP",java.sql.Types.TIMESTAMP))
    case _ => None
}
override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
}
}
