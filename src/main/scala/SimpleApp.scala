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

        ///********** FICHIER CSV **********///
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
        // DATAFRAME Tip
        tip = tip
        .select("tip_id","text","date","fk_user_id")

        // Affichage du dataframe Tip
        tip.printSchema()

        ///********** FICHIER JSON **********///
        // Lecture du fichier business.json
        var business_info = spark.read.json("data/yelp_academic_dataset_business.json").cache()

        // DATAFRAME Business
        var dim_business = business_info
            .select("business_id",
            "name",
            "address",
            "city",
            "state",
            "postal_code",
            "review_count",
            "stars",
            "is_open",
            "latitude",
            "longitude",
            "hours.Monday",
            "hours.Tuesday",
            "hours.Wednesday",
            "hours.Thursday",
            "hours.Friday",
            "hours.Saturday",
            "hours.Sunday")

        business.printSchema()

        // DATAFRAME Category
        var categories_info = business_info
            .withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))
        
        categories_info = categories_info
            .withColumnRenamed("categories", "category_name")
      
        categories_info = categories_info
            .filter(col("category_name").notEqual("None"))
      
        categories_info = categories_info
            .drop(col("categories"))

        // DATAFRAME Service
        var dim_service = business_info
            .select("business_id",
            "attributes.ByAppointmentOnly",
            "attributes.OutdoorSeating",
            "attributes.BusinessAcceptsCreditCards",
            "attributes.WiFi")

        //Conversion des champs en boolean
        dim_service = dim_service
            .withColumn("ByAppointmentOnly", col("ByAppointmentOnly").cast(BooleanType))
            .withColumn("OutdoorSeating", col("OutdoorSeating").cast(BooleanType))
            .withColumn("BusinessAcceptsCreditCards", col("BusinessAcceptsCreditCards").cast(BooleanType))

        //On nettoie la donnée de WiFi pour avoir seulement les termes 
        dim_service = dim_service
            .withColumn("WiFi", regexp_extract(col("WiFi"), "(?:u')?(.*?)'", 1))

        // Remplacement des valeurs null par false
        dim_service = dim_service
            .na
            .fill(false)

        //Création de l'id service_id

        // DATAFRAME Accessibility
        var dim_accessibility = business_info
            .select("business_id",
            "attributes.GoodForKids",
            "attributes.WheelchairAccessible",
            "attributes.BikeParking")

        //Conversion des champs en boolean
        dim_accessibility = dim_accessibility
            .withColumn("GoodForKids", col("GoodForKids").cast(BooleanType))
            .withColumn("WheelchairAccessible", col("WheelchairAccessible").cast(BooleanType))
            .withColumn("BikeParking", col("BikeParking").cast(BooleanType))

        // Remplacement des valeurs null par false
        dim_accessibility = dim_accessibility
            .na
            .fill(false)

        //Création de l'id accessibility_id

        // DATAFRAME Restaurant
        var dim_restaurant = business_info
            .select("business_id",
            "attributes.RestaurantsPriceRange2",
            "attributes.RestaurantsGoodForGroups",
            "attributes.RestaurantsTakeOut",
            "attributes.RestaurantsReservations",
            "attributes.RestaurantsDelivery",
            "attributes.RestaurantsTableService")

        //Conversion des champs en boolean
        dim_restaurant = dim_restaurant
            .withColumn("RestaurantsPriceRange2", col("RestaurantsPriceRange2").cast(BooleanType))
            .withColumn("RestaurantsGoodForGroups", col("RestaurantsGoodForGroups").cast(BooleanType))
            .withColumn("RestaurantsTakeOut", col("RestaurantsTakeOut").cast(BooleanType))
            .withColumn("RestaurantsReservations", col("RestaurantsReservations").cast(BooleanType))
            .withColumn("RestaurantsDelivery", col("RestaurantsDelivery").cast(BooleanType))
            .withColumn("RestaurantsTableService", col("RestaurantsTableService").cast(BooleanType))

        // Remplacement des valeurs null par false
        dim_restaurant = dim_restaurant
            .na
            .fill(false)

        //Création de l'id restaurant_id
        
        // Supression des doublons
        // categories = categories.dropDuplicates()
        
        //Identifiant parking_id, id qui va incrémenter automatiquement
        categories = categories.withColumn("category_id", monotonically_increasing_id())

        categories = categories.select("category_id","category_name")

        // Lecture du fichier checkin.json
        var checkin_info = spark.read.json("data/yelp_academic_dataset_checkin.json").cache()

        var checkin = checkin_info
            .withColumn("checkin_id", monotonically_increasing_id())

        // Paramètres de la connexion BD
        import java.util.Properties
        // BD ORACLE
        Class.forName("oracle.jdbc.driver.OracleDriver")
        val urlOracle = "jdbc:oracle:thin:@stendhal:1521:enss2023"
        val connectionPropertiesOracle = new Properties()
        connectionPropertiesOracle.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
        connectionPropertiesOracle.setProperty("user", "ad578175")
        connectionPropertiesOracle.setProperty("password", "ad578175")

        // BD POSTGRESQL
        val urlPostgreSQL = "jdbc:postgresql://stendhal:5432/tpid2020"
        val connectionProporetiesPostgreSQL = new Properties()
        connectionProporetiesPostgreSQL.setProperty("driver", "org.postgresql.Driver")
        connectionProporetiesPostgreSQL.put("user", "tpid")
        connectionProporetiesPostgreSQL.put("password", "tpid")
        
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
