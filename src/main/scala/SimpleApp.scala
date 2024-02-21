import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DateType, TimestampType}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import java.util.Properties
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.functions.{col, when, regexp_extract}

object App {
    def main(args: Array[String]): Unit = {
        // Initialisation de Spark
        val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()

        // Paramètres de la connexion BD
        // BD POSTGRESQL
        val urlPostgreSQL = "jdbc:postgresql://stendhal:5432/tpid2020"
        val connectionProporetiesPostgreSQL = new Properties()
        connectionProporetiesPostgreSQL.setProperty("driver", "org.postgresql.Driver")
        connectionProporetiesPostgreSQL.put("user", "tpid")
        connectionProporetiesPostgreSQL.put("password", "tpid")
        

        // BD ORACLE
        Class.forName("oracle.jdbc.driver.OracleDriver")
        val urlOracle = "jdbc:oracle:thin:@stendhal:1521:enss2023"
        val connectionPropertiesOracle = new Properties()
        connectionPropertiesOracle.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
        connectionPropertiesOracle.setProperty("user", "ad578175")
        connectionPropertiesOracle.setProperty("password", "ad578175")
   
        val dialect = new OracleDialect
        JdbcDialects.registerDialect(dialect)

        ///********** FICHIER JSON **********///
        // Lecture du fichier business.json
        var business_info = spark.read.json("dataset/yelp_academic_dataset_business.json").cache()

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

        // Affichage du dataframe dim_business
        dim_business.printSchema()
        dim_business.show()

        // Ecriture du dataframe dim_business dans la table dimension_business de la base de données Oracle
        dim_business.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "dimension_business", connectionPropertiesOracle)

        // DATAFRAME Category
        var categories_info = business_info
            .withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))

        // Suppression des doublons basée sur le nom de la catégorie
        categories_info = categories_info.distinct()

        // Ajout d'un ID unique à chaque catégorie
        categories_info = categories_info.withColumn("category_id", monotonically_increasing_id())

        categories_info = categories_info.withColumnRenamed("categories", "category_name")

        // Sélection des colonnes nécessaires
        categories_info = categories_info.select("category_id", "category_name")
        categories_info = categories_info.dropDuplicates(Seq("category_name"))

        //Creation de la table dimension_category
        categories_info.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "dimension_category", connectionPropertiesOracle)


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

        //Renommage de la colonne wifi
        dim_service = dim_service
            .withColumnRenamed("WiFi", "Wifi")

        //On nettoie la donnée de WiFi pour avoir seulement les termes 
        dim_service = dim_service
            .withColumn("Wifi", regexp_extract(col("Wifi"), "^(?:u')?'?([^']*)'?$", 1))

        // Remplacer les valeurs null dans la colonne "Wifi" par "undefined"
        dim_service = dim_service
            .withColumn("Wifi", when(col("Wifi").isNull, lit("undefined")).otherwise(col("Wifi")))

        // Pour les autres champs, si vous souhaitez toujours remplacer null par false
        dim_service = dim_service
            .na.fill(false, Seq("ByAppointmentOnly", "OutdoorSeating", "BusinessAcceptsCreditCards"))

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

        //Ecriture des dataframes dans les tables correspondantes de la base de données Oracle
        dim_service.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "dimension_service", connectionPropertiesOracle)

        dim_accessibility.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "dimension_accessibility", connectionPropertiesOracle)

        dim_restaurant.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "dimension_restaurant", connectionPropertiesOracle)       
        
        // Supression des doublons
        categories_info = categories_info.dropDuplicates()

        // Identifiant category_id, id qui va incrémenter automatiquement
        categories_info = categories_info.withColumn("category_id", monotonically_increasing_id())

        // Sélection des colonnes nécessaires
        categories_info = categories_info.select("category_id", "category_name")


        // Lecture du fichier checkin.json
        var checkin_info = spark.read.json("dataset/yelp_academic_dataset_checkin.json").cache()

        var checkin = checkin_info
            .withColumn("checkin_id", monotonically_increasing_id())

        // Chargement de la table user de la base de données PostgreSQL
        val userDF = spark.read
            .jdbc(urlPostgreSQL, "yelp.user", connectionProporetiesPostgreSQL)

        // Chargement de la table elite de la base de données PostgreSQL
        val eliteDF = spark.read
            .jdbc(urlPostgreSQL, "yelp.elite", connectionProporetiesPostgreSQL)

        // Jointure des DataFrames sur la colonne user_id
        val eliteMembersDF = eliteDF
            .join(userDF, "user_id") // Utilisez "user_id" comme clé de jointure

        // Sélection et transformation des colonnes pour la dimension Elite
        val dimension_elite = eliteMembersDF
            .select(
                eliteDF.col("user_id"),
                eliteDF.col("year"),
                userDF.col("average_stars"),
                userDF.col("useful")
            )

        // Affichage du schéma pour vérification
        dimension_elite.printSchema()
        dimension_elite.show()

        // Ecriture du DataFrame dans la table dimension_elite de la base de données Oracle
        dimension_elite.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "dimension_elite", connectionPropertiesOracle)

        // Ecriture de la table de faits dans la base de données Oracle


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