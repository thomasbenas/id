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
        //dim_business.printSchema()
        //dim_business.show()

        // Ecriture du dataframe dim_business dans la table dimension_business de la base de données Oracle
        dim_business.write
           .mode(SaveMode.Overwrite)
           .jdbc(urlOracle, "business", connectionPropertiesOracle)

        // DATAFRAME Category
        var dim_category = business_info
            .withColumn("categories", explode(org.apache.spark.sql.functions.split(col("categories"), ",")))

        // Suppression des doublons basée sur le nom de la catégorie
        dim_category = dim_category.distinct()

        // Ajout d'un ID unique à chaque catégorie
        dim_category = dim_category.withColumn("category_id", monotonically_increasing_id())

        dim_category = dim_category.withColumnRenamed("categories", "category_name")

        // Sélection des colonnes nécessaires
        dim_category = dim_category.select("category_id", "category_name")
        dim_category = dim_category.dropDuplicates(Seq("category_name"))

        //Creation de la table dimension_category
        dim_category.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "category", connectionPropertiesOracle)

         val business_category = business_info
            .select("business_id", "categories")
            .withColumn("category_name", explode(split(col("categories"), ",")))
            .join(dim_business, "business_id")
            .join(dim_category, "category_name")
            .select("business_id", "category_id")

        // Écrivez la table de liaison business_category dans la base de données Oracle
        business_category.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "business_category", connectionPropertiesOracle)

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
            .jdbc(urlOracle, "service", connectionPropertiesOracle)

        dim_accessibility.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "accessibility", connectionPropertiesOracle)

        dim_restaurant.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "restaurant", connectionPropertiesOracle)       
        
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

        // Chargement de la table review de la base de données PostgreSQL
        val reviewDF = spark.read
            .jdbc(urlPostgreSQL, "yelp.review", connectionProporetiesPostgreSQL)

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
        
        // Selection des reviews des utilisateurs élites
        val dim_review = reviewDF
            .join(eliteDF, "user_id")
            .select(
                reviewDF.col("review_id"),
                reviewDF.col("user_id"),
                reviewDF.col("business_id"),
                reviewDF.col("stars"),
                reviewDF.col("useful"),
                reviewDF.col("text"),
                reviewDF.col("date")
            )

        // Filtrer les avis avec un texte supérieur à 4000 caractères
        val dim_review_filtered = dim_review.filter(length(dim_review("text")) <= 4000)


        // // Affichage du schéma pour vérification
        // dimension_elite.printSchema()
        // dimension_elite.show()

        // dim_review.show()

        // // Ecriture du DataFrame dans la table review de la base de données Oracle
        // dim_review_filtered.write
        //     .mode(SaveMode.Overwrite)
        //     .jdbc(urlOracle, "review", connectionPropertiesOracle)

        // // Ecriture du DataFrame dans la table dimension_elite de la base de données Oracle
        // dimension_elite.write
        //     .mode(SaveMode.Overwrite)
        //     .jdbc(urlOracle, "elite", connectionPropertiesOracle)

        //  Ecriture de la table de faits dans la base de données Oracle avec buisness_id, review_id, category_id
        val fact_review = reviewDF
            .select(
                reviewDF.col("review_id"),
                business_category.col("business_id"),
                dim_category.col("category_id"),
            )
        
        fact_review.show()

        // // Ecriture du DataFrame dans la table tendency de la base de données Oracle
        fact_review.write
            .mode(SaveMode.Overwrite)
            .jdbc(urlOracle, "tendency", connectionPropertiesOracle)

        // Fermeture de la session Spark
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
