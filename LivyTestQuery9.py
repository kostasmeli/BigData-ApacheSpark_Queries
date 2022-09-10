from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("How many users watched the same movie the same timestamp")\
        .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
#stagemetrics = StageMetrics(spark)
#stagemetrics.begin()
Movies_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/movie.csv")
      )
Ratings_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/rating.csv")
      )
Ratings_Dataframe=(Ratings_Dataframe.withColumn("day",date_format('timestamp','dd'))
                                    .withColumn("hour",hour("timestamp"))
                  )
rdf=Ratings_Dataframe.groupBy("movieId","day",'hour').agg(count("userId").alias('NumberOfUsers'))
rdf.filter("NumberOfUsers>1").select(sum("NumberOfUsers")).show()
#stagemetrics.end()
#stagemetrics.print_report()
