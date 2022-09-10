from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("Number of Rating for every movie")\
        .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
#stagemetrics = StageMetrics(spark)
#stagemetrics.begin()
Ratings_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/rating.csv")
      )
Movies_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/movie.csv")
      )

newdf=(Ratings_Dataframe.groupBy("movieId").count()
                        .join(Movies_Dataframe,Movies_Dataframe.movieId==Ratings_Dataframe.movieId)
                        .drop(Movies_Dataframe.movieId)
                        .drop(Movies_Dataframe.genres)
                        .drop(Ratings_Dataframe.movieId)
                        .withColumnRenamed('count', 'NumberOfRatings')
      )
newdf.sort("NumberOfRatings",ascending=False).show(n=5,truncate=0)
#stagemetrics.end()
#stagemetrics.print_report()
