from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("Top 10 movies for every year")\
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
Ratings_Dataframe=Ratings_Dataframe.withColumn("timestamp",date_format("timestamp","yyy"))
tdf=(Ratings_Dataframe.groupBy(["timestamp","movieId"]).avg("rating")
     .join(Movies_Dataframe,Movies_Dataframe.movieId==Ratings_Dataframe.movieId)
     .drop(Movies_Dataframe.movieId)
     .drop(Movies_Dataframe.genres) 
     .sort("timestamp")
    )
windowDept = Window.partitionBy("timestamp").orderBy(col("avg(rating)").desc())
FullQuery=(tdf.withColumn("row",row_number().over(windowDept))
           .filter(col("row") <= 10)
           .drop("row")
          )
FullQuery.where("timestamp==2005").sort("title").show(truncate=0)
#stagemetrics.end()
#stagemetrics.print_report()
