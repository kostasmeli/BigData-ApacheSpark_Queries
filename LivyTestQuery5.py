from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("Tags and Title for every movie for 2015")\
        .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
#stagemetrics = StageMetrics(spark)
#stagemetrics.begin()
Tags_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/tag.csv")
      )
Movies_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/movie.csv")
      )
Tags_Dataframe=Tags_Dataframe.withColumn("timestamp",date_format("timestamp","yyy"))

Tagdf=(Tags_Dataframe.where("timestamp==2015")
                     .drop('userId')
                     .join(Movies_Dataframe,Movies_Dataframe.movieId==Tags_Dataframe.movieId)
                     .drop(Movies_Dataframe.movieId)
                     .drop(Movies_Dataframe.genres)   
      )

Tagdf=Tagdf.groupBy("title").agg(collect_list("tag"))

Tagdf.withColumn("tags", concat_ws(",", "collect_list(tag)")) \
     .drop("collect_list(tag)")\
     .sort('title',ascending=True)\
     .show(n=5,truncate=0)
#stagemetrics.end()
#stagemetrics.print_report()
