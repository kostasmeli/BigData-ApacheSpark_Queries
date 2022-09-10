#from sparkmeasure import StageMetrics
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("A Boring Query")\
        .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
#stagemetrics = StageMetrics(spark)
#stagemetrics.begin()
Movies_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/movie.csv")
      )
Tags_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/tag.csv")
      )
Tags_Dataframe=Tags_Dataframe.withColumn('tag',lower(col('tag'))).withColumn('tag',regexp_replace('tag','[^\w\s]+', ''))
Joined_Dataframe=(Tags_Dataframe.join(Movies_Dataframe,Movies_Dataframe.movieId==Tags_Dataframe.movieId)
                  .select(Movies_Dataframe.title,Tags_Dataframe.userId,Tags_Dataframe.movieId,Tags_Dataframe.tag)
                  .filter(Tags_Dataframe.tag.contains("boring"))
                  .dropDuplicates(subset=['movieId'])
                 )
Joined_Dataframe.select("title").sort('title').show(truncate=0)
#stagemetrics.end()
#stagemetrics.print_report()
