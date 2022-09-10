from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("Number of Movies per category which are funny and over 3.5 rating")\
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
Tags_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/tag.csv")
      )
Tags_Dataframe=Tags_Dataframe.withColumn('tag',lower(col('tag'))).withColumn('tag',regexp_replace('tag','[^\w\s]+', ''))
#Format the Movie Dataframe
Mdf=(Movies_Dataframe.select("*", split(col("genres"), "\|"))
                     .drop('genres')
                     .withColumnRenamed('split(genres, \\|)','genresar')
    )
Moviedf=Mdf.select("*",explode(Mdf.genresar).alias("Genre")).drop("genresar").sort("Genre",ascending=False)

Ratings_Dataframe=Ratings_Dataframe.filter("rating>3.5").drop('timestamp')
Tags_Dataframe=Tags_Dataframe.filter("tag='funny'").drop('timestamp')
rtdf=Ratings_Dataframe.join(Tags_Dataframe,['movieId','userId'])
rtmdf=rtdf.join(Moviedf,['movieId'])
FullQuery=rtmdf.groupBy("Genre").agg(count('movieId').alias("NumberOfMovies"))
FullQuery.sort('Genre').show()
#stagemetrics.end()
#stagemetrics.print_report()
