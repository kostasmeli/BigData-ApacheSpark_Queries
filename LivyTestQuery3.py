from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("Bollywood over 3 rating")\
        .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
#stagemetrics = StageMetrics(spark)
#stagemetrics.begin()
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
Tags_Dataframe=Tags_Dataframe.withColumn('tag',lower(col('tag'))).withColumn('tag',regexp_replace('tag', '[^\w\s]+', ''))
Joined_Dataframe=(Tags_Dataframe.filter(Tags_Dataframe.tag=='bollywood') 
                                .join(Ratings_Dataframe,[(Ratings_Dataframe.userId==Tags_Dataframe.userId)& (Ratings_Dataframe.movieId==Tags_Dataframe.movieId)])
                                .where(Ratings_Dataframe.rating>3)
                                .dropDuplicates(['userId'])
                                .select(Tags_Dataframe.userId,Tags_Dataframe.movieId,Tags_Dataframe.tag,Ratings_Dataframe.rating)
                 )
Joined_Dataframe.sort("userId").show()
#stagemetrics.end()
#stagemetrics.print_report()
