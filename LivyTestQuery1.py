from pyspark.sql import SparkSession
#from sparkmeasure import StageMetrics
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077")\
        .appName("Jumanji Query")\
        .getOrCreate()
#stagemetrics = StageMetrics(spark)
#stagemetrics.begin()
Ratings_Dataframe=(spark.read.format("csv")
            .option("header","True")
            .option("inferSchema","True")
            .load("/home/administrator/Downloads/movielens/rating.csv")
      )
#Number of people who rated Jumanji
Ratings_Dataframe.where(Ratings_Dataframe.movieId==2).count()
#stagemetrics.end()
#stagemetrics.print_report()
