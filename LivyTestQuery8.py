from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark=SparkSession \
        .builder \
        .master("spark://ubuntu:7077 ")\
        .appName("Top movies for every category")\
        .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

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
#Format the Rating Dataframe
Rdf=(Ratings_Dataframe.drop('timestamp')
                      .drop('userId')
                      .groupBy("movieId").count()
                      .withColumnRenamed('count', 'NumberOfRatings')
    )
#Format the Movie Dataframe
Mdf=(Movies_Dataframe.select("*", split(col("genres"), "\|"))
                     .drop('genres')
                     .withColumnRenamed('split(genres, \|, -1)','genresar')
    )
Moviedf=Mdf.select("*",explode(Mdf.genresar).alias("Genre")).drop("genresar").sort("Genre",ascending=False)

#Inner-Join on movieId
mrdf1=Moviedf.filter("Genre !='(no genres listed)'").join(Rdf,["movieId"]) 

#Partition by Genre and descending order by NumberOfRatings, 
windowDept = Window.partitionBy("Genre").orderBy(col("NumberOfRatings").desc())
FullQuery=(mrdf1.withColumn("row",row_number().over(windowDept))
           .filter(col("row") <= 1)
           .drop("row")
          )
FullQuery.orderBy("Genre",ascending=True).show(truncate=0)
