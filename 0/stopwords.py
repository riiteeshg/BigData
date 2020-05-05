from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("StopWords example")\
         .getOrCreate()


    sentenceData = spark.createDataFrame([
        (0, ["I", "saw", "the", "red", "balloon"]),
        (1, ["Mary", "had", "a", "little", "lamb"])
], ["id", "raw"])

remover =stopwords(inputCol="raw", outputCol="filtered")
remover.transform(sentenceData).show(truncate=False)    
    