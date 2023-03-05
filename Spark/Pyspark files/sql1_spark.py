import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg


if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Usage: spark-etl [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("SparkETL")\
        .getOrCreate()


    delay_flights = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
    starttime=time.perf_counter()
    selected_data = delay_flights.groupBy('Year').agg(avg((col('CarrierDelay') / col('ArrDelay')) * 100).alias('avg_delay'))
    endtime=time.perf_counter()
    selected_data.show()
    timetaken=endtime-starttime
    print(f"Elapsed {timetaken:.03f} secs")
    selected_data.write.format("csv").option("header", "true").save(sys.argv[2])