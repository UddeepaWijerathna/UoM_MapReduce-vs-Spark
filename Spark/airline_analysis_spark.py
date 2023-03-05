import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time


def delay_flights_analysis(data_source, output_uri):
    
    with SparkSession.builder.appName("Delayed Flights Analysis").getOrCreate() as spark:
        # Load the delayed flights CSV data
        if data_source is not None:
            query_result =[]
            delay_flights_df = spark.read.option("header", "true").csv(data_source)
            # Create an in-memory DataFrame to query
            delay_flights_df.createOrReplaceTempView("delay_flights")

            start_time = time.time()
            # Create a DataFrame of year wise carrier delays
            year_wise_carrier_delays = spark.sql("""
                SELECT Year,
                avg((CarrierDelay/ArrDelay)*100) AS CarrierDelay
                from delay_flights
                GROUP BY Year
                ORDER BY Year""")
            year_wise_carrier_delays.show()
            elapsed_time = time.time() - start_time
            # Print the execution time
            print("Execution time: ", elapsed_time, "seconds")
            query_result.append(("year_wise_carrier_delays", elapsed_time))
           
            start_time = time.time()
            # Create a DataFrame of year wise NAS delays
            year_wise_nas_delays = spark.sql("""
                SELECT Year,
                avg((NASDelay/ArrDelay)*100) AS NASDelay
                from delay_flights
                GROUP BY Year
                ORDER BY Year""")
            
            year_wise_nas_delays.show()
            elapsed_time = time.time() - start_time
            # Print the execution time
            print("Execution time: ", elapsed_time, "seconds")
            query_result.append(("year_wise_nas_delays", elapsed_time))
                
            start_time = time.time()
            # Create a DataFrame of year wise Weather delay delays
            year_wise_weather_delays = spark.sql("""
                SELECT Year,
                avg((WeatherDelay/ArrDelay)*100) AS WeatherDelay
                from delay_flights
                GROUP BY Year
                ORDER BY Year""")
            year_wise_weather_delays.show()
            elapsed_time = time.time() - start_time
            # Print the execution time
            print("Execution time: ", elapsed_time, "seconds")
            query_result.append(("year_wise_weather_delays", elapsed_time))
                
            start_time = time.time()
            # Create a DataFrame of year wise late aircraft delay delays
            year_wise_late_aircraft_delays = spark.sql("""
                SELECT Year,
                avg((LateAircraftDelay/ArrDelay)*100) AS LateAircraftDelay
                from delay_flights
                GROUP BY Year
                ORDER BY Year""")
           
            year_wise_late_aircraft_delays.show()
            elapsed_time = time.time() - start_time
            # Print the execution time
            print("Execution time: ", elapsed_time, "seconds")
            query_result.append(("year_wise_late_aircraft_delays", elapsed_time))
                
            start_time = time.time()
            # Create a DataFrame of year wise security delay delays
            year_wise_security_delays = spark.sql("""
                SELECT Year,
                avg((SecurityDelay/ArrDelay)*100) AS SecurityDelay
                from delay_flights
                GROUP BY Year
                ORDER BY Year""")
        
            year_wise_security_delays.show()
            elapsed_time = time.time() - start_time
            # Print the execution time
            print("Execution time: ", elapsed_time, "seconds")
            query_result.append(("year_wise_security_delays", elapsed_time))
                
            # Write the results to the specified output URI
            year_wise_carrier_delays = year_wise_carrier_delays.coalesce(1)
            year_wise_carrier_delays.write.mode("overwrite").option("header", "true").csv(output_uri + "/year_wise_carrier_delays_spark")
            
            year_wise_nas_delays = year_wise_nas_delays.coalesce(1)
            year_wise_nas_delays.write.mode("overwrite").option("header", "true").csv(output_uri + "/year_wise_nas_delays_spark")
            
            year_wise_weather_delays = year_wise_weather_delays.coalesce(1)
            year_wise_weather_delays.write.mode("overwrite").option("header", "true").csv(output_uri + "/year_wise_weather_delays_spark")
            
            year_wise_late_aircraft_delays = year_wise_late_aircraft_delays.coalesce(1)
            year_wise_late_aircraft_delays.write.mode("overwrite").option("header", "true").csv(output_uri + "/year_wise_late_aircraft_delays_spark")
            
            year_wise_security_delays = year_wise_security_delays.coalesce(1)
            year_wise_security_delays.write.mode("overwrite").option("header", "true").csv(output_uri + "/year_wise_security_delays_spark")

            query_results_df = spark.createDataFrame(query_result, ["QueryName", "ExecutionTime"])
            query_results_df = query_results_df.coalesce(1)
            query_results_df.write.mode("overwrite").option("header", "true").csv(output_uri + "/query_execution_time")

            
            # Stop the SparkSession
            spark.stop()




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV delayed flights data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    delay_flights_analysis(args.data_source, args.output_uri)
            
