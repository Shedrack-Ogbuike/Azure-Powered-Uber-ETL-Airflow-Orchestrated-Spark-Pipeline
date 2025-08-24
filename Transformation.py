from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, when
from pyspark.sql.types import FloatType, IntegerType
from Extraction import run_extraction


def run_transformation(df):
    spark = SparkSession.builder.appName("UberETL").getOrCreate()
   
    # Clean column names
    df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])

    # Fill missing values
    df_cleaned = df.fillna({
        "Avg_VTAT": 0,
        "Avg_CTAT": 0,
        "Cancelled_Rides_by_Customer": 0,
        "Reason_for_cancelling_by_Customer": "NA",
        "Cancelled_Rides_by_Driver": 0,
        "Driver_Cancellation_Reason": "NA",
        "Incomplete_Rides": 0,
        "Incomplete_Rides_Reason": "NA",
        "Booking_Value": 0,
        "Ride_Distance": 0,
        "Driver_Ratings": 0,
        "Customer_Rating": 0,
        "Payment_Method": "NA"
    })

    # Drop Date column if exists
    if "Date" in df_cleaned.columns:
        df_cleaned = df_cleaned.drop("Date")

    # Cast numeric columns
    numeric_cols = ["Avg_VTAT", "Avg_CTAT", "Booking_Value", "Ride_Distance", "Driver_Ratings", "Customer_Rating"]
    for c in numeric_cols:
        df_cleaned = df_cleaned.withColumn(c, when(col(c) == "null", None).otherwise(col(c).cast(FloatType())))

    
    # Cast flag columns
    flag_cols = ["Cancelled_Rides_by_Customer", "Cancelled_Rides_by_Driver", "Incomplete_Rides"]
    for c in flag_cols:
        df_cleaned = df_cleaned.withColumn(c, when(col(c) == "null", None).otherwise(col(c).cast(IntegerType())))

    
    # Customers table
    customers = df_cleaned.select("Customer_ID", "Customer_Rating").dropDuplicates() \
                          .withColumn("Customer_ID_New", monotonically_increasing_id()) \
                          .select(col("Customer_ID_New").alias("Customer_ID"), "Customer_Rating")

    # Drivers table
    drivers = df_cleaned.select("Driver_Ratings").dropDuplicates() \
                        .withColumn("Driver_ID", monotonically_increasing_id()) \
                        .select("Driver_ID", "Driver_Ratings")

    # Locations table
    pickup = df_cleaned.select("Pickup_Location").withColumnRenamed("Pickup_Location", "Location_Name")
    drop = df_cleaned.select("Drop_Location").withColumnRenamed("Drop_Location", "Location_Name")
    all_locations = pickup.union(drop).dropDuplicates()
    locations = all_locations.withColumn("Location_ID", monotonically_increasing_id()) \
                             .select("Location_ID", "Location_Name")

    # Bookings table
    bookings = df_cleaned.join(drivers, on="Driver_Ratings", how="left")
    bookings = bookings.join(
        locations.withColumnRenamed("Location_ID", "Pickup_Location_ID"),
        bookings["Pickup_Location"] == locations["Location_Name"],
        how="left"
    ).drop("Location_Name")
    bookings = bookings.join(
        locations.withColumnRenamed("Location_ID", "Drop_Location_ID"),
        bookings["Drop_Location"] == locations["Location_Name"],
        how="left"
    ).drop("Location_Name")

    bookings = bookings.select(
        "Booking_ID", "Customer_ID", "Driver_ID", "Booking_Status", "Vehicle_Type",
        "Pickup_Location_ID", "Drop_Location_ID", "Avg_VTAT", "Avg_CTAT",
        "Cancelled_Rides_by_Customer", "Reason_for_cancelling_by_Customer",
        "Cancelled_Rides_by_Driver", "Driver_Cancellation_Reason",
        "Incomplete_Rides", "Incomplete_Rides_Reason", "Booking_Value",
        "Ride_Distance", "Payment_Method"
    )

    return bookings, customers, drivers, locations


    
