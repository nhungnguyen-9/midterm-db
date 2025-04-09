from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import TimestampType, FloatType, IntegerType
import sys

def main(student_id):
    spark = SparkSession.builder.appName("Process Transactions").getOrCreate()

    input_file = f"transactions_{student_id}.csv"
    clean_output = f"processed_data_{student_id}.csv"
    bad_output = f"bad_rows_{student_id}.csv"
    expected_customer_id = f"STD_{student_id}"

    # Read CSV
    df = spark.read.option("header", True).csv(input_file)

    # Cast types
    df_casted = df.withColumn("order_date", col("order_date").cast(TimestampType())) \
                  .withColumn("price", col("price").cast(FloatType())) \
                  .withColumn("quantity", col("quantity").cast(IntegerType())) \
                  .withColumn("discount", col("discount").cast(FloatType()))

    # Identify bad rows
    bad_rows = df_casted.filter(
        col("order_date").isNull() |
        col("price").isNull() |
        col("quantity").isNull() |
        col("discount").isNull() |
        (col("customer_id") != expected_customer_id)
    )

    # Good rows = total - bad
    good_rows = df_casted.subtract(bad_rows) \
                         .withColumn("total_amount", expr("quantity * price * (1 - discount)"))

    # Save to CSV
    good_rows.coalesce(1).write.mode("overwrite").option("header", True).csv(clean_output)
    bad_rows.coalesce(1).write.mode("overwrite").option("header", True).csv(bad_output)

    print(f"✅ Cleaned data saved to {clean_output}")
    print(f"❌ Bad rows saved to {bad_output}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: process_data.py <student_id>")
        sys.exit(1)
    main(sys.argv[1])