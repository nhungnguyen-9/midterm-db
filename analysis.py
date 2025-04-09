from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, weekofyear, year, month, sum as _sum, countDistinct, count
from pyspark.sql.types import TimestampType, FloatType, IntegerType
import sys

def main(student_id):
    spark = SparkSession.builder.appName("Analyze Transactions").getOrCreate()

    input_file = f"cleaned_transactions_{student_id}.csv"

    # Read cleaned data
    df = spark.read.option("header", True).csv(input_file, inferSchema=True)

    # Cast types (in case schema not inferred correctly)
    df = df.withColumn("order_date", col("order_date").cast(TimestampType())) \
           .withColumn("price", col("price").cast(FloatType())) \
           .withColumn("quantity", col("quantity").cast(IntegerType())) \
           .withColumn("discount", col("discount").cast(FloatType())) \
           .withColumn("total_amount", col("total_amount").cast(FloatType()))

    # 1. Tổng chi tiêu mỗi tuần theo customer_id
    weekly_spending = df.withColumn("week", weekofyear("order_date")) \
                        .withColumn("year", year("order_date")) \
                        .groupBy("customer_id", "year", "week") \
                        .agg(_sum("total_amount").alias("weekly_spending"))
    weekly_spending.write.mode("overwrite").option("header", True).csv(f"weekly_spending_{student_id}.csv")

    # 2. Phân cụm hành vi mua hàng theo customer_id
    customer_behavior = df.groupBy("customer_id").agg(
        count("transaction_id").alias("total_orders"),
        _sum("total_amount").alias("total_spending"),
        countDistinct("price").alias("distinct_products")
    )
    customer_behavior.write.mode("overwrite").option("header", True).csv(f"customer_behavior_{student_id}.csv")

    # 3. Truy vấn Spark SQL - giảm đơn 3 tháng gần nhất
    monthly_orders = df.withColumn("year", year("order_date")) \
                       .withColumn("month", month("order_date")) \
                       .groupBy("customer_id", "year", "month") \
                       .agg(count("*").alias("order_count"))

    monthly_orders.createOrReplaceTempView("monthly_orders")

    query = """
    SELECT mo1.customer_id
    FROM monthly_orders mo1
    JOIN monthly_orders mo2 ON mo1.customer_id = mo2.customer_id AND mo2.year = mo1.year AND mo2.month = mo1.month - 1
    JOIN monthly_orders mo3 ON mo1.customer_id = mo3.customer_id AND mo3.year = mo1.year AND mo3.month = mo1.month - 2
    WHERE mo3.order_count > mo2.order_count AND mo2.order_count > mo1.order_count
    """

    declining_customers = spark.sql(query)
    declining_customers.write.mode("overwrite").option("header", True).csv(f"declining_customers_{student_id}.csv")

    print("✅ Phân tích hoàn tất!")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: analyze_transactions.py <student_id>")
        sys.exit(1)
    main(sys.argv[1])