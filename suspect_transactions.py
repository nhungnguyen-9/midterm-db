from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, expr, percentile_approx
import sys

def main(student_id):
    spark = SparkSession.builder.appName("Detect Suspect Transactions").getOrCreate()

    input_file = f"cleaned_transactions_{student_id}.csv"
    output_file = f"suspect_transactions_{student_id}.csv"

    # Đọc dữ liệu đã xử lý
    df = spark.read.option("header", True).csv(input_file, inferSchema=True)

    # --- Cách 1: Dùng trung vị ---
    median = df.selectExpr("percentile_approx(total_amount, 0.5)").first()[0]
    threshold = 5 * median

    outliers = df.filter(col("total_amount") > threshold)

    outliers.write.mode("overwrite").option("header", True).csv(output_file)

    print(f"Giao dịch nghi vấn đã được lưu tại: {output_file}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: detect_outliers.py <student_id>")
        sys.exit(1)
    main(sys.argv[1])