from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import sys

def main(student_id):
    spark = SparkSession.builder.appName("CustomerClustering").getOrCreate()
    df = spark.read.csv(f"transactions_{student_id}.csv", header=True, inferSchema=True)

    customer_data = df.groupBy("customer_id").agg(
        {"transaction_id": "count", "price": "sum"}
    ).withColumnRenamed("count(transaction_id)", "total_orders") \
     .withColumnRenamed("sum(price)", "total_spent")

    assembler = VectorAssembler(inputCols=["total_orders", "total_spent"], outputCol="features")
    customer_features = assembler.transform(customer_data)

    kmeans = KMeans(k=3, seed=1, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(customer_features)
    clustered_customers = model.transform(customer_features)

    clustered_pd = clustered_customers.select("customer_id", "total_orders", "total_spent", "cluster").toPandas()

    plt.figure(figsize=(10, 6))
    sns.scatterplot(x="total_orders", y="total_spent", hue="cluster", data=clustered_pd, palette="viridis", s=100)
    plt.xlabel("Tổng số đơn hàng")
    plt.ylabel("Tổng chi tiêu")
    plt.title("Phân cụm khách hàng bằng KMeans")
    plt.legend(title="Cụm")
    plt.grid()

    output_file = f"customer_clusters_bonus_{student_id}.png"
    plt.savefig(output_file)
    print(f"Đã lưu biểu đồ tại {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: visualize_transactions.py <student_id>")
        sys.exit(1)
    main(sys.argv[1])