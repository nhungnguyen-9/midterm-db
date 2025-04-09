import csv
import random
import sys
from datetime import datetime, timedelta

def generate_transactions(student_id, total=1_000_000, error_rate=0.1):
    filename = f"transactions_{student_id}.csv"
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['transaction_id', 'customer_id', 'order_date', 'price', 'quantity', 'discount'])

        for i in range(1, total + 1):
            # Random order date within the past year
            order_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S')
            price = round(random.uniform(10, 500), 2)
            quantity = random.randint(1, 10)
            discount = round(random.uniform(0, 0.5), 2)

            # 10% dòng lỗi
            if random.random() < error_rate:
                customer_id = f"INVALID_{random.randint(1000, 9999)}"
            else:
                customer_id = f"STD_{student_id}"

            writer.writerow([f"TX{i:07d}", customer_id, order_date, price, quantity, discount])

    print(f"File '{filename}' created with {total} records.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_data.py <student_id>")
        sys.exit(1)
    
    student_id = sys.argv[1]
    generate_transactions(student_id)