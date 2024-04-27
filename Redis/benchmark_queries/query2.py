import redis
import time
from collections import defaultdict
r = redis.Redis(host="0.0.0.0", port=6379, db=2)
orders = defaultdict(list)
start_time = time.time()
all_keys = r.scan_iter()
for key in all_keys:
    order_data = r.hgetall(key)
    order_date = order_data.get(b"o_orderdate").decode("utf-8")
    ship_date = order_data.get(b"l_shipdate").decode("utf-8")
    order_key = order_data.get(b"o_orderkey").decode("utf-8")
    ship_priority = order_data.get(b"o_shippriority").decode("utf-8")
    extended_price = float(order_data.get(b"l_extendedprice"))
    discount = float(order_data.get(b"l_discount"))
    if (
        "1990-08-29" < ship_date
        and order_date < "2000-08-29"
    ):
        revenue = extended_price * (1 - discount)
        orders[order_key].append({
            "revenue": revenue,
            "order_date": order_date,
            "ship_priority": ship_priority
        })
sorted_orders = sorted(
    orders.items(),
    key=lambda x: sum(item["revenue"] for item in x[1]),
    reverse=True
)
end_time = time.time()
for order_key, order_data in sorted_orders:
    total_revenue = sum(item["revenue"] for item in order_data)
    order_date = order_data[0]["order_date"]
    ship_priority = order_data[0]["ship_priority"]
    print(f"Order Key: {order_key}, Total Revenue: {total_revenue}, Order Date: {order_date}, Ship Priority: {ship_priority}")
print(f"Execution time: {end_time - start_time} seconds")