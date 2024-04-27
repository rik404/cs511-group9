import redis
import time
r = redis.Redis(host="0.0.0.0", port=6379, db=2)
order_counts = {}
start_time = time.time()
all_keys = r.scan_iter()
for key in all_keys:
    order_data = r.hgetall(key)
    order_date = order_data.get(b"o_orderdate").decode("utf-8")
    commit_date = order_data.get(b"l_commitdate").decode("utf-8")
    receipt_date = order_data.get(b"l_receiptdate").decode("utf-8")
    order_priority = order_data.get(b"o_orderpriority").decode("utf-8")
    if (
        "1990-01-01" <= order_date < "1996-05-04"
        and commit_date < receipt_date
    ):
        order_counts[order_priority] = order_counts.get(order_priority, 0) + 1
sorted_counts = sorted(order_counts.items())
end_time = time.time()
for order_priority, count in sorted_counts:
    print(f"Order Priority: {order_priority}, Order Count: {count}")
print(f"Execution time: {end_time - start_time} seconds")