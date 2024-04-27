import redis
from datetime import datetime, timedelta
import time
r = redis.Redis(host="0.0.0.0", port=6379, db=2)
group_stats = {}
start_time = time.time()
all_keys = r.scan_iter()
for key in all_keys:
    line_item_data = r.hgetall(key)
    ship_date = line_item_data.get(b"l_shipdate").decode("utf-8")
    return_flag = line_item_data.get(b"l_returnflag").decode("utf-8")
    extended_price = float(line_item_data.get(b"l_extendedprice"))
    discount = float(line_item_data.get(b"l_discount"))
    tax = float(line_item_data.get(b"l_tax"))
    quantity = int(line_item_data.get(b"l_quantity"))
    ship_date = datetime.strptime(ship_date, "%Y-%m-%d")
    cutoff_date = datetime(1998, 12, 1) - timedelta(days=90)
    if ship_date <= cutoff_date:
        if return_flag not in group_stats:
            group_stats[return_flag] = {
                "sum_qty": 0,
                "sum_base_price": 0,
                "sum_disc_price": 0,
                "sum_charge": 0,
                "avg_qty": 0,
                "avg_price": 0,
                "avg_disc": 0,
                "count_order": 0,
            }
        group_stats[return_flag]["sum_qty"] += quantity
        group_stats[return_flag]["sum_base_price"] += extended_price
        group_stats[return_flag]["sum_disc_price"] += extended_price * (1 - discount)
        group_stats[return_flag]["sum_charge"] += extended_price * (1 - discount) * (1 + tax)
        group_stats[return_flag]["avg_qty"] += quantity
        group_stats[return_flag]["avg_price"] += extended_price
        group_stats[return_flag]["avg_disc"] += discount
        group_stats[return_flag]["count_order"] += 1
end_time = time.time()
for group_key, stats in sorted(group_stats.items()):
    print(f"Return Flag: {group_key}")
    print(f"Sum Quantity: {stats['sum_qty']:.2f}, Sum Base Price: {stats['sum_base_price']:.2f}, Sum Disc Price: {stats['sum_disc_price']:.2f}, Sum Charge: {stats['sum_charge']:.2f}")
    print(f"Avg Quantity: {stats['avg_qty'] / stats['count_order']:.2f}, Avg Price: {stats['avg_price'] / stats['count_order']:.2f}, Avg Disc: {stats['avg_disc'] / stats['count_order']:.2f}, Count Order: {stats['count_order']}")
    print("=" * 10)  # Print a line of 10 '='
    print()
print(f"Execution time: {end_time - start_time:.2f} seconds")