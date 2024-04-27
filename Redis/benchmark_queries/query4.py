import time
import redis
r = redis.Redis(host="0.0.0.0", port=6379, db=2)
start_time = time.time()
revenue_by_nation = {}
for key in r.scan_iter():
    data = r.hgetall(key)
    order_date = data.get(b"o_orderdate").decode("utf-8")
    if '1992-01-01' <= order_date < '1998-04-02':
        extended_price = float(data[b"l_extendedprice"])
        discount = float(data[b"l_discount"])
        revenue = extended_price * (1 - discount)
        nation_key = "nation:" + data[b"s_nationname"].decode("utf-8")
        revenue_by_nation[nation_key] = revenue_by_nation.get(nation_key, 0) + revenue
sorted_revenue = sorted(revenue_by_nation.items(), key=lambda x: x[1], reverse=True)
end_time = time.time()
for nation, revenue in sorted_revenue:
    print(f"Nation: {nation}, Revenue: {revenue}")
print(f"Time taken: {end_time - start_time} seconds")