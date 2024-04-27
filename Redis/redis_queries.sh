echo "Executing Benchmark Query 1"
start_time_query1=$(date +%s)
python3 benchmark_queries/query1.py
end_time_query1=$(date +%s)
echo "Time taken for Query 1: $((end_time_query1 - start_time_query1)) seconds"

echo "Executing Benchmark Query 2"
start_time_query2=$(date +%s)
python3 benchmark_queries/query2.py
end_time_query2=$(date +%s)
echo "Time taken for Query 2: $((end_time_query2 - start_time_query2)) seconds"

echo "Executing Benchmark Query 3"
start_time_query3=$(date +%s)
python3 benchmark_queries/query3.py
end_time_query3=$(date +%s)
echo "Time taken for Query 3: $((end_time_query3 - start_time_query3)) seconds"

echo "Executing Benchmark Query 4"
start_time_query4=$(date +%s)
python3 benchmark_queries/query4.py
end_time_query4=$(date +%s)
echo "Time taken for Query 4: $((end_time_query4 - start_time_query4)) seconds"

echo "Executing Benchmark Query 5"
start_time_query5=$(date +%s)
python3 benchmark_queries/query5.py
end_time_query5=$(date +%s)
echo "Time taken for Query 5: $((end_time_query5 - start_time_query5)) seconds"