rm -f TerraSort_Cap.csv
rm -f TerraSort_Cap_PreSorted.csv
python3 terrasort-helper.py
docker compose -f cs511p1-compose.yaml cp TerraSort_Cap.csv main:/TerraSort_Cap.csv
docker compose -f cs511p1-compose.yaml cp CsvFilterAndSort.py main:/CsvFilterAndSort.py
docker compose -f cs511p1-compose.yaml cp TerraSort_Cap_PreSorted.csv main:/TerraSort_Cap_PreSorted.csv
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'mkdir Sorting_Output && hdfs dfs -mkdir -p /Sorting && hdfs dfs -chmod 777 /Sorting && hdfs dfs -put -f /TerraSort_Cap.csv /Sorting/TerraSort_Cap.csv && hdfs dfs -put -f /TerraSort_Cap_PreSorted.csv /Sorting/TerraSort_Cap_PreSorted.csv'
docker compose -f cs511p1-compose.yaml cp test_grading.sh  main:/test_grading.sh
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'bash test_grading.sh'