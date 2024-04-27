#Adding Headers
# python3 Adding_headers.py

#Preprocess CSVs
python3 preprocess_customer.py
python3 preprocess_order.py
python3 preprocess_lineitem.py

#Start postgres container
docker pull postgres:latest
docker run --name postgres511 -e POSTGRES_PASSWORD=cs511 -d -p 5432:5432 --network redis_default postgres
docker exec -it postgres511 psql -U postgres -c "CREATE DATABASE cs511;"
docker compose -f cs511p1-compose.yaml cp postgresql-42.7.3.jar main:/postgresql-42.7.3.jar

#Copy CSV to main
docker compose -f cs511p1-compose.yaml cp Snow/snow_region.csv main:/snow_region.csv
docker compose -f cs511p1-compose.yaml cp Snow/snow_nation.csv main:/snow_nation.csv
docker compose -f cs511p1-compose.yaml cp Snow/snow_part.csv main:/snow_part.csv
docker compose -f cs511p1-compose.yaml cp preprocessed_snow_customer.csv main:/snow_customer.csv
docker compose -f cs511p1-compose.yaml cp Snow/snow_supplier.csv main:/snow_supplier.csv
docker compose -f cs511p1-compose.yaml cp preprocessed_snow_order.csv main:/snow_order.csv
docker compose -f cs511p1-compose.yaml cp Snow/snow_part_supplier.csv main:/snow_part_supplier.csv
docker compose -f cs511p1-compose.yaml cp preprocessed_snow_line_item.csv main:/snow_line_item.csv


#Copy Scripts to main
docker compose -f cs511p1-compose.yaml cp loadRegion.py main:/loadRegion.py
docker compose -f cs511p1-compose.yaml cp loadNation.py main:/loadNation.py
docker compose -f cs511p1-compose.yaml cp loadPart.py main:/loadPart.py
docker compose -f cs511p1-compose.yaml cp loadCustomer.py main:/loadCustomer.py
docker compose -f cs511p1-compose.yaml cp loadSupplier.py main:/loadSupplier.py
docker compose -f cs511p1-compose.yaml cp loadOrder.py main:/loadOrder.py
docker compose -f cs511p1-compose.yaml cp loadPartSupplier.py main:/loadPartSupplier.py
docker compose -f cs511p1-compose.yaml cp loadLineItem.py main:/loadLineItem.py


#Load CSVs to HDFS
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'mkdir /postgres && hdfs dfs -mkdir -p /postgres && hdfs dfs -chmod 777 /postgres'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_region.csv /postgres/snow_region.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_nation.csv /postgres/snow_nation.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_part.csv /postgres/snow_part.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_customer.csv /postgres/snow_customer.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_supplier.csv /postgres/snow_supplier.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_order.csv /postgres/snow_order.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_part_supplier.csv /postgres/snow_part_supplier.csv'
docker compose -f cs511p1-compose.yaml exec main bash -x -c 'hdfs dfs -put -f /snow_line_item.csv /postgres/snow_line_item.csv'