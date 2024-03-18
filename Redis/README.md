# CS 511: Project 1 HDFS/Spark

Please refer to the instructions at [https://ddkang.github.io/teaching/2024-spring/p1](https://ddkang.github.io/teaching/2024-spring/p1).

To get started, spin up the cluster.
```bash
bash start-all.sh
```

Test HDFS deployment.
```bash
bash test_hdfs.sh
```

Test Spark deployment.
```bash
bash test_spark.sh
```
#### TerraSort Sorting Implementation

- To execute the sorting and checking the grading once, you can run `testsetup_docker.sh`.
- To run the sorting funtion multiple times and try sorting different CSVs in every iteration and grade every iteration, you can run `test_bulk.sh`.
- The script `terrasort-helper.py` generates the CSV in the required format and it is saved as `TerraSort_Cap.csv`.
- The script also performs the required sorting operations on the generated CSV and saves it as `TerraSort_Cap_PreSorted.csv`.
- The presorted CSV is considered as ground truth and used to verify the CSV after sorting by Spark.