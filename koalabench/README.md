# KoalaBench

A big data benchmark for decision support systems based on TPC-H

The Koala Big Data Bench is a benchmark data generator for testing decision support systems in the Big Data trend. It extends the TPC-H benchmark, well-known decision support benchmark. Some of the modifications are inspired from the SSB benchmark. The existing benchmarks are not compatible to NoSQL systems; they are conceived to work with relational databases. The new benchmark adapts to most database solutions (RDBMS and NoSQL).

## Data Models Supported

Data can be generated to follow different conceptual logical schemas for data warehousing: 
- snow flake logical model 
      - with schema as in [image](http://www.irit.fr/recherches/SIG/files/model_for_snow.pdf) 
      - with instances like the following [JSON](http://www.irit.fr/recherches/SIG/files/instance_snow.json )
- star logical model
      - with schema as in [image](http://www.irit.fr/recherches/SIG/files/model_for_star.pdf )
      - with instances like the following [JSON](http://www.irit.fr/recherches/SIG/files/instance_star.json )
- flat model 
      - with schema as in [image](http://www.irit.fr/recherches/SIG/files/model_for_flat.pdf )
      - with instances like the following [JSON](http://www.irit.fr/recherches/SIG/files/instance_flat.json )
- sparse vector 
      - with schema as in [image](http://www.irit.fr/recherches/SIG/files/model_for_sparse_order.pdf )
      - with instances like the following [JSON](http://www.irit.fr/recherches/SIG/files/instance_sparse.json )


## Usage

The below steps are for running the code in ```IntelliJ IDEA Community Edition```.
Other code editors can also be used.

- Click on ```Run```
- Click on ```Edit Configurations...```
- Choose the jdk version, preferably ```java 17```
- In the fully qualified name of the class ensure it is, ```new_bench.main.DBGen```
- In the CLI arguments, provide
```<file format> <data-model> sf<scale-factor>```
- For example, to generate a flat data model
```csv flat sf2```, to generate a snow data model ```csv snow sf3```
.

## Credits

This work is based on [KoalaBench](https://github.com/arlind29/KoalaBench) an open-source project, with minor bug fixes.