# DBLP GraphDatabase

## Loading
1. Generate CSV file of the DBLP XML dump file using ThomHurks tools (https://github.com/ThomHurks/dblp-to-csv) and store it in the "dblp_dump/" directory in this project.  
 ```python XMLToCSV.py dblp.xml dblp.dtd output.csv```
2. Run a2_data_preparation_NugrohoSalmaan.py 
3. Copy the output in the "out/" directory to neo4j import directory, by default the directory path is /var/lib/neo4j/import/
Run a2_loading_NugrohoSalmaan.py
4. Run ```a2_loading_NugrohoSalmaan.py```

## Evolving
1. Run ```a3_prepare_evolving.py```
2. Copy the the generated file (```out/authors_with_affiliation.csv```) to neo4j import directory
3. Run ```a3_evolving_NugrohoSalmaan.py``` to start the evolving process

## Running Queries
Run ```b_queries_NugrohoSalmaan.py``` using -q parameter specifying query number. For example to run query 1 (the h-index query), run the following command:

```python b_queries_NugrohoSalmaan.py -q 1```

The fourth query require additional year parameter that can be specified using -y option. For example:

```python b_queries_NugrohoSalmaan.py -q 4 -y 1999```

## Running Graph Algorithm
Run ```c_graph_algorithm_NugrohoSalmaan``` using -a parameter specifying algorithm number. To run our first algorithm (article rank) use ```-a 1```, and to run our second algorithm (louvain community detection) use ```-a 2```. For example:

```python c_graph_algorithm_NugrohoSalmaan.py -a 1```

## Running Recommender
Run d_recommender_NugrohoSalmaan.py with the following parameters:
- -t (in a fraction) homogenity threshold for a conference to be considered as community
- -m minimum paper appears in top 100 to be considered as guru
- -k string representation of keywords, e.g:'["database", "sql"]' 

For example:
```
python d_recommender_NugrohoSalmaan.py -t 0.5 -m 1 -k '["database", "sql"]' 
````

In order to get meaningful result, we can use the following keyword list as ```keywords``` argument:
```
'["database","index","indexing","querying","data","sql","olap","b+-trees","dbms","dbmss","views","databases","queries","postgres","picodbms","xml","tables","oodbms","relational"]'
```

