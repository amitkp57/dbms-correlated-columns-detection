# Jupyter

## Detecting correlated columns in DBMS systems


On a large number of relations, there can be many correlated columns or groups of correlated columns. Finding out such columns can provide useful business analytics and insight into the data. This can also help in data lineage to track the origin of the data and subsequent transformations. Some of these relationships are pretty obvious, like primary keys, foreign keys, other indexes, etc. But the main challenges are to find out the hidden relationships, especially when data types of columns are different. For example, zip code and street address columns are correlated even though they are of different data types. 


We propose Pearson correlation with simple random sampling on numeric, date, datetime, time, boolean type columns. We also used MinHash with LSH clustering method to group non-numeric columns with a high probability of correlation. These correlation values will be indexed to enable queries. We test our method against a naive Pearson correlation matrix on an open dataset to demonstrate its correctness and speedup.