# Jupyter

## Detecting correlated columns in DBMS systems


On a database with a large number of relations, there can exist many correlated columns or groups of correlated columns. 
Finding out such columns can provide useful business analytics and insight into the data. This can also help in data 
lineage to track the origin of the data and subsequent transformations. Some of these relationships are pretty obvious 
like primary keys, foreign keys, other indexes, etc. But the main challenges are to find out the hidden relationships. 
We picked the Pearson correlation as a measurement of correlation between numerical columns (numerical, date, timestamp, 
boolean types). For non-numerical columns (string type), we used LSH with minhashing to find strongly related columns 
with high probability. As LSH with minhashing is a probabilistic approach, there can be false positives and negatives as 
well. Similarly for Pearson correlation calculation, we have used simple random sampling to reduce the overall compute 
cost. However our solution is expected to find strongly correlated columns with high probability. We tried to scale it 
up to run on relations with up to 10000 columns.
