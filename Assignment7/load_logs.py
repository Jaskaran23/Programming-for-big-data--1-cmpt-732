from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
cluster = Cluster(['199.60.17.188', '199.60.17.216'])
import re
import sys
import os
import gzip
import uuid 
import datetime

inputs = sys.argv[1]
keyspace = sys.argv[2]
table_name = sys.argv[3]

session = cluster.connect(keyspace)

batch = BatchStatement(consistency_level = ConsistencyLevel.ONE)
insert_query = session.prepare ('Insert into '+ table_name +' (host,id,bytes,datetime,path) VALUES (?,?,?,?,?)')
counter = 0
batch_size = 200
for f in os.listdir(inputs):
    with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
        for line in logfile:
        	line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
        	val = re.search(line_re,line)
        	if val:
        		y = line_re.split(line)
        		y[2] = datetime.datetime.strptime(y[2],"%d/%b/%Y:%H:%M:%S")
        		y[4] = int(y[4])
        		#print(y[1],y[2],y[3],y[4])
        		batch.add(insert_query,[y[1],uuid.uuid4(),y[4],y[2],y[3]])
        		counter = counter + 1
        		#insert_query.consistency_level = ConsistencyLevel.ONE
        		
        		#for i in range (batch_size):
        		if(counter > batch_size):
        			session.execute(batch)
        			batch.clear()
        			counter = 0
session.execute(batch)
        		#session.execute(insert_query,[y[1],uuid.uuid4(),y[4],y[2],y[3]])
           
