import sys
from pyspark import SparkContext

# Instanciate the spark context
sc = SparkContext()

# Load the the input files
# The RDD is the key/value list with key the filename and value the file content
files = sc.wholeTextFiles(sys.argv[1])

# Generate an object for each line on each file like ('store_name', 'city', 'month', 'income')
f1 = files.map(lambda file: (file[0].split("/")[-1], file[1]))
f2 = f1.map(lambda file: (file[0].split(".txt")[0], file[1]))
f3 = f2.flatMapValues(lambda v: v.split("\r\n"))
f4 = f3.map(lambda kv: (kv[0], kv[0].split("_")[0], kv[1].split(" ")[0], kv[1].split(" ")[1]))
cityAsKey = f4.map(lambda scmr: (scmr[1], scmr[0], scmr[2], scmr[3]))

a = cityAsKey.map(lambda income_line: (income_line[2], (income_line[1], int(income_line[3])))) \
  .reduceByKey(lambda a, b: a if a[1] > b[1] else b) \
  .mapValues(lambda a: a[0])

# Display result
print("The store that achieves the best performance in each month")

result = a.collect()

for res in result:
  print(res)