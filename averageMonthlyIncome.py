import sys
from pyspark import SparkContext

# Instanciate the spark context
sc = SparkContext()

# Load the the input files
# The RDD is the key/value list with key the filename and value the file content
files = sc.wholeTextFiles(sys.argv[1])

# Split the files by lines
lines = files.flatMap(lambda file: file[1].splitlines())

# We create a tuple for each line with key: the month, value: the revenue
months_with_revenues = lines.map(lambda line: (line.split(' ')[0], int(line.split(' ')[1])))

# We group the revenue per month
revenues_per_month = months_with_revenues.groupByKey()

average_income_per_month = revenues_per_month.map(lambda month_revenues: (month_revenues[0], sum(month_revenues[1])/len(month_revenues[1])))
# Compute the average income per month

result = average_income_per_month.collect()

for res in result:
  print(res)