import sys
from pyspark import SparkContext

# Instanciate the spark context
sc = SparkContext()

# Load the the input files
# The RDD is the key/value list with key the filename and value the file content
files = sc.wholeTextFiles(sys.argv[1])

# Split the files by lines
cities_with_lines = files.map(lambda file: file[1].splitlines())

print("Total revenue per city per year")