import sys
from pyspark import SparkContext

# Instanciate the spark context
sc = SparkContext()

# Load the the input files
# The RDD is the key/value list with key the filename and value the file content
files = sc.wholeTextFiles(sys.argv[1])

# Split the files by lines
lines = files.flatMap(lambda file: file[1].splitlines())

# Get the income values
income_list = lines.map(lambda line: int(line.split(' ')[1]))

# Compute the income on the 1 year data
total_income = income_list.reduce(lambda count1, count2: count1 + count2)

# Compute the monthly average over 1 year (12 months in a year)
average_monthly_income_on_1_year = total_income / 12

# Display the result
print("Average monthly income of the shop in France (on a 1 year data):", average_monthly_income_on_1_year)