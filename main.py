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
cityAsKey = f4.map(lambda scmr: (scmr[1], scmr[0], scmr[2], int(scmr[3])))

# persist the cityAsKey rdd, because this rdd is used by multiple queries
cityAsKey.cache()

## Query 1
# Get all income values
income_list = cityAsKey.map(lambda income_line: income_line[3])

# Compute the total income for the shop on 1 year
total_income = income_list.reduce(lambda income1, income2: income1 + income2)

# Compute the monthly average over 1 year (12 months in a year)
average_monthly_income_on_1_year = total_income / 12


## Query 2
# Prepare the data, by transforming the cityAsKey rdd as a tuple list with key: city, value: income value
income_list_with_city_as_key = cityAsKey.map(lambda income_line: (income_line[0], income_line[3]))

# Compute the revenue on 1 year for each city
total_revenue_per_city = income_list_with_city_as_key.reduceByKey(lambda income1, income2: income1 + income2)

# persist the total_revenue_per_city rdd cause we will use it again with the query 3
total_revenue_per_city.persist()


## Query 3
# Using the revenue on 1 year for each city result from the previous query,
# we can easily compute the average monthly income per city by dividing the previous results by 12
average_monthly_income_on_1_year_per_city = total_revenue_per_city.mapValues(lambda income_on_1_year: income_on_1_year/12)


## Query 4
# Prepare the data, by transforming the cityAsKey rdd as a tuple list with key: store, value: income value
income_list_with_store_as_key = cityAsKey.map(lambda income_line: (income_line[1], income_line[3]))

# Compute th revenue on 1 year for each store
total_revenue_per_store = income_list_with_store_as_key.reduceByKey(lambda income1, income2: income1 + income2)


## Query 5
# Prepare the data, by transforming the cityAsKey rdd as a tuple list with key: month, value: (store, income)
income_with_store_list_with_month_as_key = cityAsKey.map(lambda income_line: (income_line[2], (income_line[1], income_line[3])))

# Find the best income for each month
best_income_per_month = income_with_store_list_with_month_as_key.reduceByKey(lambda income_line1, income_line2: income_line1 if income_line1[1] > income_line2[1] else income_line2)

# Get the store name for each best income line of the month
best_performance_store_per_month = best_income_per_month.mapValues(lambda income_line: income_line[0])


# Query 6 (not required)
# This query displays the average income of the shop in France (on a 1 year data) per month
# Prepare the data, by transforming the cityAsKey rdd as a tuple list with key: month, value: income
income_list_with_month_as_key = cityAsKey.map(lambda income_line: (income_line[2], income_line[3]))

# Regroup the incomes per month
incomes_per_month = income_list_with_month_as_key.groupByKey()

# Compute the average income per month
average_income_per_month = incomes_per_month.mapValues(lambda incomes: sum(incomes)/len(incomes))


# Display the results
# Query 1:
print("\nAverage monthly income of the shop in France (on a 1 year data):")
print(average_monthly_income_on_1_year)

# Query 2:
print("\nTotal revenue per city per year:")
for city, income in total_revenue_per_city.collect():
  print(city, ": ", income)

# Query 3:
print("\nAverage monthly income of the shop in each city (on a 1 year data):")
for city, income in average_monthly_income_on_1_year_per_city.collect():
  print(city, ": ", income)

# Query 4:
print("\nTotal revenue per store per year:")
for store, income in total_revenue_per_store.collect():
  print(store, ": ", income)

# Query 5:
print("\nThe store that achieves the best performance in each month:")
for month, store in best_performance_store_per_month.collect():
  print(month, ": ", store)

# Query 6:
print("\nAverage income of the shop in France (on a 1 year data) per month:")
for month, average_income in average_income_per_month.collect():
  print(month, ": ", average_income)