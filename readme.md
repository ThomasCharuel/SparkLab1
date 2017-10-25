Exercise: Shop Revenue Analysis

The revenues of a souvenir shop that is present in different cities in France is given in text files.
Each file contains the monthly income of a shop's branch in one year (each line contains a month and the corresponding revenue).

Each store (shop's branch) is identified by a name as follows:
- city_i; i=1, 2, ... Depending on the number of stores in the city
- If only one store is present in a city X, it is identified by the name of the city

The goal is to write a small Spark script that allows to display different statistics on the shop's performance:
- Average monthly income of the shop in France (on a 1 year data)
- Total revenue per city per year
- Average monthly income of the shop in each city (on a 1 year data)
- Total revenue per store per year
- THe store that achiveves the best performance in each month