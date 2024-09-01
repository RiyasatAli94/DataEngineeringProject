import pyodbc
from pyspark.sql import SparkSession 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, sum as spark_sum, to_date

# Initialize Spark Session
spark = SparkSession.builder.appName("VaccinationDataProcessing").getOrCreate()

# Database connection parameters
SERVER = 'SQL5097.site4now.net'
DATABASE = 'db_a96a3c_riyasatali'
USERNAME = 'db_a96a3c_riyasatali_admin'
PASSWORD = 'Berlin@12277'

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Load CSV data into PySpark DataFrame
csv_path = 'CountryVaccinations.csv'
vaccination_df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Show the data
vaccination_df.show()

# Insert data into SQL Server if not already present
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()
cursor.execute("select count(*) from Vaccination")
row_count = cursor.fetchone()[0]

if row_count == 0:
    vaccination_pd_df = vaccination_df.toPandas()  # Convert to pandas for insertion
    for index, row in vaccination_pd_df.iterrows():
        cursor.execute("INSERT INTO Vaccination (Country, date, vaccine, total) VALUES (?, ?, ?, ?)",
                       row['location'], row['date'], row['vaccine'], row['total_vaccinations'])
        print("Run")
    cnxn.commit()

# Load data from SQL Server into PySpark DataFrame
query = "(SELECT * FROM Vaccination) AS VaccinationData"
vaccination_sql_df = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{SERVER};databaseName={DATABASE}",
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
    dbtable=query,
    user=USERNAME,
    password=PASSWORD
).load()

# Convert 'date' column to date type
vaccination_sql_df = vaccination_sql_df.withColumn("date", to_date(col("date")))

# Fill or drop missing values if necessary
vaccination_sql_df = vaccination_sql_df.na.drop()

# Total vaccinations by country
total_vaccinations_by_country = vaccination_sql_df.groupBy("Country").agg(spark_sum("total").alias("total_vaccinations"))

# Total vaccinations by vaccine type
total_vaccinations_by_vaccine = vaccination_sql_df.groupBy("vaccine").agg(spark_sum("total").alias("total_vaccinations"))

# Total vaccinations over time
total_vaccinations_over_time = vaccination_sql_df.groupBy("date").agg(spark_sum("total").alias("total_vaccinations"))

# Convert to pandas DataFrame for visualization
total_vaccinations_by_country_pd = total_vaccinations_by_country.toPandas()
total_vaccinations_by_vaccine_pd = total_vaccinations_by_vaccine.toPandas()
total_vaccinations_over_time_pd = total_vaccinations_over_time.toPandas()

# Plot Total vaccinations by country
plt.figure(figsize=(12, 8))
sns.barplot(x='total_vaccinations', y='Country', data=total_vaccinations_by_country_pd.sort_values('total_vaccinations', ascending=False).head(10))
plt.title('Top 10 Countries by Total Vaccinations')
plt.xlabel('Total Vaccinations')
plt.ylabel('Country')
plt.show()

# Plot Total vaccinations by vaccine type
plt.figure(figsize=(12, 8))
sns.barplot(x='total_vaccinations', y='vaccine', data=total_vaccinations_by_vaccine_pd.sort_values('total_vaccinations', ascending=False))
plt.title('Total Vaccinations by Vaccine Type')
plt.xlabel('Total Vaccinations')
plt.ylabel('Vaccine')
plt.show()

# Plot Total vaccinations over time
plt.figure(figsize=(12, 8))
sns.lineplot(x='date', y='total_vaccinations', data=total_vaccinations_over_time_pd)
plt.title('Total Vaccinations Over Time')
plt.xlabel('Date')
plt.ylabel('Total Vaccinations')
plt.show()

# Filter data for selected countries
selected_countries = ['Germany', 'United States', 'Japan', 'Italy']  # Replace with actual country names
filtered_data = vaccination_sql_df.filter(col("Country").isin(selected_countries))

# Convert filtered data to pandas DataFrame for visualization
filtered_data_pd = filtered_data.toPandas()

# Plot vaccination trends for selected countries
plt.figure(figsize=(12, 8))
sns.lineplot(x='date', y='total', hue='Country', data=filtered_data_pd)
plt.title('Vaccination Trends for Selected Countries')
plt.xlabel('Date')
plt.ylabel('Total Vaccinations')
plt.legend(title='Country')
plt.show()

# Stop the Spark session
spark.stop()
