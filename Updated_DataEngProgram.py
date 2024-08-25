
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyodbc

# Spark Session Setup
spark = SparkSession.builder.appName("CountryVaccinations").getOrCreate()

# Database connection parameters
SERVER = 'SQL5097.site4now.net'
DATABASE = 'db_a96a3c_riyasatali'
USERNAME = 'db_a96a3c_riyasatali_admin'
PASSWORD = 'Berlin@12277'

# Database connection string using pyodbc (to keep the SQL Server connection functionality)
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Read the CSV file using Spark
csv_path = '/Users/riyasatali/Desktop/MyDocument/Subjects/Project_Data_Eng/PythonProgram/CountryVaccinations.csv'
vaccination_df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Show the DataFrame content
vaccination_df.show()

# Filter the necessary columns (similar to the previous logic in pandas)
selected_df = vaccination_df.select(col("location").alias("Country"), col("date"), col("vaccine"), col("total_vaccinations"))

# Display filtered DataFrame
selected_df.show()

# Database operations: Write DataFrame to the SQL Server database
# Write data to the Vaccination table (overwrite mode is used to replace existing data)
selected_df.write \
        .format("jdbc")     \
        .option("url", f"jdbc:sqlserver://{SERVER}:1433;databaseName={DATABASE};user={USERNAME};password={PASSWORD}")     \
        .option("dbtable", "Vaccination")     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")     \
        .mode("overwrite")     \
        .save()

# Verify if data has been written to the database
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()
cursor.execute("SELECT COUNT(*) FROM Vaccination")
row_count = cursor.fetchone()[0]

print(f"Row count in Vaccination table: {row_count}")
cursor.close()
cnxn.close()

# Stop the Spark session
spark.stop()
