import pyodbc
from pyspark.sql import SparkSession 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns



SERVER = 'SQL5097.site4now.net'
DATABASE = 'db_a96a3c_riyasatali'
USERNAME = 'db_a96a3c_riyasatali_admin'
PASSWORD = 'Berlin@12277'

connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

Path = 'CountryVaccinations.csv'
datfile = pd.read_csv(Path)
print(datfile)
Country = datfile['location']
date =  datfile['date']
vaccine = datfile['vaccine']
total = datfile['total_vaccinations']
cnxn=pyodbc.connect(connectionString)
cursor=cnxn.cursor()
cursor.execute("select count(*) from Vaccination")
row_count = cursor.fetchone()[0]

if row_count == 0:
    for number in range(len(Country)):
        CountryStr = Country[number]
        dateStr = date[number] 
        vaccineStr = vaccine[number]
        totalStr = total[number]
        cursor.execute("INSERT INTO Vaccination(Country,date,vaccine,total) VALUES (?,?,?,?)",(CountryStr,dateStr,vaccineStr,number))
        print("Run")
    cnxn.commit()



query = "SELECT * FROM Vaccination"

# Execute the query
cursor.execute(query)

# Fetch all results
rows = cursor.fetchall()

# Get column names from cursor description
columns = [columns[0] for columns in cursor.description]

# Convert the fetched data to a pandas DataFrame
df = pd.DataFrame.from_records(rows, columns=columns)

print(rows)

csv_file_path = 'Database.csv'
df.to_csv(csv_file_path, index=False)

#load the data from csv file
file_path = 'Database.csv'
data_CSV = pd.read_csv(file_path)
# Convert date column to datetime
data_CSV['date'] = pd.to_datetime(data_CSV['date'])

# Fill or drop missing values if necessary
data_CSV = data_CSV.dropna()

# Ensure data types are correc
print(data_CSV.dtypes)

# Total vaccinations by country
total_vaccinations_by_country = data_CSV.groupby('Country')['total'].sum().reset_index()

# Total vaccinations by vaccine type
total_vaccinations_by_vaccine = data_CSV.groupby('vaccine')['total'].sum().reset_index()

# Total vaccinations over time
total_vaccinations_over_time = data_CSV.groupby('date')['total'].sum().reset_index()

print(total_vaccinations_by_country.head())
print(total_vaccinations_by_vaccine.head())
print(total_vaccinations_over_time.head())

# Total vaccinations by country
plt.figure(figsize=(12, 8))
sns.barplot(x='total', y='Country', data=total_vaccinations_by_country.sort_values('total', ascending=False).head(10))
plt.title('Top 10 Countries by Total Vaccinations')
plt.xlabel('Total Vaccinations')
plt.ylabel('Country')
plt.show()

# Total vaccinations by vaccine type
plt.figure(figsize=(12, 8))
sns.barplot(x='total', y='vaccine', data=total_vaccinations_by_vaccine.sort_values('total', ascending=False))
plt.title('Total Vaccinations by Vaccine Type')
plt.xlabel('Total Vaccinations')
plt.ylabel('Vaccine')
plt.show()

# Total vaccinations over time
plt.figure(figsize=(12, 8))
sns.lineplot(x='date', y='total', data=total_vaccinations_over_time)
plt.title('Total Vaccinations Over Time')
plt.xlabel('Date')
plt.ylabel('Total Vaccinations')
plt.show()

# Filter data for selected countries
selected_countries = ['Germany', 'United States','Japan','Italy']  # Replace with actual country names
filtered_data = data_CSV[data_CSV['Country'].isin(selected_countries)]

# Plot vaccination trends for selected countries
plt.figure(figsize=(12, 8))
sns.lineplot(x='date', y='total', hue='Country', data=filtered_data)
plt.title('Vaccination Trends for Selected Countries')
plt.xlabel('Date')
plt.ylabel('Total Vaccinations')
plt.legend(title='Country')
plt.show()