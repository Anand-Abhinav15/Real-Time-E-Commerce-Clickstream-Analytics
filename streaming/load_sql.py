import os
import io
import pandas as pd

from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from sqlalchemy import create_engine, text


#Config

load_dotenv()

STORAGE_ACCOUNT = "stclickstreamaa01"
STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")

SQL_SERVER = "sql-clickstream-dev-db.database.windows.net"
SQL_DATABASE = "sqldb-clickstream"
SQL_USERNAME = "clickstreamadmin"
SQL_PASSWORD = os.getenv("SQL_PASSWORD")


#Validation

if not STORAGE_KEY:
    raise ValueError(
        "AZURE_STORAGE_KEY is missing from .env"
    )

if not SQL_PASSWORD:
    raise ValueError(
        "SQL_PASSWORD is missing from .env"
    )


#ADLS Client

service_client = DataLakeServiceClient(
    account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=STORAGE_KEY
)

file_system = service_client.get_file_system_client("gold")


#Read Parquet Files From ADLS

def read_parquet_folder(folder_path):
    """ 
    Reads all Parquet files from an ADLS Gen2 folder
    and combines them into a single Pandas DataFrame.
    """

    print(f"\nReading: {folder_path}")

    paths = file_system.get_paths(path=folder_path)

    dataframes = []

    for path in paths:

        if path.is_directory:
            continue

        if not path.name.endswith(".parquet"):
            continue 

        print(f"  -> {path.name}")

        file_client = file_system.get_file_client(path.name)

        data = file_client.download_file().readall()

        df = pd.read_parquet(io.BytesIO(data), engine = "pyarrow")

        dataframes.append(df)

    
    if not dataframes:
        raise RuntimeError(
            f"No Parquet files found in {folder_path}"
        )

    return pd.concat(
        dataframes, ignore_index = True
    )


#Read Product Trends

print("\n===========================")
print("Reading Product Trends")
print("\n===========================")

product_trends = read_parquet_folder(
    "powerbi/product_trends"
)

print(
    f"\nProduct Trends records: "
    f"{len(product_trends)}"
)


#Read Daily KPIs

print("\n===========================")
print("Reading Daily KPIs")
print("\n===========================")

daily_kpis = read_parquet_folder(
    "powerbi/daily_kpis"
)

print(
    f"\nDaily KPI records: "
    f"{len(daily_kpis)}"
)


#SQL Connection

connection_string = (
    "mssql+pyodbc://"
    f"{SQL_USERNAME}:{SQL_PASSWORD}"
    f"@{SQL_SERVER}:1433/"
    f"{SQL_DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
)

engine = create_engine(
    connection_string,
    fast_executemany=True  
)


#Test Connection

print("\n==================================")
print("Testing SQL Connection")
print("\n==================================")

with engine.connect() as connection:

    result = connection.execute(
        text("SELECT 1")
    )

    print(
        f"SQL connection successful: "
        f"{result.scaler()}"
    )


#Load Product Trends

print("\n========================================")
print("LOADING PRODUCT TRENDS")
print("========================================")


product_trends.to_sql(
    "ProductTrends",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=1000 
)

print(
    f"Loaded {len(product_trends)}"
    f"ProductTrends records"
)


#Load Daily KPIs

print("\n========================================")
print("LOADING DAILY KPIs")
print("========================================")


daily_kpis.to_sql(
    "DailyKPIs",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=1000 
)

print(
    f"Loaded {len(daily_kpis)}"
    f"DailyKPIs records"
)


#Verify

print("\n========================================")
print("VERIFYING SQL DATA")
print("========================================")

with engine.connect() as connection:

    product_count = connection.execute(
        text("SELECT COUNT(*) FROM ProductTrends")
    ).scaler()

    kpi_count = connection.execute(
        text("SELECT COUNT(*) FROM DailyKPIs")
    ).scaler()


print(f"ProductTrends rows: {product_count}")

print(f"DailyKPIs rows: {kpi_count}")


print("\n========================================")
print("SQL LOADING COMPLETE")
print("========================================")


