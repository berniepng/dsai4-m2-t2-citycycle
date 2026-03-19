from google.cloud import bigquery

project = "citycycle-dsai4"
client  = bigquery.Client(project=project)

sql = """
    SELECT
        id,
        installed,
        locked,
        CAST(install_date AS DATE)      AS install_date,
        CAST(removal_date AS DATE)      AS removal_date,
        name,
        CAST(terminal_name AS STRING)   AS terminal_name,
        CAST(latitude  AS FLOAT64)      AS latitude,
        CAST(longitude AS FLOAT64)      AS longitude,
        bikes_count,
        docks_count,
        nbEmptyDocks,
        temporary
    FROM `bigquery-public-data.london_bicycles.cycle_stations`
"""

df = client.query(sql).to_dataframe()
print(f"Fetched {len(df)} stations")

table_id = f"{project}.citycycle_raw.cycle_stations"
job = client.load_table_from_dataframe(
    df, table_id,
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
)
job.result()
print(f"Loaded {len(df)} rows into {table_id} ✓")