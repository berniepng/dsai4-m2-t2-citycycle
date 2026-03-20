from google.cloud import bigquery

project = "citycycle-dsai4"
client = bigquery.Client(project=project)

sql = """
    SELECT
        id,
        installed,
        install_date,
        locked,
        name,
        latitude,
        longitude,
        docks_count,
        temporary,
        terminal_name
    FROM `bigquery-public-data.london_bicycles.cycle_stations`
"""

df = client.query(sql).to_dataframe()
print(f"Fetched {len(df)} stations")

table_id = f"{project}.citycycle_raw.cycle_stations"
job = client.load_table_from_dataframe(
    df,
    table_id,
    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
)
job.result()
print(f"Loaded {len(df)} rows into {table_id} ✓")
