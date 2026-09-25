import duckdb
P="'/Users/edwarddgao/openapply/data/**/*.parquet'"
con=duckdb.connect(config={"memory_limit":"20GB","preserve_insertion_order":False,"threads":6})
con.execute(f"""CREATE TABLE act AS SELECT source_slug co,
  count(DISTINCT CASE WHEN date<=DATE '2026-05-19' THEN id END) ej,
  count(DISTINCT CASE WHEN date> DATE '2026-07-15' THEN id END) lj
  FROM read_parquet({P},hive_partitioning=1) GROUP BY source_slug""")
TOOLS=["Snowflake","Databricks","Fivetran","LangChain","Kubernetes","Terraform","Airflow","dbt","Okta","Datadog"]
print("Among companies posting >=25 jobs in BOTH windows (so absence is meaningful):")
print(f"{'tool':12s} {'had early':>10} {'DROPPED':>9} {'drop rate':>10} {'ADOPTED':>9} {'adopt rate':>11}")
for t in TOOLS:
    con.execute(f"""CREATE OR REPLACE TEMP TABLE m AS SELECT source_slug co,
      max(CASE WHEN date<=DATE '2026-05-19' AND contains(lower(description_html),lower('{t}')) THEN 1 ELSE 0 END) e,
      max(CASE WHEN date> DATE '2026-07-15' AND contains(lower(description_html),lower('{t}')) THEN 1 ELSE 0 END) l
      FROM read_parquet({P},hive_partitioning=1) GROUP BY source_slug""")
    had,drop,noe,adopt=con.sql("""SELECT
        sum(CASE WHEN e=1 THEN 1 ELSE 0 END), sum(CASE WHEN e=1 AND l=0 THEN 1 ELSE 0 END),
        sum(CASE WHEN e=0 THEN 1 ELSE 0 END), sum(CASE WHEN e=0 AND l=1 THEN 1 ELSE 0 END)
      FROM m JOIN act USING(co) WHERE act.ej>=25 AND act.lj>=25""").fetchone()
    print(f"{t:12s} {had:10,} {drop:9,} {drop/max(had,1):9.1%} {adopt:9,} {adopt/max(noe,1):10.1%}",flush=True)
