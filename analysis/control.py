import duckdb
P="'/Users/edwarddgao/openapply/data/**/*.parquet'"
con=duckdb.connect(config={"memory_limit":"20GB","preserve_insertion_order":False,"threads":6})
# companies posting CONTINUOUSLY: present both early and late, with real volume in each
con.execute(f"""CREATE TABLE act AS
 SELECT source_slug co,
   count(DISTINCT CASE WHEN date<=DATE '2026-05-19' THEN id END) early_jobs,
   count(DISTINCT CASE WHEN date> DATE '2026-07-15' THEN id END) late_jobs
 FROM read_parquet({P},hive_partitioning=1) GROUP BY source_slug""")
cont=con.sql("SELECT count(*) FROM act WHERE early_jobs>=10 AND late_jobs>=10").fetchone()[0]
print(f"continuously-posting companies (>=10 jobs in each window): {cont:,} of 11,233\n")
TOOLS=["Snowflake","Databricks","Fivetran","LangGraph","LangChain","Kubernetes","Terraform","Anthropic","Airflow","dbt"]
print(f"{'tool':12s} {'had early':>10} {'ADOPTED late':>13} {'rate':>7}   (continuous posters only)")
for t in TOOLS:
    con.execute(f"""CREATE OR REPLACE TEMP TABLE m AS
      SELECT source_slug co,
        max(CASE WHEN date<=DATE '2026-05-19' AND contains(lower(description_html),lower('{t}')) THEN 1 ELSE 0 END) e,
        max(CASE WHEN date> DATE '2026-07-15' AND contains(lower(description_html),lower('{t}')) THEN 1 ELSE 0 END) l
      FROM read_parquet({P},hive_partitioning=1) GROUP BY source_slug""")
    r=con.sql("""SELECT sum(CASE WHEN e=1 THEN 1 ELSE 0 END), sum(CASE WHEN e=0 AND l=1 THEN 1 ELSE 0 END),
                        count(*)
                 FROM m JOIN act USING(co) WHERE act.early_jobs>=10 AND act.late_jobs>=10""").fetchone()
    had,adopted,tot=r
    print(f"{t:12s} {had:10,} {adopted:13,} {adopted/max(tot-had,1):7.1%}",flush=True)
