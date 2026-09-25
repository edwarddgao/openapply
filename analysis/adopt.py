import duckdb
P="'/Users/edwarddgao/openapply/data/**/*.parquet'"
con=duckdb.connect(config={"memory_limit":"20GB","preserve_insertion_order":False,"threads":6})
TOOLS=["Snowflake","Databricks","dbt","Fivetran","Looker","Kafka","Airflow","Salesforce","NetSuite",
"Workday","ServiceNow","Kubernetes","Terraform","LangChain","LangGraph","OpenAI","Anthropic","Datadog",
"Okta","Rippling","Ramp","Stripe","Temporal","Retool","Segment","Amplitude","Pinecone","Bedrock"]
sel=",\n".join([f"min(CASE WHEN contains(lower(description_html),lower('{t}')) THEN date END) AS \"{t}\"" for t in TOOLS])
con.execute(f"CREATE TABLE adopt AS SELECT source_slug, {sel} FROM read_parquet({P},hive_partitioning=1) GROUP BY source_slug")
print("companies:",con.sql("SELECT count(*) FROM adopt").fetchone()[0],flush=True)
d0,d1=con.sql(f"SELECT min(date),max(date) FROM read_parquet({P},hive_partitioning=1)").fetchone()
print("window",d0,"→",d1,flush=True)
cut=con.sql(f"SELECT DISTINCT date FROM read_parquet({P},hive_partitioning=1) ORDER BY date LIMIT 1 OFFSET 10").fetchone()[0]
print("cutoff (skip initial backlog):",cut,"\n",flush=True)
print(f"{'tool':14s} {'total cos':>10} {'NEW after cutoff':>18}")
for t in TOOLS:
    tot,new=con.sql(f'SELECT count("{t}"), count(CASE WHEN "{t}">DATE \'{cut}\' THEN 1 END) FROM adopt').fetchone()
    print(f"{t:14s} {tot:10,} {new:18,}",flush=True)
