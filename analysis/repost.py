import duckdb
P="'/Users/edwarddgao/openapply/data/**/*.parquet'"
con=duckdb.connect(config={"memory_limit":"20GB","preserve_insertion_order":False,"threads":6})
con.execute(f"""CREATE TABLE life AS SELECT id, any_value(source_slug) co, any_value(lower(trim(title))) t,
  min(date) f, max(date) l FROM read_parquet({P},hive_partitioning=1) GROUP BY id""")
d1=con.sql("SELECT max(l) FROM life").fetchone()[0]
# same company + same title, but a NEW id starting after an old one ended = delete-and-repost
r=con.sql(f"""
 WITH pairs AS (
   SELECT a.co, a.t, a.id old_id, b.id new_id, a.l AS old_end, b.f AS new_start
   FROM life a JOIN life b ON a.co=b.co AND a.t=b.t AND a.id<>b.id
   WHERE b.f > a.l AND date_diff('day',a.l,b.f) BETWEEN 0 AND 30)
 SELECT count(*) FROM pairs""").fetchone()[0]
tot=con.sql("SELECT count(*) FROM life").fetchone()[0]
print(f"total postings: {tot:,}")
print(f"delete-and-repost pairs (same company+title, new id within 30d of old ending): {r:,}")
print(f"  => {r/tot:.2%} of postings are relistings my method would score as NEW\n")
print("companies doing it most:")
for row in con.sql(f"""
 WITH pairs AS (SELECT a.co, a.t FROM life a JOIN life b ON a.co=b.co AND a.t=b.t AND a.id<>b.id
   WHERE b.f > a.l AND date_diff('day',a.l,b.f) BETWEEN 0 AND 30)
 SELECT co, count(*) n FROM pairs GROUP BY co ORDER BY n DESC LIMIT 8""").fetchall():
    print(f"   {row[1]:5}  {row[0]}")
