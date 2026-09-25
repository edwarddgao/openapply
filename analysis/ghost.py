import duckdb
P="'/Users/edwarddgao/openapply/data/**/*.parquet'"
con=duckdb.connect(config={"memory_limit":"20GB","preserve_insertion_order":False,"threads":6})
con.execute(f"""CREATE TABLE life AS
 SELECT id, any_value(source_slug) co, any_value(title) title,
        min(date) first_seen, max(date) last_seen,
        count(DISTINCT date) days_seen
 FROM read_parquet({P},hive_partitioning=1) GROUP BY id""")
d0,d1=con.sql("SELECT min(first_seen),max(last_seen) FROM life").fetchone()
print(f"window {d0} → {d1}")
# only postings that genuinely APPEARED inside the window (exclude day-1 backlog)
con.execute(f"CREATE TABLE born AS SELECT * FROM life WHERE first_seen > DATE '{d0}'")
n=con.sql("SELECT count(*) FROM born").fetchone()[0]
print(f"postings first appearing inside the window: {n:,}\n")
print("how long do NEW postings stay live? (still-open ones counted at censor date)")
for d in (7,14,30,60,90):
    r=con.sql(f"""SELECT
       count(CASE WHEN date_diff('day',first_seen,last_seen)>={d} THEN 1 END)*1.0/count(*)
       FROM born WHERE first_seen <= DATE '{d1}' - INTERVAL {d} DAY""").fetchone()[0]
    print(f"   still live after {d:>3} days: {r:6.1%}")
print("\nlongest-running open postings (still live at window end):")
for r in con.sql(f"""SELECT co,title,date_diff('day',first_seen,last_seen) age
   FROM born WHERE last_seen=DATE '{d1}' ORDER BY age DESC LIMIT 8""").fetchall():
    print(f"   {r[2]:>4}d  {r[0]:<24} {r[1][:52]}")
print("\ncompanies with most long-running (>60d) open postings:")
for r in con.sql(f"""SELECT co, count(*) n FROM born
   WHERE last_seen=DATE '{d1}' AND date_diff('day',first_seen,last_seen)>60
   GROUP BY co ORDER BY n DESC LIMIT 10""").fetchall():
    print(f"   {r[1]:4}  {r[0]}")
