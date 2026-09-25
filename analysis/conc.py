import duckdb,html,re,collections
P="'/Users/edwarddgao/openapply/data/date=2026-08-21/**/*.parquet'"
con=duckdb.connect(config={"memory_limit":"20GB","preserve_insertion_order":False,"threads":6})
cur=con.execute(f"SELECT source_slug,title,description_html FROM read_parquet({P},hive_partitioning=1)")
LI=re.compile(r"<li[^>]*>(.*?)</li>",re.S|re.I); TAG=re.compile(r"<[^>]+>")
FAM=[("forward deployed",r"forward deployed"),("recruiter",r"\brecruit(er|ing)\b"),
 ("AP/AR clerk",r"accounts (payable|receivable)|ap/ar|billing (clerk|specialist)"),
 ("payroll",r"payroll"),("bookkeeper",r"bookkeep|staff accountant"),
 ("credentialing",r"credential"),("claims processor",r"claims (processor|examiner|specialist|analyst)"),
 ("medical coder",r"medical (coder|coding)|coding specialist"),
 ("prior authorization",r"prior auth"),("data entry",r"data entry"),
 ("customer support",r"(customer|technical) support (rep|specialist|associate|agent)"),
 ("SDR",r"\b(sdr|bdr|sales development)\b"),("QA tester",r"\bqa (analyst|tester|engineer)\b"),
 ("compliance analyst",r"compliance (analyst|specialist|officer)"),
 ("underwriter",r"underwrit"),("loan processor",r"loan (processor|officer)"),
 ("scheduler",r"scheduler|scheduling coordinator"),("dispatcher",r"dispatcher"),
 ("AML/KYC",r"\b(aml|kyc|financial crime|sanctions)\b"),("title examiner",r"title (examiner|officer|closer)")]
FAMrx=[(n,re.compile(r,re.I)) for n,r in FAM]
ACT=re.compile(r"\b([a-z]{4,14}(?:e|ing|s)?)\s+((?:the |a |an |all |monthly |weekly |daily )?[a-z][a-z\- ]{3,26}?)(?=[,\.;:]| and | to | for | with | in | on |$)")
buckets=collections.defaultdict(list); cos=collections.defaultdict(set)
while True:
    rows=cur.fetchmany(20000)
    if not rows: break
    for slug,title,desc in rows:
        if not desc or not title: continue
        fam=next((n for n,rx in FAMrx if rx.search(title)),None)
        if not fam: continue
        cos[fam].add(slug)
        t=html.unescape(desc)
        for b in LI.findall(t):
            s=" ".join(TAG.sub(" ",b).split()).lower()
            if 25<len(s)<200: buckets[fam].append(s)
print(f"{'role family':22s} {'cos':>5} {'bullets':>8} {'top-20 task share':>19}")
res=[]
for fam,bl in buckets.items():
    if len(bl)<300: continue
    c=collections.Counter()
    for s in bl:
        for m in ACT.finditer(s):
            o=" ".join(m.group(2).split())
            if len(o)>=4 and len(o.split())<=4: c[(m.group(1),o)]+=1
    tot=sum(c.values()) or 1
    share=sum(n for _,n in c.most_common(20))/tot
    res.append((share,len(cos[fam]),len(bl),fam,c.most_common(4)))
for share,nc,nb,fam,top in sorted(res,reverse=True):
    print(f"{fam:22s} {nc:5,} {nb:8,} {share:18.1%}   e.g. {', '.join(v+' '+o for (v,o),_ in top[:3])[:70]}")
