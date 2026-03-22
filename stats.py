"""Stats sur la base DuckDB."""
import duckdb
from pathlib import Path

DB_PATH = Path("./inpi_bulk/benchmark.duckdb")

con = duckdb.connect(str(DB_PATH), read_only=True)

print("\n═══ BASE BENCHMARK INPI ═══")
total = con.execute("SELECT COUNT(*) FROM bilans").fetchone()[0]
print(f"Total bilans        : {total:,}")

annees = con.execute("SELECT annee, COUNT(*) as n FROM bilans GROUP BY annee ORDER BY annee DESC").fetchall()
print("\nPar année :")
for a, n in annees[:8]:
    print(f"  {a} : {n:,}")

nafs = con.execute("SELECT naf, COUNT(*) as n FROM bilans GROUP BY naf ORDER BY n DESC LIMIT 20").fetchall()
print("\nTop 20 NAF :")
for naf, n in nafs:
    print(f"  {naf} : {n:,}")

size = round(DB_PATH.stat().st_size / 1024 / 1024, 1)
print(f"\nTaille DuckDB : {size} MB")
con.close()
