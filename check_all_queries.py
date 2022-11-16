import psycopg2
import json
import glob
import duckdb
import os
import subprocess
files = glob.glob("queries/[0-9]*.sql")
files.sort()

con_duckdb = duckdb.connect("job.duckdb")
con_postgers = psycopg2.connect("dbname=job").cursor()

# disable postgres 'parallel' plans because exchange ops make me sad
con_postgers.execute('SET max_parallel_workers_per_gather = 0')
con_postgers.execute('SET enable_nestloop to false')

postgres_result = "postgres_result.txt"
duckdb_result = "duckdb_result.txt"

def run_query_on_postgres(query_file):
    global postgres_result
    with open(query_file, 'r') as file:
        con_postgers.execute(file.read())
        res = con_postgers.fetchall()
        with open(postgres_result, 'w') as result_file:
            for line in res:
                result_file.write(str(line))
                result_file.write("\n")


def run_query_on_duckdb(query_file):
    with open(query_file, 'r') as file:
        con_duckdb.execute(file.read())
        res = con_duckdb.fetchall()
        with open(duckdb_result, 'w') as result_file:
            for line in res:
                result_file.write(str(line))
                result_file.write("\n")
    

def is_there_diff():
    try:
        out = subprocess.check_output(["diff", postgres_result, duckdb_result])
    except Exception as e:
        return True
    return False



def main():
    diff = False
    diff_queries = ""
    for query in files:
        run_query_on_postgres(query)
        run_query_on_duckdb(query)
        is_there_diff()
        if is_there_diff():
            print(f"{query} produces different results from duckdb and postgres")
            diff_queries += f"{query.replace("queries/","").replace(".sql", "")}, "
            diff = True
    if diff:
        print(f"looks queries {diff_queries} produce different result(s)")
    else:
        print("No differences detected, YAY! DuckDB and postgres are the same.")

main()
os.remove(postgres_result)
os.remove(duckdb_result)