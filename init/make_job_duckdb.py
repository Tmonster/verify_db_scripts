import duckdb
import json
import glob
import os

con = duckdb.connect("job.duckdb")

with open("init/schema.sql") as f:
  con.execute(f.read())


JOB_CSV_DIR = "/Users/tomebergen/Documents/Datasets/imdb_clean"

con.execute(f"INSERT INTO aka_name SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/aka_name.csv');")
con.execute(f"INSERT INTO aka_title SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/aka_title.csv');")
con.execute(f"INSERT INTO cast_info SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/cast_info.csv');")
con.execute(f"INSERT INTO char_name SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/char_name.csv');")
con.execute(f"INSERT INTO comp_cast_type SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/comp_cast_type.csv');")
con.execute(f"INSERT INTO company_name SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/company_name.csv');")
con.execute(f"INSERT INTO company_type SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/company_type.csv');")
con.execute(f"INSERT INTO complete_cast SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/complete_cast.csv');")
con.execute(f"INSERT INTO info_type SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/info_type.csv');")
con.execute(f"INSERT INTO keyword SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/keyword.csv');")
con.execute(f"INSERT INTO kind_type SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/kind_type.csv');")
con.execute(f"INSERT INTO link_type SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/link_type.csv');")
con.execute(f"INSERT INTO movie_companies SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/movie_companies.csv');")
con.execute(f"INSERT INTO movie_info SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/movie_info.csv');")
con.execute(f"INSERT INTO movie_info_idx SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/movie_info_idx.csv');")
con.execute(f"INSERT INTO movie_keyword SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/movie_keyword.csv');")
con.execute(f"INSERT INTO movie_link SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/movie_link.csv');")
con.execute(f"INSERT INTO name SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/name.csv');")
con.execute(f"INSERT INTO person_info SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/person_info.csv');")
con.execute(f"INSERT INTO role_type SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/role_type.csv');")
con.execute(f"INSERT INTO title SELECT * FROM read_csv_auto('{JOB_CSV_DIR}/title.csv');")

# con.execute("EXPORT DATABASE 'job_exported' (FORMAT PARQUET);")

exit()
