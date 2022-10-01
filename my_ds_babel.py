import sqlite3
import pandas as pd
import csv
from pathlib import Path

class My_DS_Babel:

    def sql_to_csv(db_name, table_name):
        con = sqlite3.connect(db_name)
        cur = con.cursor()
        # res = cur.execute("SELECT * FROM fault_lines")
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
        df.to_csv("all_fault_lines.csv", index=False)
        cur.close()

    def csv_to_sql(db_name, table_name, csv_name):
        Path(db_name).touch()
        con = sqlite3.connect(db_name)
        cur = con.cursor()
        df = pd.read_csv(csv_name)
        column_names = tuple([col for col in df.columns])
        cur.execute(f'''CREATE TABLE {table_name} {column_names}''')
        con.commit()
        df.to_sql(table_name, con, if_exists='append', index = False)
        con.close()

My_DS_Babel.sql_to_csv('all_fault_line.db', "fault_lines")
My_DS_Babel.csv_to_sql('list_volcanos.db', "volcanos", "list_volcano (1).csv")
