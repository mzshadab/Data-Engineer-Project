import os 
import pandas as pd
import mysql.connector

# Database config details

db_config = {
        "host" = "localhost",
        "user" = "root",
        "password" = "Shadab",
        "database" = "retail_db"
}

# Output Directory

local_output_dir = "/home/hdoop/retail_project/"
os.makedirs(local_output_dir,exist_ok=True)

# Hdfs output directory

hdfs_output_dir = "/data/extracted/"

# write it to hdfs

os.system(f"hdfs dfs -mkdir -p {hdfs_output_dir}")


# Extract_data

def extract_data(table_name):
    try:
        conn = mysql.connector.connect(**db_config)
        query = f"select * from {table_name}"
        df = pd.read_sql(query,conn)
        

        local_output_path = os.path.join(local_output_dir,f"{table_name}.csv")
        df.to_csv(local_output_path,index=False)
        print(f"Extracted {table_name} to {local_output_path})
         

        # Upload to hdfs
        hdfs_output_path = os.path.join(hdfs_output_dir, f"{table_name}.csv")
        os.system(f"hdfs dfs -put -f {local_output_path} {hdfs_output_path}")
        print(f"Uploaded {table_name} to hdfs at {hdfs_output_path}")

    finally:
        conn.close()

if __name__ == "__main__":
    extract_table("customers")
    extract_table("orders")


        
