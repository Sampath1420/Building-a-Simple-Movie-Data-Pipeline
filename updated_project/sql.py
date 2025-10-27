import sqlite3
import re

# Paths
db_path = "movie_analytics.db"
sql_file_path = "queries.sql"

# Connect to SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Read SQL file
with open(sql_file_path, 'r', encoding='utf-8') as file:
    sql_content = file.read()

# Remove comments (lines starting with --)
sql_content = re.sub(r'--.*', '', sql_content)

# Remove extra separator lines (like ---)
sql_content = re.sub(r'-{3,}', '', sql_content)

# Split queries by semicolon, clean them up
queries = [q.strip() for q in sql_content.split(';') if q.strip()]

print(f"Found {len(queries)} queries to execute.\n")

# Execute queries one by one
for i, query in enumerate(queries, start=1):
    try:
        print(f"Executing Query {i}:")
        print(query[:150] + ('...' if len(query) > 150 else ''))

        cursor.execute(query)
        results = cursor.fetchall()

        if query.lower().startswith('select'):
            print("Result:")
            for row in results:
                print(row)
        else:
            conn.commit()
            print("Query executed successfully (non-select).")

        print("\n" + "-" * 80 + "\n")

    except Exception as e:
        print(f"Error executing query {i}: {e}\n")

conn.close()
print("All queries executed successfully!")

# import sqlite3

# # 1. Connect to your database
# conn = sqlite3.connect("movie_analytics.db")  # replace with your .db file path
# cursor = conn.cursor()

# # 2. See all available tables (optional)
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# tables = cursor.fetchall()
# print("Tables in the database:", tables)

# # 3. Fetch all records from a specific table
# table_name = "movies"  # replace with the table name
# cursor.execute(f"SELECT * FROM {table_name};")


# # 4. Fetch results
# rows = cursor.fetchall()

# # 5. Print results
# for row in rows:
#     print(row)
# print(len(rows))

# # 6. Close connection
# conn.close()

