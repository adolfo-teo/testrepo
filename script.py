import os
import subprocess
import json
import sqlparse

sql_folder = "/home/test/api/scripts"
execution_records_file = "/home/test/execution_records.json"
database_name = "school"

def execute_sql_command(sql_command):
    command = ['sudo', '-u', 'postgres', 'psql', '-d', database_name, '-c', sql_command.strip()]
    result = subprocess.run(command)
    return result.returncode == 0

def load_execution_records(file_path):
    if not os.path.isfile(file_path):
        return {}

    with open(file_path, "r") as file:
        try:
            return json.load(file)
        except json.JSONDecodeError:
            return {}

def save_execution_records(file_path, execution_records):
    with open(file_path, "w") as file:
        json.dump(execution_records, file)

def execute_sql_files(folder_path):
    execution_records = load_execution_records(execution_records_file)

    for root, dirs, files in sorted(os.walk(folder_path, topdown=True)):
        dirs.sort()

        folder_executed = True

        if root not in execution_records:
            execution_records[root] = {"success": False, "files": []}

        print(f"Executing folder: {root}")

        for file in sorted(files):
            if file.endswith(".sql"):
                sql_file_path = os.path.join(root, file)
                with open(sql_file_path, 'r') as sql_file:
                    content = sql_file.read()

                parsed = sqlparse.parse(content)
                queries = [str(query) for query in parsed]
                queries = [query.strip() for query in queries if query.strip()]

                if file not in execution_records[root]["files"]:
                    execution_records[root]["files"].append(file)

                if execution_records[root].get(file) is None:
                    execution_records[root][file] = {"last_executed_line": -1, "lines": []}

                current_line = execution_records[root][file]["last_executed_line"] + 1

                print(f"Executing file: {sql_file_path}")

                if current_line >= len(queries):
                    print(f"All queries in file {sql_file_path} are completed. Nothing to do.")
                    continue

                for line in queries[current_line:]:
                    if current_line < len(execution_records[root][file]["lines"]) and execution_records[root][file]["lines"][current_line]["success"]:
                        print(f"Line {current_line+1} in file {sql_file_path} already executed successfully.")
                    else:
                        if execute_sql_command(line.strip()):
                            execution_records[root][file]["lines"].append({"success": True})
                            execution_records[root][file]["last_executed_line"] = current_line
                            print(f"Line {current_line+1} in file {sql_file_path} executed successfully.")
                        else:
                            folder_executed = False
                            print(f"Error occurred on line {current_line+1} in file {sql_file_path}.")
                            break

                    current_line += 1

                lines_pending = len(queries) - current_line
                if lines_pending > 0:
                    print(f"There are {lines_pending} lines pending in file {sql_file_path}.")

        if folder_executed:
            execution_records[root]["success"] = True

    save_execution_records(execution_records_file, execution_records)

execute_sql_files(sql_folder)
