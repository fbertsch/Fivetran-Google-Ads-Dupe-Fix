from google.cloud import bigquery
import google
import yaml
import click
import traceback

tables_and_keys_file = "tables_and_keys.yaml"

check_query_skeleton = """
select * from (
    select
        max({order_key})
            over (partition by {primary_keys_str}, updated_at) latest,
        *
    from `{project}.{dataset}.{table_name}`
) with_latest
where {order_key} < latest
"""


def check_rows(tables, project, dataset):
    print("Checking history tables for duplicates")
    client = google.cloud.bigquery.Client()
    queries = {}

    for table_name, details in tables.items():
        primary_keys_str = ", ".join(details["primary_keys"])
        check_query = check_query_skeleton.format(
            table_name=table_name,
            primary_keys_str=primary_keys_str,
            project=project,
            dataset=dataset,
            order_key=details["order_key"],
        )
        queries[table_name] = client.query(check_query)

    for table, query in queries.items():
        print(f"  {table}... ", end="")
        try:
            if res := list(query.result()):
                print(f"has {len(res)} duplicates")
            else:
                print(f"has no duplicates")
        except google.api_core.exceptions.NotFound:
            print(f"is not present")
            pass
        except google.api_core.exceptions.BadRequest as e:
            exception_details = traceback.format_exception_only(type(e), e)
            print(exception_details[-1].strip())


backup_query_skeleton = """
CREATE TABLE `{project}.{dataset}.{table_name}_backup`
AS SELECT * FROM `{project}.{dataset}.{table_name}`
"""
def backup_rows(tables, project, dataset):
    client = bigquery.Client()
    queries = {}

    for table_name, details in tables.items():
        backup_query = backup_query_skeleton.format(
            table_name=table_name,
            project=project,
            dataset=dataset,
        )
        queries[table_name] = client.query(backup_query)

    for table, query in queries.items():
        print(f"  {table}... ", end="")
        try:
            query.result()
            print(f"backed up")
        except google.api_core.exceptions.NotFound:
            print(f"is not present")
            pass
        except google.api_core.exceptions.BadRequest as e:
            exception_details = traceback.format_exception_only(type(e), e)
            print(exception_details[-1].strip())
        except google.api_core.exceptions.Conflict:
            print(f"is already backed up")


delete_backup_query_skeleton = """
DROP TABLE `{project}.{dataset}.{table_name}_backup`
"""
def delete_backup_rows(tables, project, dataset):
    client = bigquery.Client()
    queries = {}

    for table_name, details in tables.items():
        backup_query = delete_backup_query_skeleton.format(
            table_name=table_name,
            project=project,
            dataset=dataset,
        )
        queries[table_name] = client.query(backup_query)

    for table, query in queries.items():
        print(f"  {table}_backup... ", end="")
        try:
            query.result()
            print(f"deleted")
        except google.api_core.exceptions.NotFound:
            print(f"is not present")
            pass
        except google.api_core.exceptions.BadRequest as e:
            exception_details = traceback.format_exception_only(type(e), e)
            print(exception_details[-1].strip())
        except google.api_core.exceptions.Conflict:
            print(f"is already backed up")


delete_query_skeleton = """
delete from `{project}.{dataset}.{table_name}` tname
where exists (
    select * from (
        select
            max({order_key})
                over (partition by {primary_keys_str}, updated_at) latest,
            *
        from `{project}.{dataset}.{table_name}`
    ) with_latest
    where tname.{order_key} < with_latest.latest
    and with_latest.{order_key} = tname.{order_key}
    and with_latest.updated_at = tname.updated_at
    and {primary_keys_str_comparison}
);
"""


def delete_rows(tables, project, dataset):
    print("Deleting duplicates from history tables")

    client = bigquery.Client()
    queries = {}

    for table_name, details in tables.items():
        primary_keys_str = ", ".join(details["primary_keys"])
        primary_keys_str_comparison = " and ".join(
            (f"with_latest.{key} = tname.{key}" for key in details["primary_keys"])
        )
        deletion_query = delete_query_skeleton.format(
            table_name=table_name,
            primary_keys_str=primary_keys_str,
            primary_keys_str_comparison=primary_keys_str_comparison,
            project=project,
            dataset=dataset,
            order_key=details["order_key"],
        )
        try:
            table = client.get_table(f"{project}.{dataset}.{table_name}")

            print(deletion_query)
            i = input("Continue? y/n")
            if i != "y":
                continue

            queries[table_name] = (client.query(deletion_query), table.num_rows)
        except google.api_core.exceptions.NotFound:
            continue

    for table, (query, original_rows) in queries.items():
        print(f"  {table}... ", end="")
        try:
            res = query.result()
            table = client.get_table(f"{project}.{dataset}.{table_name}")
            print("updated, deleted {original_rows - table.num_rows} rows")
        except google.api_core.exceptions.NotFound:
            print(f"is not present")
            pass
        except google.api_core.exceptions.BadRequest as e:
            exception_details = traceback.format_exception_only(type(e), e)
            print(exception_details[-1].strip())


@click.command()
@click.option(
    "--delete", is_flag=True, help="Actually delete data. Run a backup and check first!"
)
@click.option(
    "--backup", is_flag=True, help="Backup the tables"
)
@click.option(
    "--delete-backup", is_flag=True, help="Delete backup tables"
)
@click.option(
    "--project", required=True, help="Bigquery project the tables are located in"
)
@click.option(
    "--dataset", required=True, help="Bigquery dataset the tables are located in"
)
def main(delete, backup, delete_backup, project, dataset):
    with open(tables_and_keys_file, "r") as f:
        tables = {table.lower(): details for table, details in yaml.safe_load(f).items()}

    if backup:
        backup_rows(tables, project, dataset)
    elif delete_backup:
        delete_backup_rows(tables, project, dataset)
    elif delete:
        delete_rows(tables, project, dataset)
    else:
        check_rows(tables, project, dataset)


if __name__ == "__main__":
    main()
