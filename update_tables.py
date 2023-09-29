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
            table_name=table_name.lower(),
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

    tables = {
        "CAMPAIGN_APP_CAMPAIGN_SETTING_HISTORY": {
            "primary_keys": ["campaign_id"],
            "order_key": "_fivetran_end",
        }
    }
    for table_name, details in tables.items():
        primary_keys_str = ", ".join(details["primary_keys"])
        primary_keys_str_comparison = " and ".join(
            (f"with_latest.{key} = tname.{key}" for key in details["primary_keys"])
        )
        deletion_query = delete_query_skeleton.format(
            table_name=table_name.lower(),
            primary_keys_str=primary_keys_str,
            primary_keys_str_comparison=primary_keys_str_comparison,
            project=project,
            dataset=dataset,
            order_key=details["order_key"],
        )
        print(deletion_query)
        i = input("Continue? y/n")
        if i != "y":
            return
        queries[table_name] = client.query(deletion_query)

    for table, query in queries.items():
        print(f"  {table}... ", end="")
        try:
            if res := list(query.result()):
                print(f"has {len(res)} results")
                print(res)
            else:
                print(f"had no results")
        except google.api_core.exceptions.NotFound:
            print(f"is not present")
            pass
        except google.api_core.exceptions.BadRequest as e:
            exception_details = traceback.format_exception_only(type(e), e)
            print(exception_details[-1].strip())


@click.command()
@click.option(
    "--delete", is_flag=True, help="Actually delete data. Run without this first!"
)
@click.option(
    "--project", required=True, help="Bigquery project the tables are located in"
)
@click.option(
    "--dataset", required=True, help="Bigquery dataset the tables are located in"
)
def main(delete, project, dataset):
    with open(tables_and_keys_file, "r") as f:
        tables = yaml.safe_load(f)

    if delete:
        delete_rows(tables, project, dataset)
    else:
        check_rows(tables, project, dataset)


if __name__ == "__main__":
    main()
