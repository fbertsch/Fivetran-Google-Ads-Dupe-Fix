# Fivetran Google Ads Dupe Fix

Note: Just for BigQuery. AWS folks will need to help themselves.

Fix the stupid dupes from Fivetran Google Ads connector.
Annoying because it's so many tables.

Check for dupes:
```
python update_tables.py --project $PROJECT_NAME --dataset $DATASET_NAME
```

Backup tables:
```
python update_tables.py --project $PROJECT_NAME --dataset $DATASET_NAME --backup
```

Delete them dupes:
WARNING: Run at your own risk! I do not take responsibility if this deletes something you wanted to keep
```
python update_tables.py --project $PROJECT_NAME --dataset $DATASET_NAME --delete
```

And check that they're gone:
```
python update_tables.py --project $PROJECT_NAME --dataset $DATASET_NAME
```

Check your data!

Finally, delete the backups (or don't!)
```
python update_tables.py --project $PROJECT_NAME --dataset $DATASET_NAME --delete-backups
```
