# RWMLP Running Example: ETL

## Installation
The following python packages are needed:

```
pip install --upgrade numpy pandas SQLAlchemy google-cloud-storage google-cloud-pubsub
```

Additionally, you will need an installation of MySQL [MySQL](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/)

## First run
For the first run you will need to add your MySQL password and username to the scripts. Additionally, you will need to assign a project_id and api_key for the google cloud storage, if you want to use cloud uploads.

Then:
1. Run the generate_data.ipynb
2. Remove the ``break``statement at the end of the generate_data.ipynb (after step 1!)
3. Run ``python on_prem_watchdog.py``
4. Run generate_data.ipynb without the ``break``statement.
5. Check out your terminal loggings.
6. Stop the files when your done testing.

## Issues
When issues occure send a mail to [sascha.kaltenpoth@uni-paderborn.de](mailto:sascha.kaltenpoth@uni-paderborn.de).