User Guide
==========

The API exposes two primary functions, one for storing and another for querying funds' data.

Start `mongod`
--------------

For starters, make sure you have `MongoDB <https://docs.mongodb.com/>`_ up and running.
Assuming you're running it locally, you can simply run ``mongo`` on a terminal. If it fails,
you need to start the service. To do so, you can try:

.. code-block:: shell

    # Linux
    sudo systemctl start mongod

    # Mac
    brew services start mongodb-community

All data retrieved from the endpoint will be written diretly to the MongoDB database and
collection configured in :py:mod:`bzfunds.settings.py <bzfunds.settings>`. By default, the
database is configured to run locally:

.. code-block:: python3

    # (...)

    MONGODB = {
        "host": "localhost",
        "port": 27017,
        "db": "bzfundsDB",
        "collection": "funds",
        "username": os.environ.get("MONGODB_USERNAME"),
        "password": os.environ.get("MONGODB_PASSWORD"),
    }


Downloading data
----------------

To retrieve and download available data, you can use the :py:func:`download_data
<bzfunds.api.download_data>` function. 

.. code-block:: python3

    from bzfunds import download_data

    download_data(start_year="2020")

By default, the function will download data for the last 5 years. The dataset starts in
2005. Downloading the whole history at once is possible by passing ``start_year=2005``,
but it takes a few minutes to run and requires around 5 GB of disk space (as of
2022).

Assuming you want to automatically update the dataset on a daily basis, you can use the
following syntax (and wrap it in some ``cronjob``):

.. code-block:: python3

    download_data(update_only=True)

This will only download data starting from the last available `date` stored in the
database.


Querying the database
---------------------

Once the data is downloaded into the configured database, you can easily query it either by one or
more funds' CNPJ, or an arbitrary date range, using :py:func:`get_data <bzfunds.api.get_data>`:

.. code-block:: python3

    from bzfunds import get_data

    df = get_data(start_dt="2020-01-01", end_dt="2020-03-30")

Dates must be either a `datetime` object or a string in the `YYYY-MM-DD` format.

