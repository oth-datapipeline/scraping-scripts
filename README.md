# Scraping scripts
Repository for managing data scrapers/collectors for multiple data sources

## Getting started
After cloning this repository, create a venv in the folder of the repository using:

`python -m venv venv`

To activate the venv, enter the following command:

`venv\Scripts\activate` if you're on Windows

`source venv/bin/activate` on Unix

Afterwards, install all requirements:

`pip install -r requirements.txt`

## Starting the script
The main script which does the data collection and publishing to the Kafka Broker is `data_collection.py`.

It can be started like this in the console: `python data_collection.py --config CONFIG {rss, twitter, reddit}` where `--config` specifies the path to the configuration file 
and the positional argument specifies the data source where data is collected from (RSS-feeds, Twitter or Reddit).

If you want to scrape data from **RSS-feeds** you have pass the additional argument `--base_url` which specifies the URL of the RSS-Feed database where the links to the relevant RSS-Feeds can be found.

## Developer documentation
The documentation of the classes and the scripts can be found in the [docs](docs) folder.