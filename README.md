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

For the scraper-script to work, you will also need to create a `.env`-file in your project's root folder.
Here you have to insert the following credentials:

```sh
# Example .env entries
REDDIT_CLIENT_ID="<client_id_you_have_obtained_from_reddit>"
REDDIT_CLIENT_SECRET="<client_secret_you_have_obtained_from_reddit>"
TWITTER_CONSUMER_KEY="<consumer_key_you_have_obtained_from_twitter>"
TWITTER_CONSUMER_SECRET="<consumer_secret_you_have_obtained_from_twitter>"
TWITTER_BEARER_TOKEN="<bearer_token_you_have_obtained_from_twitter>"
MONGO_HOST="<mongo_host_name>"
MONGO_PORT="<mongo_port_name>"
MONGO_INITDB_ROOT_USERNAME="<root_username_for_mongodb>"
MONGO_INITDB_ROOT_PASSWORD="<root_password_for_mongodb>"
KAFKA_BOOTSTRAP_SERVERS="<Servers where producer should connect to in the format host:port>"
```

If the script is started locally, you have to set the following environment variables for MongoDB and Kafka:
```sh
MONGO_HOST="localhost"
MONGO_PORT="27017"
KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

If the script is started as a service in the docker-compose file of the [oth-pipeline/infrastructure](https://github.com/oth-datapipeline/infrastructure) repository, you have to set the follwing values for MongoDB and Kafka:
```sh
MONGODB_HOST="mongo"
MONGODB_PORT="27017"
KAFKA_BOOTSTRAP_SERVERS="broker:29092" # if the Kafka Cluster consists of multiple hosts, you give the connections as comma separated strings (host1:port1,host2:port2,...)
```

## Starting the script
The main script which does the data collection and publishing to the Kafka Broker is `data_collection.py`.

It can be started like this in the console: `python data_collection.py --config CONFIG {rss, twitter, reddit}` where `--config` specifies the path to the configuration file
and the positional argument specifies the data source where data is collected from (RSS-feeds, Twitter or Reddit).

If you want to scrape data from **RSS-feeds** you have pass the additional argument `--base_url` which specifies the URL of the RSS-Feed database where the links to the relevant RSS-Feeds can be found.

## Developer documentation
The documentation of the classes and the scripts can be found in the [docs](docs) folder.
