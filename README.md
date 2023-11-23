# Multinational Retail Data Centralisation

# Table of Contents
1. [Introduction](#introduction)
1. [Installation instructions](#installation-instructions)
1. [Usage instructions](#usage-instructions)
1. [File structure](#file-structure)
1. [License information](#license-information)

## Introduction

Multinational Retail Data Centralisation is a project to pull various sources of data that corresponds to retail business and collate them into a single database.
Data sources are a variety of formats (PostrgreSQL database, PDF, CSV, JSON) hosted on AWS. Accessing data is done through a variety of methods to test different tools.

## Installation instructions

To install this Multinational Retail Data Centralisation project clone and enter the repository
```
git clone https://github.com/Ciaran-Mu/multinational-retail-data-centralisation.git
cd multinational-retail-data-centralisation
```
Before running the program, credentials for local and remote database access must be created in a secret file (not included in this repository) called `db_creds.yaml`. This credentials file should contain the following:

```
RDS_HOST: #HOST
RDS_PASSWORD: #PASSWORD
RDS_USER: #USER
RDS_DATABASE: #DATABASE
RDS_PORT: #PORT

LOCAL_HOST: #HOST
LOCAL_PASSWORD: #PASSWORD
LOCAL_USER: #USER
LOCAL_DATABASE: #DATABASE
LOCAL_PORT: #PORT
```

## Usage instructions

Run the main file
```
python main.py
```
_Note that some functionality will not be accessible without the correct database credentials which are in a secret file `db_creds.yaml`._

The program will run through the sections of extracting data, cleaning data, uploading to new local database and finally querying the database.

The following is the typical output of the program in the terminal.

![Screenshot of Output 1](/Images/Screenshot1.png)
![Screenshot of Output 2](/Images/Screenshot2.png)
![Screenshot of Output 3](/Images/Screenshot3.png)

## File structure

`database_utils.py` contains a classes 'DatabaseConnector' and 'DatabaseModifier' which initiate a connection to a database and modify an existing database, respectively.

`data_extraction.py` contains a class 'DataExtractor' which contains methods for extracting data from multiple different sources.

`data_cleaning.py` contains a class 'DataCleaning' which contains static methods for performing the data cleaning steps.

`data_querying.py` contains a class 'DataQuerier' which contains static methods for a set of specific SQL queries to be sent to a database.

`main.py` contains the main program which calls all the relevant methods of the classes defined within the above python files.

## License information

MIT License