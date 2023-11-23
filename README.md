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

![Screenshot of Output 1](/Images/Screenshot1.png)
![Screenshot of Output 2](/Images/Screenshot2.png)
![Screenshot of Output 3](/Images/Screenshot3.png)
![Screenshot of Output 4](/Images/Screenshot4.png)


## File structure



## License information

MIT License