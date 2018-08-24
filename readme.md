# Google DataFlow python workflow for Stat - App Engine deployment with CRON.

Stat dataflow Process by [JR Oakes](https://twitter.com/jroakes) of [Adapt](https://adaptpartners.com/). Also updated for Dataflow to work with more current releases of `google-cloud-dataflow` and `apache-beam`.

Google App Engine Deployment by Marcin.  Blog post [here](http://zablo.net/blog/post/python-apache-beam-google-dataflow-cron).

---
This repository contains a project for logging Stat API data to BigQuery. It can be used as an example of how to deploy Google Dataflow (Apache Beam) pipeline to App Engine in order to run it as as CRON job.
*It only works on App Engine Flex Environment*, due to I/O used by Apache Beam (on App Engine Standard it throws an error about Read-only file system).

## Description
1. **setup.py** file is important - without it, Dataflow engine will be unable to distribute packages across dynamically spawned DF workers
1. **app.yaml** contains definition of App Engine app, which will spawn Dataflow pipeline
1. **cron.yaml** contains definition of App Engine CRON, which will ping one of the App endpoints (in order to spawn Dataflow pipeline)
1. **appengine_config.py** adds dependencies to locally installed packages (from **lib** folder)
1. **config_template.py** contains configuration options for Stat Dataflow pipeline.

## Instruction
1. Remember to put ```__init__.py``` files into all local packages
1. Install all required packages into local **lib** folder: ```pip install -r requirements.txt -t lib```
1. Add file named `config.py` to `dataflow_pipeline` using `config_template.py` as an example.
1. To deploy App Engine app, run: ```gcloud app deploy app.yaml```
1. To deploy App Engine CRON, run: ```gcloud app deploy cron.yaml```

## Config Options
* **SUBDOMAIN**: The subdomain of your getstat.com URL. (eg. )[subdomain].getstat.com)
* **API_KEY**: Your API key from Stat
* **PRIOR_DAYS**: The number od days in the rears to pull Stat ranking data.  Default is `3`. {type: Integer}
* **DATASET**: The name of your BigQuery database.  Should already exist.
* **TABLE_NAME**: The name of the table in your BigQuery database.  Should not exist.
* **PROJECT**: Your GCS project id.
