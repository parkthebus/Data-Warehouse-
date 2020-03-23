## Udacity-Project3 : Cloud Data Warehouses
This project is the third project for Udacity Data Engineering Nanodegree Program.
In this project, I apply what I've learned on Data Warehouses and AWS to build an ETL pipeline for a database hosted on Redshift.

As an Data Engineer, I'm tasked with building an ETL pipeline that extracts music data from `Amazon S3`, stages them in `Amazon Redshift`, and transforms data into a set of dimensional tables for analytics team to continue finding insights in what songs their users are listening to.

The project is written in `python` and uses `Amazon s3` for file storage and `Amazon Redshift`
 for database storage and data warehouse purpose.

## Source Data
The source data is in log files given the Amazon s3 bucket  at `s3://udacity-dend/log_data` 
and `s3://udacity-dend/song_data` containing log data and songs data respectively.

Log files contains songplay events of the users in json format 
while song_data contains list of songs details.

## Database Schema
Following are the fact and dimension tables made for this project:
#### Dimension Tables:
   * users
        * columns: user_id, first_name, last_name, gender, level
   * songs
        * columns: song_id, title, artist_id, year, duration
   * artists
        * columns: artist_id, name, location, lattitude, longitude
   * time
        * columns: start_time, hour, day, week, month, year, weekday
   
#### Fact Table:
   * songplays
        * columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## To run the project:
   * Run command to install requirements.
        > pip install -r requirements.txt
   * Set your AWS environment.
   * Update the `dwh.cfg` file with you Amazon Redshift cluster credentials and IAM role that can access the cluster.
   * Run `python create_tables.py`. This will create the database and all the required tables.
   * Run `python etl.py`. This will start pipeline which will read the data from files and populate the tables.
