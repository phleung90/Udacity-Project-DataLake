# Udacity-Project-DataLake

## Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. <br />
<br />
The project is to with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 
This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Datasets
The two datasets that reside in S3. Here are the S3 links for each:  <br />
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

**Song Dataset** <br />
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

An example is as follows:<br />
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
<br />
<br />
**Log Dataset**<br />
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.<br />
<br />
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.<br />
<br />
log_data/2018/11/2018-11-12-events.json<br />
log_data/2018/11/2018-11-13-events.json<br />
<br />
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.<br />
![image](https://user-images.githubusercontent.com/38469208/154943758-ef743627-5f35-4406-873f-c24c00f4a8f3.png)

## Data lake schema, fact and diemnsion table
The data mart schema is star schema, with `songplays` as a fact table and `users,songs,artists,time` tables as dimension tables

**Fact Table** <br />
**songplays** - records in log data associated with song plays i.e. records with page NextSong
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**<br />
**users** - users in the app<br />
  - user_id, first_name, last_name, gender, level<br />

**songs** - songs in music database
  - song_id, title, artist_id, year, duration<br />

**artists** - artists in music database
  - artist_id, name, location, lattitude, longitude<br />

**time** - timestamps of records in songplays broken down into specific units
  - start_time, hour, day, week, month, year, weekday<br />


## Spark process 
To create the dimension table, the process is as follows: 

1. Create a spark session 
2. Read the json file from S3 to spark dataframe
3. Select the data in spark dataframe and form the fact and dimension tables
4. Write the outfile df into a parquet file and upload to s3

## Files incurred 
`etl.py` - The main script<br /> 
`dl.cfg` - The access config to AWS 





