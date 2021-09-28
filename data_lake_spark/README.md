    # S3 Data lake
This project focus on building a Data Lake with AWS S3 infrastructure. For Dealing with the data, Pyspark is going to be used, the Spark interface for Python.

## Data flow
Basicly, the data is loaded from S3, transformed with pyspark in a relational model  through a star schema, and then loaded back to S3:

1. Load data from S3
2. Transform the data in a relational model with pyspark
3. Load data back to S3

### Data Format
The data  it's JSON in the raw format. Once the data is transformed, the data is going to be loaded in the .parquet format.

## Tables
The data will have four dimension tables and one fact table:
##### Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong: \
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


##### Dimension Tables 
- users - users in the app: \
user_id, first_name, last_name, gender, level


- songs - songs in music database: \
song_id, title, artist_id, year, duration


- artists - artists in music database: \
artist_id, name, location, lattitude, longitude


- time - timestamps of records in songplays broken down into specific units: \
start_time, hour, day, week, month, year, weekday


### Requirements for running
- Python 3
- AWS Account
- S3 configurated for receiving files

### etl.py
The etl file that has all the functions that extract, transform and load the data.

### dl.cfg
File that has the AWS credentials needed to acess the S3 service.

### etl.pynb
Jupyter notebook that was used to build the etl.py file.

