import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''create and configure pyspark session. Return spark session object'''
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    return spark
    


def process_song_data(spark, input_data, output_data):
    '''ETL function on song data. Receives spark session object, input and output of s3 paths'''
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*.json'
    
    # read song data file
    df =spark.read.json(song_data,multiLine=True) 

    # extract columns to create songs table
    songs_table=df.select('song_id','title','artist_id','year','duration').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').mode('overwrite').format("csv").save(output_data+'songs_table')

    # extract columns to create artists table
    artist_table=df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artist_table.write.mode('overwrite').format("csv").save(output_data+'artis_tstable')
    

def process_log_data(spark, input_data, output_data):
    '''ETL function on log data.Receives spark session object, input and output of s3 paths'''
    # get filepath to log data file
    log_data=input_data + 'log-data/*/*/*.json'

    # read log data file
    df =spark.read.json(log_data,multiLine=True)
    
    # filter by actions for song plays
    df = df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    artists_table=df.select('userId','firstName','lastName','gender','level').dropDuplicates(['userId'])
    
    # write users table to parquet files
    artists_table.write.mode('overwrite').format("csv").save(output_data+'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(year(df.timestamp).alias('year'), month(df.timestamp).alias('month'), dayofmonth(df.timestamp).alias('day_of_month'), hour(df.timestamp).alias('hour'),weekofyear(df.timestamp).alias('weekofyear'),df.timestamp).dropDuplicates(['timestamp'])

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode('overwrite').format("csv").save(output_data+'time_table')

    # read in song data to use for songplays table
    songs_df = songs_table.join(artist_table, songs_table.artist_id == artist_table.artist_id).drop(songs_table.artist_id)
    songs_df=songs_df.select('artist_id','song_id','title','artist_name','duration')
    songs_df=songs_df.join(df,(songs_df.title==df.song) & (songs_df.artist_name==df.artist) & (songs_df.duration==df.length),'right')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songs_df.select('timestamp','userId','level','song_id','artist_id','sessionId','location','userAgent')


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode('overwrite').format("csv").save(output_data+'songplays_table/')
    

def main():
    '''main function to use as module'''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://myawsbuckethermit97/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()
