#!/usr/bin/python
# -*- coding: utf-8 -*-

#python3 youtube_pipeline.py --start_dt='2017-11-14' --end_dt='2018-06-14'

import sys

import getopt
from datetime import datetime, timedelta

import pandas as pd

from sqlalchemy import create_engine

if __name__ == "__main__":

    #input params setup
    unixOptions = "s:e:"  
    gnuOptions = ["start_dt=", "end_dt="] 

    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:]    #excluding script name

    try:  
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:  
        # output error, and return with an error code
        print (str(err))
        sys.exit(2)

    start_dt = ''
    end_dt = ''   
    for currentArgument, currentValue in arguments:  
        if currentArgument in ("-s", "--start_dt"):
            start_dt = currentValue                                   
        elif currentArgument in ("-e", "--end_dt"):
            end_dt = currentValue  

    #db configuration
    db_config = {'user': 'my_user',
                 'pwd': 'my_user_password',
                 'host': 'localhost',
                 'port': 5432,
                 'db': 'youtube'}   

    #creating a connection to DB
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                                db_config['pwd'],
                                                                db_config['host'],
                                                                db_config['port'],
                                                                db_config['db']))
    #getting raw data
    query = '''
                SELECT record_id,
                       video_id, 
                       region,                            
                       trending_date, 
                       channel_title, 
                       category_title, 
                       publish_time, 
                       views, 
                       likes, 
                       dislikes, 
                       comment_count
                FROM trending_vids
                WHERE trending_date BETWEEN '{}' AND '{}'
            '''.format(start_dt, end_dt)

    raw = pd.io.sql.read_sql(query, con = engine, index_col = 'record_id')
    raw['trending_date'] = pd.to_datetime(raw['trending_date']).dt.date

    #building agregated tables
    trending_by_time = (raw.groupby(['region', 'category_title', 'trending_date'])
                           .agg({'video_id': 'count'})
                           .rename(columns = {'video_id': 'videos_count'}))
    trending_by_time = trending_by_time.reset_index()   

    emotions_by_time_category = (raw.sort_values(by = ['region', 
                                                       'category_title',
                                                       'video_id', 
                                                       'trending_date'])
                                    .groupby(['region', 'video_id', 'category_title'])
                                    .agg({'trending_date': 'last',
                                          'likes': 'last',
                                          'dislikes': 'last'})
                                    .reset_index()
                                    .groupby(['region', 'category_title', 'trending_date'])
                                    .agg({'likes': 'sum', 'dislikes': 'sum'}))                      
    emotions_by_time_category = emotions_by_time_category.reset_index()    

    #writing results to db
    tables = {'trending_by_time' : trending_by_time, 
              'emotions_by_time_category': emotions_by_time_category}

    for table_name, table_data in tables.items():

        #flushing the existing data
        query = '''
                    DELETE FROM {} WHERE trending_date BETWEEN '{}' AND '{}'
                '''.format(table_name, start_dt, end_dt)
        engine.execute(query)

        #writing new data 
        table_data.to_sql(name = table_name,
                          con = engine, 
                          if_exists = 'append', 
                          index = False)

    print('All done.')    