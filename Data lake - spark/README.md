## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## The purpose of this database in context of the startup, Sparkify, and their analytical goals.
Same as all other business, the final goal of data analytics is to increase the number of users and eventually the company revenue. 

To do so, the company would need to first need to know about user behaviours. 
By understanding the user behaviour e.g the demographic information such as countries, Sparkify can then find out the target customer group and do promotion accordingly.   

## Example queries and results for song play analysis
To increase the conversion, Sparkify can first try to find out those users which are active and not being paid member yet 
The query would be as follows: 

WITH paid_menber as (
SELECT 
   user_id, 
   MAX(CASE WHEN level='Paid' then 1 else 0 END) as max_status 
FROM 
   user_table
GROUP BY 1 
HAVING max_status = 1
)

SELECT 
   user_id, 
   first_name,
   last_name, 
   COUNT(DISTINCT sessionId) AS usage_times
FROM 
   songplays_table st
LEFT JOIN user_table ut ON st.user_id = ut.user_id
WHERE 
   user_id NOT IN (SELECT DISTINCT user_id FROM paid_member)
GROUP BY 1,2,3
ORDER BY 4 DESC 