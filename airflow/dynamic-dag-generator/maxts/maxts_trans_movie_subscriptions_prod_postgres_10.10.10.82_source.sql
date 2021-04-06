-- Sample SQL to find the max ts 
SELECT cast(Max({}) as varchar(40))
FROM   trans.movie_subscriptions 
WHERE  database_id=1 and state='success'
and {}>='{}';
