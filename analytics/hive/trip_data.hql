-- 201402_trip_data (08-2013 to 02-2014)
-- 201408_trip_data (01-2014 to 08-2014)
-- 201508_trip_data (09-2014 to 08-2015)

-- top startstations (08-2013 to 08-2015)
select startstation, sum(total_visits) as total_visits
from (
    select startstation, count(1) as total_visits
    from 201408_trip_data
    group by startstation
  union all
    select startstation, count(1) as total_visits
    from 201402_trip_data
    group by startstation  
  union all
    select startstation, count(1) as total_visits
    from 201508_trip_data
    group by startstation
) t
group by startstation
order by total_visits desc;

-- worst startstations (08-2013 to 02-2014)
select startstation, count(1) as total_visits
from 201402_trip_data
group by startstation 
order by total_visits asc;

-- top endstations (08-2013 to 08-2014)
select endstation, sum(total_visits) as total_visits
from (
    select endstation, count(1) as total_visits
    from 201408_trip_data
    group by endstation
  union all
    select endstation, count(1) as total_visits
    from 201402_trip_data
    group by endstation 
) t
group by endstation
order by total_visits desc;

