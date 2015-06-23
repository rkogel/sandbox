with subcat as
        (
                select subcategory, 100*sum(case when score >=9 then 1.00 when score <=6 then -1.00 end)/count(1) as nps
                from user_nps
                where date > current_date - interval '3 month'
                group by 1 having count(1) >=60
        ),

cat as
        (
                select category, 100*sum(case when score >=9 then 1.00 when score <=6 then -1.00 end)/count(1) as nps
                from user_nps
                where date > current_date - interval '3 month'
                group by 1
        ),

con as
        (
                select ci.course_subcategory,
                       sum(udc.minconsumed)/count(distinct udc.userid) as avg_consumption_first7d
                from 
                        (
                                select userid,
                                       courseid,
                                       minconsumed,
                                       min(date) over(partition by userid, courseid),
                                       datediff(day, min(date) over (partition by userid, courseid), date) as delay
                                from analytics.user_daily_course_consumption
                        ) udc,
                        analytics.user_enrollment ue,
                        course_info ci
                where ue.traffic_channel != 'external_partner' and ue.created > '2014-1-1' and ue.is_refund=false
                and udc.min < current_date - interval '7 day' and udc.delay <7 and udc.min > current_date - interval '3 month'
                and ue.userid = udc.userid and ue.courseid = udc.courseid and (ue.paid_amount >0 or ue.course_was_premium =0)
                and ci.courseid = udc.courseid
                group by 1
        )

select a.courseid, a.nps_con_error, erf_con.odd, a.nps_rating_error, erf_rating.odd, a.raw_nps_error, erf_nps.odd,
       ((case when a.nps_qscore is null then a.nps_con else (a.nps_qscore + a.nps_con)*0.5 end)*erf_con.odd + coalesce(a.nps_rating*erf_rating.odd,0) + coalesce(a.raw_nps*erf_nps.odd,0))/(erf_con.odd + coalesce(erf_rating.odd,0) + coalesce(erf_nps.odd,0)) as predicted_nps,
       least(a.nps_con_error, a.nps_rating_error, a.raw_nps_error) as predicted_nps_error
from
        (
           select ci.courseid,
           				35 + ci.course_quality_score*.14 as nps_qscore,
                  case when subcat.nps is null then cat.nps else subcat.nps end + coalesce((0.4 - 0.15*case when is_premium in ('Yes', 'true') then 1 else 0 end)*(ci.avg_consumption_first7d - con.avg_consumption_first7d),0) as nps_con,
                  case when ci.avg_consumption_first7d is null or con.avg_consumption_first7d is null then 17.8 else 16.5 end as nps_con_error,
                  -100 + 30*ci.avg_rating as nps_rating,
                  case when num_rating <45 then 22*power(num_rating,-.1) when num_rating is null then null else 14.8 end as nps_rating_error,
                  ci.nps as raw_nps,
                  78*power(num_nps,-.5) as raw_nps_error
           from course_info ci
           left join subcat on subcat.subcategory = ci.course_subcategory
           inner join cat on cat.category = ci.course_category
           inner join con on con.course_subcategory = ci.course_subcategory
        ) a
left join adhoc.error_function_lookup erf_con on erf_con.z100 = round(1000/a.nps_con_error)
left join adhoc.error_function_lookup erf_rating on erf_rating.z100 = round(1000/a.nps_rating_error)
left join adhoc.error_function_lookup erf_nps on erf_nps.z100 = round(1000/a.raw_nps_error)
