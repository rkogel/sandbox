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
		select ci.subcategory,
					 sum(udc.minconsumed)/count(distinct udc.userid) as avg_con_first7d
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

select a.courseid,
			 ((case when a.nps_qscore is null then a.nps_con else (a.nps_qscore + a.nps_con)*0.5 end)*erf_con.odd + coalesce(a.nps_review*erf_review.odd,0) + coalesce(a.raw_nps*erf_nps.odd,0))/(erf_con.odd + coalesce(erf_review.odd,0) + coalesce(erf_nps.odd,0)) as predicted_nps
from
	(
		select ci.courseid,
					 35 + ci.qualityscore*.14 as nps_qscore,
					 case when subcat.nps is null then cat.nps else subcat.nps end + coalesce((0.4 - 0.15*case when ispremium in ('Yes', 'true') then 1 else 0 end)*(ci.avg_con_first7d - con.avg_con_first7d),0) as nps_con,
					 case when ci.avg_con_first7d is null or con.avg_con_first7d is null then 17.8 else 16.5 end as nps_con_error,
					 -100 + 30*ci.reviewavg as nps_review,
					 case when reviewcount <45 then 22*power(reviewcount,-.1) else 14.8 end as nps_review_error,
					 ci.nps as raw_nps,
					 78*power(npscount,-.5) as raw_nps_error
		from course_info ci
		left join subcat on subcat.subcategory = ci.subcategory
		inner join cat on cat.category = ci.category
		inner join con on con.subcategory = ci.subcategory
	) a
left join adhoc.error_function_lookup erf_con on erf_con.z100 = round(1000/a.nps_con_error)
left join adhoc.error_function_lookup erf_review on erf_review.z100 = round(1000/a.nps_review_error)
left join adhoc.error_function_lookup erf_nps on erf_nps.z100 = round(1000/a.raw_nps_error)

