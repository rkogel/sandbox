select * from (

select base.courseid,
			 case when nps.num_nps_3mo >=250 then nps.nps_3mo
			 			when nps.num_nps_250 >=30  then nps.nps_250
			 			when nps.num_nps_250  >=10 and rat.nps1 is not null then nps.num_nps_250*nps.nps_250/30 + (30-nps.num_nps_250)*(rat.nps1+rat.nps2+rat.nps3)/30
			 			when nps.num_nps_250  >=10 and con.nps1 is not null then nps.num_nps_250*nps.nps_250/30 + (30-nps.num_nps_250)*(con.nps1+con.nps2+con.nps3)/30
			 			when nps.num_nps_250  >=10 and con.nps1 is null and rat.nps1 is null then nps.num_nps_250*nps.nps_250/20 + (20-nps.num_nps_250)*base.nps/20
			 			when rat.nps1 is not null then .5*(rat.nps1+rat.nps2+rat.nps3) + .5*base.nps
			 			when con.nps1 is not null then .5*(con.nps1+con.nps2+con.nps3) + .5*base.nps
			 			else base.nps end as predictednps

from
	(
	select ci.courseid,
				 case when ci.qualityscore is not null and ci.subcategory != 'Other' then .5*subcat.nps + 0.5*(25+.33*ci.qualityscore)
				 			when ci.qualityscore is not null and ci.subcategory  = 'Other'  then .5*cat.nps + 0.5*(25+.33*ci.qualityscore)
				 			when ci.subcategory != 'Other' then subcat.nps
				 			else cat.nps end as nps
	from
		course_info ci,
		(
		select subcategory, 100*sum(case when score >=9 then 1.00 when score <=6 then -1.00 end)/count(1) as nps
		from user_nps
		and date > current_date - interval '90 day'
		group by 1
		) subcat,
		(
		select category, 100*sum(case when score >=9 then 1.00 when score <=6 then -1.00 end)/count(1) as nps
		from user_nps
		and date > current_date - interval '90 day'
		group by 1
		) cat
	where subcat.subcategory = ci.subcategory and cat.category = ci.category
	) base

left join 
  (
    select courseid,
           100*sum(case when score >=9 and date >current_date - interval '3 month' then 1 when score <=6 and date >current_date - interval '3 month' then -1 end)/count(case when date >current_date - interval '3 month' then 1 end) as nps_3mo,
           count(case when date >current_date - interval '3 month' then 1 end) as num_nps_3mo,
           100*sum(case when score >=9  and rank <=250 then 1 when score <=6 and rank <=250 then -1 end)/count(case when rank <=250 then 1 end) as nps_250,
           count(case when rank <=250 then 1 end) as num_nps_250
    from
      (
        select courseid, date, score, rank() over(partition by courseid order by date desc)
        from user_nps
      )
    where rank <=250 or date >current_date - interval ' 3 month'
    group by 1 having count(1) >=20
  ) nps
on nps.courseid = base.courseid

left join 
	(
	select cr.courseid,
				 -132.7 + 38.6*avg(cr.rating*1.00) as nps1,
				 case when ci.category in ('Development','IT & Software','Personal Development','Marketing','Business','Design','Office Productivity') then -6.6 else 0 end as nps2,
				 case when ci.ispremium = 'Yes' then -7.2 else 0 end as nps3
	from course_review cr, course_info ci
	where cr.isspam = 'No'
	and cr.courseid = ci.courseid
	group by 1,3,4 having count(1) >=5
	) rat
on rat.courseid = base.courseid

left join
	(
	select udc0.courseid,
				 32 + 0.4*sum(udc.minconsumed)/count(distinct udc0.userid) as nps1,
				 case when ci.category in ('Development','IT & Software','Personal Development','Marketing','Business','Design','Office Productivity') then -12 else 0 end as nps2,
				 case when ci.ispremium = 'Yes' then -12 else 0 end as nps3
	from 
		(
		select userid,
					 courseid,
					 min(date) as date0
		from user_daily_course_consumption
		group by 1,2 having min(date) >= '2014-1-1' and min(date) < current_date - interval '1 week'
		) udc0,
		user_enrollment ue,
		user_daily_course_consumption udc,
		course_info ci
	where (ue.paid_amount >0 or ue_course_was_premium = 'No') and ue.traffic_channel != 'external_partner'
	and udc.date >= '2013-1-1'
	and ci.courseid = udc0.courseid
	and ue.userid = udc0.userid and ue.courseid = udc0.courseid
	and udc.courseid = udc0.courseid and udc.userid = udc0.userid and udc.date <= udc0.date0 + interval '1 week'
	group by 1,3,4 having count(distinct udc0.userid) >=10
	) con
on con.courseid = base.courseid
group by 1,2

) order by random() limit 20
;
