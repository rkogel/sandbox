select c.id as courseid,
			 c.title,
			 c.userid,
			 u.title,
			 c.created,
			 case when cc.title is null then 'NA' else cc.title end as category,
			 case when cs.title is null then 'NA' else cs.title end as subcategory,
			 cs.istechnical,
			 least(ch.min, c.publishedtime) as firstpublishedtime,
			 case when qrph.min is not null then qrph.min
						when c.publishedtime < '2012-12-01' and (cal.min is not null or c.adminrating >=7) then c.publishedtime
						when c.publishedtime >='2012-12-01' and cal.min is not null then cal.min 
						when c.publishedtime >='2012-12-01' and c.adminrating >=7 then c.publishedtime end as firstapprovedtime,
			 c.locale,
			 c.adminrating,
			 qrph2.score as qualityscore,
			 c.ispremium,
			 c.isprivate,
			 c.ispublished,
			 c.sourceorganizationid,
			 
			 pif.ispercentagedealsagreed,
			 pif.isfixedpriceddealsagreed,
			 pif.ismarketingboostagreed,
			 
			 case when nps.num_nps_3mo >=250 then nps.nps_3mo else nps.nps_250 end as nps,
			 case when nps.num_nps_3mo >=250 then nps.num_nps_3mo else nps.num_nps_250 end as npscount,
			 80*power(case when nps.num_nps_3mo >=250 then nps.num_nps_3mo else nps.num_nps_250 end, -0.5) as precision,
			 pn.predictednps,
			 
			 ue.totalenrollment,
			 ue.paidenrollment,
			 ue.totalrevenue,
			 ue.revenue30d,
			 
			 rat.reviewavg,
			 rat.reviewcount,
			 
			 con.avg_consumption
			 
from course c

inner join u_user u on u.id = c.userid

left join course_has_subcategory chs on chs.courseid = c.id and chs.isprimary = 'true'
left join course_subcategory cs on cs.id = chs.coursesubcategoryid
left join course_category cc on cc.id = cs.coursecategoryid

left join
	(
	select courseid, min(created)
	from course_admin_rating_log
	where adminrating >=7
	group by 1
	) cal on cal.courseid = c.id

left join
	(
	select courseid, min(modified)
	from course_quality_historicalqualityreviewprocess
	where status = 'approved' and scorecardid != 10
	group by 1
	)qrph
on qrph.courseid = c.id

left join course_quality_historicalqualityreviewprocess qrph2 on qrph2.courseid = qrph.courseid and qrph2.modified = qrph.min

left join
	(
	select courseid, min(created)
	from course_history
	where actiontype = 'publish-course'
	group by 1
	) ch
on ch.courseid = c.id

-- NPS
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
on nps.courseid = c.id

-- revenue + enrollment
left join
	(
	select courseid,
				 count(1) as totalenrollment,
				 count(case when paid_amount >0 then 1 end) as paidenrollment,
				 sum(paid_amount) as totalrevenue,
				 sum(case when created > current_date - interval '30 day' then paid_amount else 0 end) as revenue30d
	from user_enrollment
	where traffic_channel != 'external_partner'
	group by 1
	) ue
on ue.courseid = c.id
	
-- predicted NPS
left join
	(
	select courseid, max(predictednps) as predictednps
	from predicted_nps
	group by 1
	) pn
on pn.courseid = c.id
	
-- marketing program affiliations
left join
	(
	select distinct userid, ispercentagedealsagreed, isfixedpriceddealsagreed, ismarketingboostagreed
	from premium_instructor_info
	where active = 'true'
	) pif
on pif.userid = c.userid
	
-- reviews
left join 
	(
	select courseid, avg(rating*1.00) as reviewavg, count(rating) as reviewcount
	from course_review
	where isspam = 'No'
	group by 1 having count(1) >=5
	) rat
on rat.courseid = c.id
	
-- avg consumption of paid students
left join
	(
	select udc.courseid, sum(udc.minconsumed)/count(distinct udc.userid) as avg_consumption
	from user_daily_course_consumption udc, user_enrollment ue
	where ue.paid_amount >0
	and ue.userid = udc.userid and ue.courseid = udc.courseid
	group by 1
	) con
on con.courseid = c.id

where c.publishedtime is not null

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
