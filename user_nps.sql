select ucf.courseid,
			 ucf.userid,
			 date(ucf.created) as date,
			 ucf.score,
			 ucf.comment,
			 cs.title as subcategory,
			 cc.title as category,
			 course.userid as instructorid,
			 course.ispremium,
			 case when ucl.oauth2clientid =1 then 'web' when ucl.oauth2clientid =5 then 'iOS app' when ucl.oauth2clientid =202 then 'Android app' else 'mini-apps & other' end as device,
			 ue.paid_amount as pricepaid

from raw_data.user_course_feedback ucf

inner join raw_data.course on course.id = ucf.courseid and course.sourceorganizationid is null
left  join raw_data.course_has_subcategory chs on chs.courseid = course.id and chs.isprimary = 'true'
left  join raw_data.course_subcategory cs on cs.id = chs.coursesubcategoryid
left  join raw_data.course_category cc on cc.id = cs.coursecategoryid

left  join raw_data.user_completed_lecture ucl on ucl.courseid = ucf.courseid and ucl.userid = ucf.userid and ucl.lectureid = ucf.sourcelectureid

-- revert back to joining ue directly onto ucf once the uniqueness issue has been solved and move the filters out of the subquery
inner join 
	(
		select userid,
					 courseid,
					 sum(paid_amount) as paid_amount,
					 max(case when course_was_premium =true then 1 else 0 end) as course_was_premium
		from analytics.user_enrollment
		where traffic_channel != 'external_partner' and (traffic_channel != 'instructor' or paid_amount >0) and created > '2014-1-1'
		group by 1,2
	) ue
on ue.userid = ucf.userid and ue.courseid = ucf.courseid

where ucf.score is not null and ucf.wascourseprivate =0

;
