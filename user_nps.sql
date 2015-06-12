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
			 sum(ue.pricepaid) as pricepaid
from user_course_feedback ucf

inner join course on course.id = ucf.courseid and course.sourceorganizationid is null
left  join course_has_subcategory chs on chs.courseid = course.id and chs.isprimary = 'true'
left  join course_subcategory cs on cs.id = chs.coursesubcategoryid
left  join course_category cc on cc.id = cs.coursecategoryid

inner join user_enrollment ue on ue.userid = ucf.userid
															and ue.courseid = ucf.courseid
															and ue.channel != 'external_partner'
															and date > '2014-1-1'
															and (ue.channel != 'instructor' or ue.pricepaid >0)

left  join user_completed_lecture ucl on ucl.courseid = ucf.courseid and ucl.userid = ucf.userid and ucl.lectureid = ucf.sourcelectureid

where ucf.score is not null and ucf.wascourseprivate =0

group by 1,2,3,4,5,6,7,8,9,10
