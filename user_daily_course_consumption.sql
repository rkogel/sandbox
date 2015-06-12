select con.*,
			 coalesce(note.count,0) as notes,
			 coalesce(cd.count,0) + coalesce(cdr.count,0) as discussions
from
	(
	select upl.userid,
				 upl.courseid,
				 date(upl.created),
				 count(1)*.25 as minconsumed,
				 count(case when upl.clientid =1 or udv.device = 'web' then 1 end)*.25 as webminutes
			 	-- add 1 column for min consumed of new content
	from user_progressed_lecture upl	
	left join user_daily_visit_before021015 udv on udv.userid = upl.userid and udv.date = date(upl.created)
	where upl.total >1
	group by 1,2,3
	) con
left join
	(
	select userid, courseid, date(created), count(1)
	from note
	group by 1,2,3
	) note
	on note.userid = con.userid and note.courseid = con.courseid and note.date = con.date
left join
	(
	select userid, courseid, date(created), count(1)
	from course_discussion
	group by 1,2,3
	) cd
	on cd.userid = con.userid and cd.courseid = con.courseid and cd.date = con.date
left join
	(
	select cdr.userid, cd.courseid, date(cdr.created), count(1)
	from course_discussion_reply cdr, course_discussion cd
	where cdr.discussionid = cd.id
	group by 1,2,3
	) cdr
	on cdr.userid = con.userid and cdr.courseid = con.courseid and cdr.date = con.date
;
