select con.*,
			 coalesce(note.count,0) as notes,
			 coalesce(cd.count,0) + coalesce(cdr.count,0) as discussions
from
	(
	select userid,
				 courseid,
				 date,
				 sum(case when is_first_watch =true then minconsumed end) as first_watch_consumption,
				 sum(case when is_first_watch =false then minconsumed end) as re_watch_consumption
	from user_daily_course_consumption
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
