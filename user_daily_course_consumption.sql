select userid,
			 courseid,
			 date(created),
			 case when clientid =-2  then 'iOS Web'
			 			when clientid =-2  then 'Android Web'
				 		when clientid =1   then 'Web'
				 		when clientid =5   then 'iOS'
				 		when clientid =202 then 'Android' else 'mini-apps' end as device,
			 case when rank=1 then true else false end as is_first_watch,
			 isofflineprogress as is_offline,
			 isbackgroundprogress as is_running_in_background,
			 count(1)*.25 as minconsumed
from (select *, rank() over(partition by userid, courseid, assetid order by created) from user_progressed_lecture where total >1 and created < current_date)
group by 1,2,3,4,5,6,7
