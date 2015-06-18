select
		courseid,
		left(created, 7) as month,
		context,
		case when context not like 'search%' then subcontext else null end as subcontext,
		context2,
		subcontext2,
		count(distinct case when context = 'direct-landed' or markedasseen = 'true' then visitid end) AS uniqueviews,
		count(distinct case when (context = 'direct-landed' or markedasseen = 'true') and islanded = 'true' then visitid end) as uniqueclicks,
		count(distinct case when (context = 'direct-landed' or markedasseen = 'true') and enrolled = 'Yes'  then visitid end) as enroll
from user_discover_course_v2
group by 1,2,3,4,5,6 having count(distinct case when context = 'direct-landed' or markedasseen = 'true' then visitid end) >0
;
