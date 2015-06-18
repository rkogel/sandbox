select list1.*,
			 list1.uniqueclicks*1.00/uniqueviews as ctr,
			 list1.enrollments *1.00/uniqueviews as enrollrate,
			 ci.category as maincategory,
			 ci.subcategory as mainsubcategory
from course_info ci,
		 (
		 select left(created,7) as month,
		 				subcontext as keyword,
		 				count(distinct visitid) as uniqueviews,
		 				count(distinct case when islanded = 'true' then visitid end) as uniqueclicks,
		 				count(distinct case when enrolled = 'Yes'  then visitid end) as enrollments		 				
		 from user_discover_course_v2
		 where created > '2014-1-1' and context = 'search' and markedasseen = 'true' and subcontext not like '%free%' and subcontext not like '%paid%'
		 group by 1,2 having count(distinct visitid) >750
		 ) list1,
		 (
		 select subcontext as keyword,
		 				courseid,
		 				rank() over (partition by subcontext order by count(1) desc, courseid desc)
		 from user_discover_course_v2
		 where created > current_date - interval '30 day' and context = 'search' and markedasseen = 'true' and subcontext not like '%free%' and subcontext not like '%paid%'
		 and islanded = 'true'
		 group by 1,2 having count(1) >10
		 ) list2
where list2.keyword = list1.keyword and list2.rank =1
and ci.courseid = list2.courseid
order by 2 desc
;
