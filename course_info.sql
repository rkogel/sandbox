insert into analytics.course_info
select distinct c.id as courseid,
                         c.title as course_title,
                         c.userid as instructor_userid,
                         u.title as instructor_title,
                         c.locale as course_locale,
                         date(c.created) as created,
                         cta.first_test_video_submit_date,
                         date(least(ch.min, c.publishedtime)) as first_published_date,
                         date(case when qrph.min is not null then qrph.min
                                                when c.publishedtime < '2012-12-01' and (cal.min is not null or c.adminrating >=7) then c.publishedtime
                                                when c.publishedtime >='2012-12-01' and cal.min is not null then cal.min
                                                when c.publishedtime >='2012-12-01' and c.adminrating >=7 then c.publishedtime end) as first_approved_date,
    
                                                                                                  c.ispremium as is_premium,
                         c.isprivate as is_private,
                         c.ispublished as is_published,
                         c.sourceorganizationid,                                                                                                 
                         case when cc.title is null then 'NA' else cc.title end as course_category,
                         case when cs.title is null then 'NA' else cs.title end as course_subcategory,
                         cs.istechnical as is_technical,
                         c.adminrating as course_admin_rating,
                         qrph2.score as course_quality_score,
                         case when qrph2.score>79 then true else false end as is_uhq,
                        
                         ue.totalrevenue as revenue,
                         ue.revenue30d as revenue_last30d,
                         pif.ispercentagedealsagreed as is_pct_deals_agreed,
                         pif.isfixedpriceddealsagreed as is_fixed_priced_deals_agreed,
                         pif.ismarketingboostagreed as is_market_boost_agreed,
                         pif.isufbcontentsubscriptionagreed as is_ufb_content_subscription_agreed,
                         
                                                                                                 nps.nps,
                                                                                                 nps.num_nps,
                         case when nps.num_nps_3mo >=250 then nps.nps_3mo else nps.nps_250 end as nps_trailing,
                         case when nps.num_nps_3mo >=250 then nps.num_nps_3mo else nps.num_nps_250 end as num_nps_trailing,
                         80*power(case when nps.num_nps_3mo >=250 then nps.num_nps_3mo else nps.num_nps_250 end, -0.5) as nps_trailing_precision,
                         pn.predicted_nps,
                         pn.predicted_nps_error,
                         
                         ue.totalenrollment as num_enrollment,
                         ue.paidenrollment as paid_enrollment,
                         
                         rat.reviewavg as avg_rating,
                         rat.reviewcount as num_rating,
                         
                         con.avg_consumption_first7d,
                         duration.hrs as hrs_of_content
     
from raw_data.course c

inner join raw_data.u_user u on u.id = c.userid

left join raw_data.course_has_subcategory chs on chs.courseid = c.id and chs.isprimary = 'true'
left join raw_data.course_subcategory cs on cs.id = chs.coursesubcategoryid
left join raw_data.course_category cc on cc.id = cs.coursecategoryid

left join
        (
        select courseid, min(created)
        from raw_data.course_admin_rating_log
        where adminrating >=7
        group by 1
        ) cal on cal.courseid = c.id

left join
        (
        select courseid, min(modified)
        from raw_data.course_quality_historicalqualityreviewprocess
        where status = 'approved' and scorecardid != 10
        group by 1
        )qrph
on qrph.courseid = c.id

left join raw_data.course_quality_historicalqualityreviewprocess qrph2 on qrph2.courseid = qrph.courseid and qrph2.modified = qrph.min

left join
        (
        select courseid, min(created)
        from raw_data.course_history
        where actiontype = 'publish-course'
        group by 1
        ) ch
on ch.courseid = c.id

-- NPS
left join 
  (
    select courseid,
           100*sum(case when score >=9 then 1 when score <=6 then -1 end)/count(1) as nps,
           count(1) as num_nps,
           100*sum(case when score >=9 and date >current_date - interval '3 month' then 1 when score <=6 and date >current_date - interval '3 month' then -1 end)/count(case when date >current_date - interval '3 month' then 1 end) as nps_3mo,
           count(case when date >current_date - interval '3 month' then 1 end) as num_nps_3mo,
           100*sum(case when score >=9  and rank <=250 then 1 when score <=6 and rank <=250 then -1 end)/count(case when rank <=250 then 1 end) as nps_250,
           count(case when rank <=250 then 1 end) as num_nps_250
    from
      (
        select courseid, date, score, rank() over(partition by courseid order by date desc)
        from analytics.user_nps
      )
    where rank <=250 or date >current_date - interval ' 3 month'
    group by 1 having count(1) >=20
  ) nps
on nps.courseid = c.id

-- change back user_enrollment after uniqueness issue has been resolved
-- revenue + enrollment
left join
        (
        select courseid,
                                 count(1) as totalenrollment,
                                 count(case when paid_amount >0 then 1 end) as paidenrollment,
                                 sum(paid_amount) as totalrevenue,
                                 sum(case when created > current_date - interval '30 day' then paid_amount else 0 end) as revenue30d
        from 
                (
                        select userid,
                                                 courseid,
                                                 sum(paid_amount) as paid_amount,
                                                 min(created) as created
                        from analytics.user_enrollment
                        where traffic_channel != 'external_partner'
                        group by 1,2
                )
        group by 1
        ) ue
on ue.courseid = c.id
        
-- predicted NPS
left join
        (
        select courseid, max(predicted_nps) as predicted_nps
        from predicted_nps
        group by 1
        ) pn
on pn.courseid = c.id
        
-- marketing program affiliations
left join
        (
        select distinct userid, ispercentagedealsagreed, isfixedpriceddealsagreed, ismarketingboostagreed,
        isufbcontentsubscriptionagreed
        from premium_instructor
        where active = 'true'
        ) pif
on pif.userid = c.userid
        
-- reviews
left join 
        (
        select courseid, avg(rating*1.00) as reviewavg, count(rating) as reviewcount
        from course_review
        where isspam = 'False'
        group by 1 having count(1) >=5
        ) rat
on rat.courseid = c.id

----test_video

left join

(
select courseid, date(min(created)) as first_test_video_submit_date
from course_test_asset
group by courseid
) cta

on c.id=cta.courseid

-- avg consumption of paid students
left join
        (
                select udc.courseid, sum(udc.minconsumed)/count(distinct udc.userid) as avg_consumption_first7d
                from 
                        (
                                select userid,
                                                         courseid,
                                                         minconsumed,
                                                         min(date) over(partition by userid, courseid),
                                                          datediff(day, min(date) over (partition by userid, courseid), date) as delay
                                from user_daily_course_consumption
                        ) udc,
                        (
                                select userid,
                                                         courseid,
                                                         sum(paid_amount) as paid_amount,
                                                         max(case when course_was_premium =true then 1 else 0 end) as course_was_premium
                                from analytics.user_enrollment
                                where traffic_channel != 'external_partner' and created > '2014-1-1'
                                group by 1,2
                        )        ue
                where udc.min < current_date - interval '7 day' and udc.delay <7
                and ue.userid = udc.userid and ue.courseid = udc.courseid and (ue.paid_amount >0 or ue.course_was_premium =0) -- change to false
                group by 1 having count(distinct udc.userid) >20
        ) con
on con.courseid = c.id

-- course duration
left join
        (
        select cha.courseid, sum(a.length)*1.00/3600 as hrs
        from course_has_asset cha, asset a
        where a.type = 'Video'
        and cha.assetid = a.id
        group by 1
        ) duration
on duration.courseid = c.id

where c.publishedtime is not null
