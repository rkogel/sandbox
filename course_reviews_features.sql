--create table adhoc.course_review_features_tmp as (

with cr as
   (select id as review_id,
           userid,
           courseid,
           created,
           modified,
           rating,
           avg(rating) over (partition by courseid
                                 order by modified asc rows unbounded preceding) as avg_course_rating_at_time_of_review,
           count(1) over (partition by courseid
                                  order by modified asc rows unbounded preceding) as num_course_reviews_at_time_of_review,
           avg(is_spam) over(partition by courseid
                                 order by modified asc rows unbounded preceding) as course_spam_pct_at_time_review,
           avg(rating) over (partition by userid) as avg_student_rating,
           count(1) over(partition by userid) as num_student_reviews,
           avg(is_spam) over(partition by userid) as student_spam_pct,
           count(1) over(partition by userid, date(created)) as num_reviews_per_student_per_day
    from raw_data.course_review
    where location in ('dashboard',
                       'my_courses',
                       '')
    order by courseid, userid, modified )

select * from (
select 
-- review and enrollment characteristics
       cr.*,
       log(greatest(cr.num_course_reviews_at_time_of_review, 1)) as logged_number_course_reviews_at_time_of_review,
       coalesce(ue.paid_amount, 0) as paid_amount,
       coalesce(ue.is_refund, FALSE) as is_refund,
       ue.coupon_code,
       coalesce(ue.course_was_premium, FALSE) as course_was_premium,
       ue.enrollment_created,
       row_number() over (partition by cr.review_id),￼
       log(greatest(cast(cons.min_consumed as double precision), 1)) as logged_consumption,

-- student characteristic
       u.countryid,
       u.userid_created,
       u.student_title,
       u.is_email_invalid,
       case when fraud.userid is not null then 1 else 0 end as is_payment_fraud,
       case when spam_cons.userid is not null then 1 else 0 end as is_spam_consumption,
       case when ii.instructor_userid is not null then 1 else 0 end as is_reviewer_instructor,
       coalesce(v.count, 1) as num_userid_created_by_this_visitorid,
       coalesce(coupon.num_suspect_coupons, 0) as num_suspect_coupons,
       coalesce(review_frequency.number_days_more_3_reviews, 0) as number_days_more_3_reviews,
       ue_student.student_num_paid_enrollments as studen_paid_enrollments,
       ue_student.student_spend_udemy,
       cr.num_student_reviews / ue_student.student_num_enrollment as pct_student_enrollment_reviews,

-- course characteristics
       log(greatest(ue_course.course_enrollment_time_review, 1)) as log_total_course_enrollments_time_review,
       log(greatest(ue_course.course_revenue_time_review, 1)) as logged_course_revenue,
       ue_course.pct_free_enrollments_course,
       greatest(datediff('month', ci.first_approved_date, cr.modified), 0) as course_months_on_platform_at_time_of_review

------------------
--- source of data
------------------

from cr

-- enrollment info associated with review
left join
   (select courseid,
           student_userid,
           paid_amount,
           is_refund,
           coupon_code,
           course_was_premium,
           enrollment_created,
           row_number() over(partition by courseid, student_userid order by enrollment_created)
    from analytics.user_enrollment) ue
on  ue.courseid = cr.courseid
and ue.student_userid = cr.userid
and ue.row_number =1

-- student info
left join
   (select id as userid,
           countryid,
           created as userid_created,
           title as student_title,
           case
             when emailstatus in ('spammed',
                                  'bounced',
                                  'invalid') then 1
             else 0
           end as is_email_invalid
    from raw_data.u_user) u
on u.userid = cr.userid

-- consumption at time of review
left join
   (select cr.review_id,
           count(1)*.25 as min_consumed
    from user_progressed_lecture upl,
         cr
    where upl.created < cr.modified
      and upl.courseid = cr.courseid
      and upl.userid = cr.userid
    group by 1) cons
on cons.review_id = cr.review_id

-- number of userids associated with a visitorid
left join
   (select distinct *
    from
       (select userid,
               count(1) over(partition by visitorid)
        from visit
        where userid is not null) ) v
on v.userid = cr.userid

-- student fraud
left join
   (select distinct userid
    from raw_data.fraud
    where isinvalidated ='No' ) fraud
on fraud.userid = cr.userid

-- spam consumption
left join
   (select distinct userid
    from analytics.spam_consumption) spam_cons
on spam_cons.userid = cr.userid

-- number of suspicious coupons per student
left join
   (select student_userid,
           count(1) as num_suspect_coupons
    from user_enrollment
    where (coupon_code ilike '%review%'
           or coupon_code ilike '%fivr%'
           or coupon_code ilike '%exchange%'
           or coupon_code ilike '%exch%')
      and coupon_code not ilike '%udemy-internal-review%'
      and coupon_code not ilike 'udemy_internal_review%'
      and coupon_code not ilike 'udemyinternalreview%'
    group by 1) coupon
on coupon.student_userid = cr.userid

-- instructor info
left join
   (select distinct instructor_userid
    from analytics.instructor_info ) ii
on ii.instructor_userid = cr.userid

-- total enrollment and spend by student (incl. after review is published)
left join
   (select student_userid,
           count(case when paid_amount >0 then 1 end) as student_num_paid_enrollments,
           count(1) as student_num_enrollment,
           sum(paid_amount) as student_spend_udemy
    from user_enrollment
    group by 1) ue_student
on ue_student.student_userid = cr.userid

-- course revenue / enrollment at time of review
left join
  (select ue.courseid,
           sum(ue.paid_amount) as course_revenue_time_review,
           count(1) as course_enrollment_time_review,
           count(case when paid_amount =0 then 1 end)::DEC/count(1) as pct_free_enrollments_course
    from cr,
         user_enrollment ue
    where ue.created < cr.modified
      and cr.courseid = ue.courseid
    group by 1) ue_course
on ue_course.courseid = cr.courseid

-- course publish info
left join
   (select courseid,
           first_approved_date,
           case when is_premium = 'Yes' then TRUE else FALSE end as is_premium
    from course_info
    where first_approved_date is not null
    ) ci
on ci.courseid = cr.courseid

-- instructor spam info at time of review
left join
   (select
    from course_info ci,
         cr
    where ci.courseid = cr.courseid ) instructor_spam
on instructor_spam.courseid = cr.courseid

-- number of days with >= 3 reviews per student
left join
   (select userid,
           count(distinct date(created)) as number_days_more_3_reviews
    from cr
    where num_reviews_per_student_per_day >= 3
    group by 1 ) review_frequency
on review_frequency.userid = cr.userid

limit 200
--where row_number = 1 )
