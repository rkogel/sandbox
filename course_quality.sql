select com.courseid,
			 com.history_id,
			 case when com.rankprocess = 1 then 'initial' else 'final' end as revieworder,
			 com.status as reviewstatus,
			 com.score,
			 com.history_date,
			 com.scorecardid,
			 com.qualitycriteriaid,
			 com.qualitycriteria,
			 com.criteriastatus,
			 cc.id as cannedcommentid,
			 cc.title as cannedcomment,
			 case when chc.id is not null then 'selected' end as cannedcommentstatus
from
	(
	select cri.*,
				 c.id as commentid,
				 rank() over (partition by cri.history_id, cri.commentthreadid order by c.id desc) as rankcomment
	from
		(
		select proc.*,
					 qcfh.qualitycriteriaid,
					 qc.title as qualitycriteria,
			 		 qcfh.commentthreadid,
			 		 qcfh.rating as criteriastatus, 
			 		 qcfh.history_date as criteria_date,
			 		 rank() over (partition by qcfh.qualitycriteriaid, proc.history_id order by qcfh.history_id desc) as rankcriteria
		from
			(
			select qrph.courseid,
						 qrph.id as qualityreviewprocessid,
						 qrph.history_id,
						 qrph.history_date,
						 qrph.scorecardid,
						 qrph.status,
						 case when status = 'approved' then score end as score,
						 rank() over (partition by qrph.courseid order by qrph.history_id) as rankprocess
			from
				(
				select courseid, min(history_id), max(history_id)
				from course_quality_historicalqualityreviewprocess
				where status in ('approved', 'needs_fixes')
				group by 1
				) ref,
			course_quality_historicalqualityreviewprocess qrph
			where qrph.courseid = ref.courseid and qrph.history_id in (ref.min, ref.max)
			) proc,
		course_quality_historicalqualitycriteriafeedback qcfh,
		quality_criteria qc
		where qcfh.qualityreviewprocessid = proc.qualityreviewprocessid and qcfh.history_date < proc.history_date
		and qc.id = qcfh.qualitycriteriaid
		) cri
	left join comment c on c.commentthreadid = cri.commentthreadid and c.created < cri.history_date
	where cri.rankcriteria = 1
	) com
left join canned_comment cc on cc.relatedobjectid = com.qualitycriteriaid and cc.relatedobjecttype = 'quality_criteria'
left join comment_has_canned_comment chc on chc.commentid = com.commentid and chc.cannedcommentid = cc.id and com.criteriastatus != 'exceptional'
where com.rankcomment = 1
;
