create table analytics.user_daily_course_consumption (
    userid bigint distkey
		courseid bigint encode lzo sortkey,
		date date sortkey,
		device varchar(25),
		is_first_watch boolean,
		is_offline boolean,
		is_running_in_background boolean,
		minconsumed float4
);
