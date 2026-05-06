	WITH server1_user_id AS (
	    SELECT datetime::date, user_id, user_role_name, u.server_id, server_name, joined_at
	    FROM user_guild AS u
	    LEFT JOIN (select * from server_full_info where season = (select max(season) from server_full_info)) using(server_id)
	    WHERE u.server_id = 'ид сервера основного'
	        AND user_id <> 'ид бота'
	        AND datetime::date = (select max(datetime::date) from user_guild where server_id = 'ид сервера основного')
	        AND user_role_name <> ''
	),
	server2_user_id AS (
	    SELECT datetime::date, user_id, user_role_name, u.server_id, server_name, joined_at, language as server_language, faction, clan_name
	    FROM user_guild AS u
	    JOIN (select * from server_full_info where season = (select max(season) from server_full_info) and faction is not null) using(server_id)
	    WHERE u.server_id <> 'ид сервера основного'
	        AND user_id <> 'ид бота'
	        AND datetime::date = (select max(datetime::date) from user_guild)
	        AND user_role_name <> ''
	),
	user_all_display_name as (
	select user_id,
	   STRING_AGG(distinct user_display_name, E', ') AS user_display_name
	   from user_guild
	   group by 1
	),
	server_min_voice AS (
	    SELECT 
	        server_id, 
	        MIN(datetime::date) as min_server2_datetime
	    FROM voice_users
	    GROUP BY server_id
	)
	SELECT DISTINCT
	    STRING_AGG(
	    		DISTINCT COALESCE(u.user_name, ' ') || E'\n' || COALESCE(d.user_display_name, ' '),
	    '; ') AS user_name,
	    s1.datetime::date as datetime_s2_role,
	    s1.user_role_name AS server1_roles,
	    s2.user_role_name AS server2_roles,
	    s2.server_name AS server2_name,
	    s1.joined_at::date as server1_joined_at,
	    s2.joined_at::date as server2_joined_at,
	    STRING_AGG(distinct date_trunc('HOUR',voice.datetime) || ': ' || voice.voice_name || '', E'\n') AS voice_yesterday_hour_utc,
	    STRING_AGG(distinct voice_all.datetime::date || ': ' || voice_all.voice_name || '', E'\n') AS voice_last_7_days_utc,
	    faction,
	    min(s2_min_voice.min_server2_datetime) over (partition by s2.server_id) as min_date_info_server2_at,
	    clan_name, 
	    server_language,
	    s1.user_id,
	    s2.server_id AS server2_server_id,
	    max(voice_all.datetime)::date as last_voice,
	    COUNT(s2.server_name) OVER (PARTITION BY s2.server_name) AS count_common_server,
	    avatar
	FROM server1_user_id AS s1
	LEFT JOIN server2_user_id AS s2
	    ON s1.datetime = s2.datetime AND s1.user_id = s2.user_id
	LEFT JOIN server_min_voice AS s2_min_voice
	    ON s2.server_id = s2_min_voice.server_id    
	LEFT JOIN (
	    SELECT DISTINCT ON (user_id)
	        user_id,
	        user_name,
	        avatar
	    FROM user_guild
	    ORDER BY user_id, datetime DESC
	) AS u
	ON s1.user_id = u.user_id  
	LEFT JOIN user_all_display_name as d
		on d.user_id = u.user_id
	LEFT JOIN voice_users AS voice
	    ON DATE_TRUNC('day', s1.datetime) - INTERVAL '1 day' = DATE_TRUNC('day', voice.datetime) AND s1.user_id = voice.user_id AND s2.server_id = voice.server_id
	LEFT JOIN voice_users AS voice_all
	   ON s1.user_id = voice_all.user_id AND s2.server_id = voice_all.server_id 
	   AND (s2.user_role_name IS NULL OR s2.user_role_name <> ' ')
	WHERE voice_all.datetime BETWEEN (CURRENT_DATE - INTERVAL '7 days') AND CURRENT_DATE or voice_all.datetime is null
		and s2.user_role_name is not null
		group by 
	s1.datetime, s1.user_role_name,s2.user_role_name,s2.server_name,s1.joined_at,s2.joined_at,s2.faction,s2_min_voice.min_server2_datetime,s2.clan_name,s2.server_language,s1.user_id,s2.server_id,u.avatar
	ORDER BY count_common_server desc,server2_name,voice_yesterday_hour_utc,voice_last_7_days_utc,user_name
	;
