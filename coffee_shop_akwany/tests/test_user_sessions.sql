select visitor_id, session_id, count(*) as pageviews_in_session
from {{ ref('int_user_sessions') }}
group by 1, 2
order by 1, 3 desc
