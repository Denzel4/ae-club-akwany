with  
    pull_requests as ( 
        select * from {{ ref('stg_github__pull_requests') }}
    ),
    repositories as ( 
        select * from {{ ref('stg_github__repositories') }}
    ),
    issues as ( 
        select * from {{ ref('stg_github__issues') }}
    ),
    issue_merged as ( 
        select * from {{ ref('stg_github__issue_merged') }}
    ),

final as (
    select 
        pull_requests.pull_request_id,
        repositories.name as repo_name,
        cast(null as string) as type, --( bug, eng, feature)
        case 
            when pull_requests.is_draft =True then 'draft'
            when issue_merged.merged_at is not null then 'merged'
            when issues.closed_at is not null then 'closed_without_merge'
            else 'open'
        end as state,
        issues.created_at as opened_at,
        issue_merged.merged_at as merged_at,
        date_diff(issue_merged.merged_at, issues.created_at, HOUR) / 24.0 as days_open_to_merge
    from pull_requests 
    left join repositories  
        on pull_requests.head_repo_id = repositories.repository_id   
    left join issues  
        on pull_requests.issue_id = issues.issue_id
    left join issue_merged    
        on pull_requests.issue_id = issue_merged.issue_id
)

select * from final
