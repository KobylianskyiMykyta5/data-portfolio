select ad_date,
round(avg(spend),2) as avg_spend,
round(max(spend),2) as max_spend,
round(min(spend),2) as min_spend
from facebook_ads_basic_daily
group by ad_date
order by ad_date asc;

select ad_date,
round(avg(spend),2) as avg_spend,
round(max(spend),2) as max_spend,
round(min(spend),2) as min_spend
from google_ads_basic_daily
group by ad_date
order by ad_date asc;

select fabd.ad_date,
round(coalesce(cast((sum(fabd.value)-sum(fabd.spend)) as numeric) / nullif (sum(fabd.spend),0),0) *100 +coalesce(cast((sum(gabd.value)-sum(gabd.spend)) as numeric) / nullif (sum(gabd.spend),0),0) *100, 1) as total_ROMI
from homeworks.facebook_ads_basic_daily as fabd
left join public.google_ads_basic_daily as gabd
on fabd.ad_date = gabd.ad_date
group by fabd.ad_date
order by total_ROMI desc
limit 5;

with f_g_basic as (select fabd.ad_date, fc.campaign_name, fabd.value
from homeworks.facebook_ads_basic_daily as fabd
left join facebook_campaign as fc
on fc.campaign_id=fabd.campaign_id
union all
select gabd.ad_date, gabd.campaign_name, gabd.value
from public.google_ads_basic_daily as gabd)
select campaign_name, sum(value) as total_value,
to_char(date_trunc('week', ad_date), 'YYYY-MM-DD') as week
from f_g_basic
group by week, campaign_name 
order by total_value desc
limit 1;

with f_g as (select fabd.ad_date, fc.campaign_name, fabd.reach
from homeworks.facebook_ads_basic_daily as fabd
left join facebook_campaign as fc
on fc.campaign_id=fabd.campaign_id
union all
select gabd.ad_date, gabd.campaign_name, gabd.reach
from public.google_ads_basic_daily as gabd),
monthly_reach as (select campaign_name,
date_trunc('month', ad_date) as ad_month,
sum(reach) as total_reach
from f_g
group by campaign_name, ad_month),
growth_calc as (select campaign_name, to_char(ad_month, 'YYYY-MM-DD') as ad_month,
total_reach,
lag(total_reach, 1) over (partition by campaign_name order by ad_month) as prev_total_reach,
(total_reach-lag(total_reach, 1) over (partition by campaign_name order by ad_month))/nullif(lag(total_reach, 1) over (partition by campaign_name order by ad_month), 0) as reach_growth
from monthly_reach)
select *
from growth_calc
where total_reach>0
and prev_total_reach > 0
order by reach_growth desc
limit 1;

with f_g as (select fabd.ad_date, fa.adset_name
from homeworks.facebook_ads_basic_daily as fabd
left join facebook_adset as fa
on fa.adset_id=fabd.adset_id
union all
select gabd.ad_date, gabd.adset_name
from public.google_ads_basic_daily as gabd), 
num_days as (select adset_name, ad_date, 
row_number() over (partition by adset_name order by ad_date) as row_num
from f_g), 
islands as (select adset_name, ad_date,
(ad_date - cast(row_num || 'days' as interval)) as islands_id
from num_days)
select adset_name, min(ad_date) as start_date, max(ad_date) as end_date, 
(max(ad_date) - min(ad_date)) + 1 as duration_days
from islands
group by adset_name, islands_id
order by duration_days desc 
limit 1;

