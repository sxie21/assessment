with date_series as(
    select date_trunc('day', generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'))::date as dt_report -- in case there is no data on a particular day
),
grouped_trades as (
    select
        ds.dt_report,
        u.login_hash,
        u.server_hash,
        t.symbol,
        u.currency,
        sum(case when t.close_time <= dt_report and t.close_time >= (ds.dt_report - interval '7 days') then volume else 0 end) as sum_volume_prev_7d,
        sum(case when t.close_time <= dt_report and t.close_time >= (ds.dt_report - interval '7 days') then 1 else 0 end) as trade_cnt_prev_7d,
        sum(case when t.close_time <= dt_report then volume else 0 end) as sum_volume_prev_all,
        sum(case when t.close_time <= dt_report and date_trunc('month', t.close_time) = '2020-08-01' then volume else 0 end) as sum_volume_2020_08,
        min(close_time)::timestamp as date_first_trade
    from 
        date_series ds 
        left join (select distinct login_hash, server_hash, currency from users where enable = 1) u on true -- remove duplicated users 
        left join trades t on u.login_hash = t.login_hash and u.server_hash = t.server_hash and t.close_time::date <= ds.dt_report
    where   --filters for quality issues
        t.contractsize is not null
        and t.volume > 0
        and symbol ~ '^[A-Z0-9]+$'
        and open_time < close_time
    group by 
        ds.dt_report,u.login_hash,u.server_hash,t.symbol,u.currency
)

select 
    dt_report,
    login_hash,
    server_hash,
    symbol,
    currency,
    sum_volume_prev_7d,
    sum_volume_prev_all,
    dense_rank() over (partition by login_hash,symbol order by sum_volume_prev_7d desc) as rank_volume_symbol_prev_7d,
    dense_rank() over (partition by login_hash order by trade_cnt_prev_7d desc) as rank_count_prev_7d, 
    sum_volume_2020_08,
    date_first_trade,
    row_number() over (order by dt_report,login_hash,server_hash,symbol) as row_number
from 
    grouped_trades
order by 
    row_number desc