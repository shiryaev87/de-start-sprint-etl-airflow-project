create table if not exists mart.f_customer_retention (
    period_name text not null,                          -- 'weekly', может быть расширено в будущем
    period_id integer not null,                         -- номер недели или месяца
    item_id integer not null,                           -- идентификатор категории товара
    new_customers_count integer not null default 0,     -- количество новых клиентов
    returning_customers_count integer not null default 0, -- количество вернувшихся клиентов
    refunded_customer_count integer not null default 0, -- количество клиентов, оформивших возврат
    new_customers_revenue numeric(14, 2) not null default 0.00,    -- доход от новых клиентов
    returning_customers_revenue numeric(14, 2) not null default 0.00, -- доход от вернувшихся клиентов
    customers_refunded integer not null default 0,      -- количество возвратов клиентов
    primary key (period_name, period_id, item_id)
);

delete from mart.f_customer_retention ;
insert into mart.f_customer_retention (
    period_name,
    period_id,
    item_id,
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)
with sales_with_week as(
select 
        fs.customer_id,
        fs.item_id,
        fs.payment_amount,
        cal.week_of_year  as period_id,
        fs.status,
        case 
            when fs.payment_amount < 0 then true 
            else false 
        end as is_refund
    from mart.f_sales fs
    join mart.d_calendar cal on fs.date_id = cal.date_id
    --where cal.week_number = extract(week from current_date) 
    ),
    customer_stats as (
    select
    	customer_id,
    	item_id,
    	period_id,
    	count(*) as order_count,
    	sum(payment_amount) as total_revenue,
    	bool_or(is_refund ) as has_refund,
    	status
    	from sales_with_week 
    	group by customer_id ,item_id ,period_id ,status
    ),
    classified_customer as (
     select
		period_id,
		item_id,
		case 
			when order_count = 1 then 'new'
			when order_count > 1 then 'returning'
		end as customer_type,
		total_revenue,
		has_refund
		from customer_stats
		),
	aggregated as(
	select
		'weekly' as period_name,
		period_id,
		item_id,
		count(*) filter (where customer_type='new') as new_customers_count,
		count(*) filter (where customer_type='returning') as returning_customers_count,
		count(*) filter (where has_refund = true) as refunded_customers_count,
		sum(total_revenue) filter(where customer_type='new') as new_customers_revenue,
		sum(total_revenue) filter(where customer_type='returning') as returning_customers_revenue,		
		count(*) filter (where has_refund = true) as customers_refunded
		from classified_customer 
		group by period_id,item_id
		)
		select
		'weekly' as period_name,
		period_id,
		item_id,
		coalesce(new_customers_count,0) as new_customers_count,
    	coalesce(returning_customers_count,0) as returning_customers_count,
    	coalesce(refunded_customers_count,0) as refunded_customers_count,
    	coalesce(new_customers_revenue,0) as new_customers_revenue,
    	coalesce(returning_customers_revenue,0) as returning_customers_revenue,
    	coalesce(customers_refunded,0) as customers_refunded
    	from aggregated;
    
    
    