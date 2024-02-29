declare @startdate datetime;

set
@startdate = ?;

with clock_in_out as (
select
	EMP_empl_no,
	case
		when datetime_in is null then dateadd(hour, -8.5, datetime_out)
		else datetime_in
	end datetime_in,
	case
		when datetime_out is null then dateadd(hour, 8.5, datetime_in)
		else datetime_out
	end datetime_out,
	case
		when datetime_in is null then 1
		else 0
	end as missed_clockin,
	case
		when datetime_out is null then 1
		else 0
	end as missed_clockout
from
	EMP_CLOCKIN
where
	(datetime_in between @startdate and dateadd(day, 1, @startdate))
	OR (datetime_in is null
		and datetime_out between DATEADD(hour, 8.5, @startdate) and dateadd(day, 1, DATEADD(hour, 8.5, @startdate)))
),
labor as (
select
	lm.EMP_empl_no,
	lm.lab_start_datetime datetime_in ,
	lm.lab_end_datetime datetime_out,
	case
				when lm.indirect_flag in ('Y') then left(emp.DEPT_dept_code,
				2)
		else eq.CPY_company_code
	end as [mode],
			case
				when lm.indirect_flag in ('Y') then right(emp.DEPT_dept_code,
				3)
		when lm.LOC_work_order_loc in ('MAIN1')
			and eq.CPY_company_code = '99' then '247'
			when lm.LOC_work_order_loc in ('MAIN1')
				and eq.CPY_company_code != '99' then tsk.benchmark_task_code
				else eq.TAX_tax_code
			end as [cost center]
		from
			clock_in_out ec
		join LAB_MAIN lm on
			lm.EMP_empl_no = ec.EMP_empl_no
			and lm.lab_date BETWEEN ec.datetime_in and ec.datetime_out
			and lm.lab_start_datetime >= @startdate
			and lm.lab_end_datetime >= @startdate
		left join EQ_MAIN eq on
					eq.EQ_equip_no = lm.EQ_equip_no
		left join DES_MAIN tsk on
					lm.TASK_task_code = tsk.TASK_task_code
		left join EMP_MAIN emp on
					emp.EMP_empl_no = lm.EMP_empl_no
),
all_time as (
select
	*
from
	(
	select
		top 1 with ties
		cio.emp_empl_no,
		datetime_in,
		null as datetime_out,
		 left(emp.DEPT_dept_code,
				2) as mode,
		 right(emp.DEPT_dept_code,
				3) as [cost center]
	from
		clock_in_out cio
	join EMP_MAIN emp on
					emp.EMP_empl_no = cio.EMP_empl_no
	order by
		row_number() over (partition by cio.emp_empl_no
	order by
		cio.datetime_in)) a
union

select
	*
from
	labor
where
	not (datepart(dd, datetime_in) != datepart(dd, datetime_out)
		and datepart(dw, datetime_out) = 1)
UNION 

select
emp_empl_no,
datetime_in,
convert (varchar(10), datetime_out, 120 ) ,
mode,
[cost center]
from
	labor
where
(datepart(dd, datetime_in) != datepart(dd, datetime_out)
	and datepart(dw, datetime_out) = 1)
union

select
emp_empl_no,
convert (varchar(10), datetime_out, 120 ) ,
datetime_out,
mode,
[cost center]
from
	labor
where
(datepart(dd, datetime_in) != datepart(dd, datetime_out)
	and datepart(dw, datetime_out) = 1)
union
select
	*
from
	(
select
top 1 with ties
		cio.emp_empl_no,
		null as datetime_in,
		datetime_out,
		left(emp.DEPT_dept_code,
				2) as mode,
		 right(emp.DEPT_dept_code,
				3) as [cost center]
from
		clock_in_out cio
join EMP_MAIN emp on
					emp.EMP_empl_no = cio.EMP_empl_no
order by
		row_number() over (partition by cio.emp_empl_no
order by
		datetime_out desc)) b
),
complete_clock_events as
(
select
	emp_empl_no,
	case
		when datetime_in is null then lag(datetime_out) over (partition by emp_empl_no
	order by
		datetime_out,
		datetime_in desc)
		else datetime_in
	end as datetime_in_adj,
	case
		when datetime_out is null then lead(datetime_in) over (partition by emp_empl_no
	order by
		datetime_in,
		datetime_out)
		else datetime_out
	end as datetime_out_adj,
	--	datetime_in,
	--	datetime_out,
	mode,
	[cost center]
from
	all_time
)
select
	row_number() over (
order by
	EMP_empl_no,
	datetime_in_adj) as [Spreadsheet Key],
	EMP_empl_no [Employee],
	null as [Time Block Reference ID],
	null as [Worker Time Block],
	null as [Delete Time Block],
	'Worked_Time_In_and_Out' as [Time Entry Code],
	null as [Project],
	null as [Project Task],
	cast(@startdate as date) as [Date],
	null as [Quantity],	
		convert(nvarchar,
		convert(datetime2(0),
		datetime_in_adj
	),
		126)
	as [In Date Time],
	case
			when datetime_out_adj = lead(datetime_in_adj) over (partition by EMP_empl_no
		order by
			datetime_out_adj) then 
		convert(nvarchar,
			DATEADD(minute,-1, convert(datetime2(0),
		datetime_out_adj
		) ),
			126)
			else
		convert(nvarchar,
			convert(datetime2(0),
			datetime_out_adj
		),
			126)
		end
		as [Out Date Time],
	'Out' as [Out Reason],
	null as [Position],
	null as [Do Not Bill],
	null as [Project Role],	
	null as [Override Rate],
	null as [Allocation Pool],	
	null as [Appropriation],
	null as [Business Unit],
	[cost center] as [Cost Center],
	CONCAT('Mode_', [mode]) [mode],
	null as [NTD_Tag],
	null as [Custom Organization 03],
	null as [Custom Organization 04],
	null as [Custom Organization 05],
	null as [Custom Organization 06],
	null as [Custom Organization 07],
	null as [Custom Organization 08],
	null as [Custom Organization 09],
	null as [Custom Organization 10],
	null as [Custom Worktag 01],
	null as [Custom Worktag 02],
	null as [Custom Worktag 03],
	null as [Custom Worktag 04],
	null as [Custom Worktag 05],
	null as [Custom Worktag 06],
	null as [Custom Worktag 07],
	null as [Custom Worktag 08],
	null as [Custom Worktag 09],
	null as [Custom Worktag 10],
	null as [Custom Worktag 11],
	null as [Custom Worktag 12],
	null as [Custom Worktag 13],
	null as [Custom Worktag 14],
	null as [Custom Worktag 15],
	'FUND100' as [Fund]
from
	complete_clock_events
where
	datetime_in_adj != datetime_out_adj
order by
	EMP_empl_no,
	datetime_in_adj ;
