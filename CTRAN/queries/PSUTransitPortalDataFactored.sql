SELECT
    service_date,
    vehicle_number,
    leave_time,
    train,
    route_number,
    --direction, -- removed SJRS per conversation with PSU 2022-11-02
    directional, --2022-02-03 SMB added the explicit trip directional
    service_key,
    trip_number,
    stop_time,
    arrive_time,
    dwell,
    location_id,
    door,
    lift,
    ons,
    offs,
    /* old apc factoring logic removed 9.5.2023 SJRS. Logic not consistent with NTD compliance since mar 2022 INIT upgrade.
    case 
        when qualified_apc = 1 then ons
        when sum(qualified_apc) over (partition by service_date, route_number, location_id) = 0 then 0
        else round( ( sum(ons * qualified_apc) over (partition by service_date, route_number, location_id) ) /  ( sum(qualified_apc) over (partition by service_date, route_number, location_id) ), 0)
    end as ons, 
    case 
        when qualified_apc = 1 then offs
        when sum(qualified_apc) over (partition by service_date, route_number, location_id) = 0 then 0
        else round( ( sum(offs * qualified_apc) over (partition by service_date, route_number, location_id) ) /  ( sum(qualified_apc) over (partition by service_date, route_number, location_id) ), 0)
    end as offs, 
    */
	-- qualified_apc,
    estimated_load,
    maximum_speed,
    round(train_mileage,2) train_mileage,
    round(pattern_distance,2) pattern_distance,
    round(location_distance,2) location_distance,
    round(x_coordinate,5) x_coordinate,
    round(y_coordinate,5) y_coordinate,
    data_source,
    schedule_status,
    trip_id,
    time_period
FROM
(
select distinct stop.opd_date as service_date,
    trip.vehicle_id as vehicle_number,
    stop.act_dep_time as leave_time,
    showtime_hhmm(stop.act_dep_time) as formatted_time,
   -- stop.event_no as event_no_stop,
    trip.block_name as train,
    --stop.employee_id as badge,                                                                    
    trip.route_number,
    case when trip.direction in ('EB','NB') then 0
        when trip.direction in ('WB','SB') then 1
        else null end as direction,
    trip.direction as directional, --2022-02-03 SMB added the explicit trip directional
    case when trip.week_day_name = 'Saturday' then 'S'
        when trip.week_day_name = 'Sunday' then 'U'
        when (trip.daytype_name = 'Sun / Hol' and trip.week_day_name != 'Sunday') then 'X'
        when trip.day_of_week in (1,2,3,4,5) then 'W'
        else null end as service_key,
    trip.event_no as trip_number,
    trip.time_period,
    stop.nom_arr_time as stop_time,
    stop.act_arr_time as arrive_time,
    stop.act_dwelltime as dwell,
    stop.point_id as location_id,                                                                   -- 2021.04.22: Updated by D. ONeil (Emp. #2833).  Previously used stop.point_code as location_id.  GTFS stop info uses point_id value.  This query should output GTFS values for reference by PORTAL.
    case when stop.point_action like '%O%' then 1 else 0 end as door,                               -- door flag. Documentation says count # of times doors were opened, but we do not have that data    
    case when stop.wheelchair_count is null then 0 else stop.wheelchair_count end as lift,
    stop.psngr_in as ons,
    stop.psngr_out as offs,                                                                     -- documentation says use raw --updated 12/3/2021 using balanced boardings to align with factored trips
    stop.trip_bal_boardings,
    stop.trip_bal_alightings,
    stop.trip_raw_boardings,
    stop.trip_raw_alightings,
    sum(stop.ACT_PM_SMB) over (Partition by stop.opd_date, stop.event_trip) as trip_pax_miles,
    /*  old apc factoring logic removed 9.5.2023 SJRS. Logic not consistent with NTD compliance since mar 2022 INIT upgrade.
    case 
        when (sum(stop.ACT_PM_SMB) over (Partition by stop.opd_date, stop.event_trip)) > 0 AND (stop.trip_bal_alightings > 0 OR stop.trip_bal_boardings > 0) then 1
        when stop.trip_bal_boardings = 0 and stop.trip_bal_boardings = 0 and stop.trip_bal_boardings = 0 and stop.trip_bal_boardings = 0 then 1
        else 0
    end as qualified_apc,
    */
    stop.load as estimated_load,
    null as maximum_speed,                                                                          -- currently inaccessible
    stop.train_mileage,
    round((trip.nom_meter_pattern-stop.dist_left)*39.37008/ 12*.000189394,4) as pattern_distance,
    stop.location_distance,
    stop.gps_longitude as x_coordinate,
    stop.gps_latitude as y_coordinate,
    case when stop.point_code is null then 1 else 2 end as data_source,
    case when trip.trip_role='S' then
        (case when trip.point_id_dep=stop.point_id then 5
            when trip.point_id_end=stop.point_id then 6
            when stop.point_role like '%T%' then 4
            else 2 end)
        else 0 end as schedule_status,
    trip.time_grp_id as trip_id --  updated event_no => time_grp_id SJRS per conversation with PSU 2022-11-02
from (
    select s.opd_date, s.act_dep_time, s.nom_arr_time, s.act_arr_time, s.act_dwelltime, s.event_trip,
            s.point_action, s.load, s.psngr_in, s.psngr_out,
            s.point_code, s.point_id, s.point_role, s.event_no,
            (sum(s.nom_meters) over (partition by s.event_trip order by s.event_no desc)) as dist_left,
        e.employee_id,
        p.wheelchair_count,
        v.gps_latitude, v.gps_longitude,
            round(((max(v.meters) over (partition by s.event_trip order by s.event_no)-
                (min(v.meters) over (partition by s.event_trip order by s.event_no))))*39.37008/ 12*.000189394,4) 
                as train_mileage,                                                                   -- validated to be 0 at first stop. Cannot use nom_pattern_point.dist_to_start because of overlaps at point ids 1095 and 999
        round((sqrt(
            (power((n.gps_latitude-v.gps_latitude)*69.0480145683935*5280,2)) +                      -- multiplied by length in miles of a degree of latitude in our area, then by the number of feet in a mile
            (power((n.gps_longitude-v.gps_longitude)*48.9928411637813*5280,2)))),6)                 -- multiplied by average length in miles of a degree of longitude in our area, then by the number of feet in a mile
            as location_distance,
        sum(s.psngr_in) over(Partition by s.opd_date, s.event_trip) as trip_bal_boardings,
        sum(s.psngr_out) over(Partition by s.opd_date, s.event_trip) as trip_bal_alightings,
        sum(s.psngr_in_org) over(Partition by s.opd_date, s.event_trip) as trip_raw_boardings,
        sum(s.psngr_out_org) over(Partition by s.opd_date, s.event_trip) as trip_raw_alightings,
         ROUND( ( ( (( (lead(v.meters) over (partition by s.event_trip order by s.event_no) ) - v.METERS) * 39.37008/*conversion to inches*/) / 12/*conversion to feet*/) *.000189394/*conversion to miles*/),4) * S.LOAD  AS ACT_PM_SMB

    from opd_v_stop s
    inner join veh_stop_employee e
        on e.event_no_trip = s.event_trip
        and e.act_dep_time=s.act_dep_time
        --and e.opd_date between :start_date and :end_date
        and e.opd_date = TO_DATE(:query_date, 'YYYY-MM-DD') --TO_DATE(SYSDATE) - 5 
    inner join veh_stop_passenger p
        on s.event_no=p.event_no
        --and s.opd_date between :start_date and :end_date
        and s.opd_date = TO_DATE(:query_date, 'YYYY-MM-DD') --TO_DATE(SYSDATE) - 5 
    inner join veh_stop v
        on s.event_no=v.event_no
        and v.event_no=p.event_no
        --and v.opd_date between :start_date and :end_date
        and v.opd_date = TO_DATE(:query_date, 'YYYY-MM-DD') --TO_DATE(SYSDATE) - 5 
    inner join nom_point n
        on n.point_id=s.point_id
        and s.opd_date between n.valid_from and n.valid_until
    order by s.event_trip, s.event_no        -- the time difference between including this or not is negligible (ran one second slower when it was removed
        ) stop
inner join (
    select t.opd_date, t.event_no, t.time_grp_id, to_number(v.short_name) as vehicle_id, t.daytype_name, t.week_day_name, t.trip_role, t.point_id_dep,
        t.point_id_end, t.block_name, t.day_of_week, t.direction, t.nom_meter_pattern,
        l.short_name as route_number,
        case
                when mod(t.nom_dep_time,86400) between 14400 and 35999 then 'AM'  -- from 4:00:00   am to 9:59:59  am
                when mod(t.nom_dep_time,86400) between 36000 and 53999 then 'MID' -- from 10:00:00  am to 2:59:59  pm
                when mod(t.nom_dep_time,86400) between 54000 and 68399 then 'PM'  -- from 3:00:00   pm to 6:59:59  pm
                when mod(t.nom_dep_time,86400) between 68400 and 86399 then 'EVE' -- from 7:00:00   pm to 11:59:59 pm
                when mod(t.nom_dep_time,86400) between 0 and 10800 then 'EVE'     -- from 12:00:00  am to 3:00:00  am (continuation of previous line, to extend period to next day)
                else 'NA'
            end as time_period
    from opd_v_trip_tpi t
    inner join nom_line l
        on t.line_id=l.line_id
        and t.opd_date between l.valid_from and l.valid_until
    inner join nom_vehicle v
        on t.vehicle_id=v.vehicle_id
        and t.opd_date between v.valid_from and v.valid_until
    where 1=1
        and v.short_name NOT IN ('1001','1002','1003')
        and t.opd_date = TO_DATE(:query_date, 'YYYY-MM-DD') --TO_DATE(SYSDATE) - 5 
        and t.trip_role = 'S'
        ) trip
    on stop.opd_date=trip.opd_date and stop.event_trip=trip.event_no
)
