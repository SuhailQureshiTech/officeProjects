-- pioneer_schema.pioneer_attendance definition

-- Drop table

-- DROP TABLE pioneer_attendance;

CREATE TABLE pioneer_attendance (
	transaction_id int4 NOT NULL,
	employee_id varchar(50) NOT NULL,
	punch_datetime timestamp NOT NULL,
	device_no varchar(50) NOT NULL,
	status varchar(50) NULL,
	record_datetime timestamp NULL DEFAULT now()
);

-- pioneer_schema.vw_pioneer_attendance_rec source

CREATE OR REPLACE VIEW pioneer_schema.vw_pioneer_attendance_rec
AS SELECT a.tmid,
    a.cardno,
    replace(a.date1::text, '-'::text, ''::text) AS date1,
    a.p_day,
    a.ismanual,
    a.machine,
    replace(min(a.timein)::text, ':'::text, ''::text) AS "time",
    replace(max(a.timeout)::text, ':'::text, ''::text) AS inout1,
    a.status AS flag
   FROM ( SELECT concat(TRIM(BOTH FROM replace(x.punch_datetime::date::text, '-'::text, ''::text))) AS tmid,
            x.employee_id AS cardno,
            x.punch_datetime::date AS date1,
            'N'::text AS p_day,
            'N'::text AS ismanual,
            'A'::text AS status,
            x.device_no AS machine,
            x.punch_datetime::time without time zone AS timein,
            x.punch_datetime::time without time zone AS timeout
           FROM pioneer_schema.pioneer_attendance x
          WHERE 1 = 1 AND x.employee_id::text !~~ '00%'::text) a
  GROUP BY a.tmid, a.cardno, (replace(a.date1::text, '-'::text, ''::text)), a.p_day, a.ismanual, a.machine, a.status;