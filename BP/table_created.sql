SELECT  DATETIME(p.start_time,'Pacific/Auckland') AS start_date,
          p.user_email,
          p.query,
          p.job_type,
          p.statement_type,
          p.priority,
          p.state,
          p.total_slot_ms,
          p.error_result,
          p.destination_table
          FROM    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT p
          WHERE 1=1
          AND (p.destination_table.table_id = 'table_name'

           )
          ORDER BY 1 DESC;