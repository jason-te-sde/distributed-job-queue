-- The query that the sampler ran every 5 s (psql -f, \watch 5) during the E5 follow-up runs.
\pset format unaligned
\pset tuples_only on
select json_build_object(
  'ts', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS'),
  'heap_bytes', pg_relation_size('jobqueue.jobs'),
  'total_bytes', pg_total_relation_size('jobqueue.jobs'),
  'indexes', (select json_object_agg(c.relname, pg_relation_size(i.indexrelid)) from pg_index i join pg_class c on c.oid = i.indexrelid where i.indrelid = 'jobqueue.jobs'::regclass),
  'stat', (select row_to_json(t) from (select n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_tup_del, autovacuum_count, to_char(last_autovacuum, 'HH24:MI:SS.MS') as last_autovacuum, autoanalyze_count from pg_stat_user_tables where relid = 'jobqueue.jobs'::regclass) t),
  'progress', (select json_agg(row_to_json(p)) from pg_stat_progress_vacuum p where p.relid = 'jobqueue.jobs'::regclass),
  'av_workers', (select json_agg(json_build_object('db', datname, 'wait_event_type', wait_event_type, 'wait_event', wait_event, 'state', state, 'running_s', round(extract(epoch from now() - backend_start)::numeric, 1), 'query', left(query, 90))) from pg_stat_activity where backend_type = 'autovacuum worker')
)
\watch 5
