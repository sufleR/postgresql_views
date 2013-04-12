
--show size af all databases
create view show_db_size as  SELECT pg_database.datname, pg_size_pretty(pg_database_size(pg_database.datname)) AS size FROM pg_database;



--show bloat of tables - how many space is wasted
create view show_bloat as  SELECT current_database() AS current_database, sml.schemaname, sml.tablename, round(
        CASE
            WHEN sml.otta = 0::double precision THEN 0.0
            ELSE sml.relpages::numeric / sml.otta::numeric
        END, 1) AS tbloat, 
        CASE
            WHEN sml.relpages::double precision < sml.otta THEN 0::numeric
            ELSE sml.bs * (sml.relpages::double precision - sml.otta)::bigint::numeric
        END AS wastedbytes, sml.iname, round(
        CASE
            WHEN sml.iotta = 0::double precision OR sml.ipages = 0 THEN 0.0
            ELSE sml.ipages::numeric / sml.iotta::numeric
        END, 1) AS ibloat, 
        CASE
            WHEN sml.ipages::double precision < sml.iotta THEN 0::double precision
            ELSE sml.bs::double precision * (sml.ipages::double precision - sml.iotta)
        END AS wastedibytes
   FROM ( SELECT rs.schemaname, rs.tablename, cc.reltuples, cc.relpages, rs.bs, ceil(cc.reltuples * ((rs.datahdr + rs.ma::numeric - 
                CASE
                    WHEN (rs.datahdr % rs.ma::numeric) = 0::numeric THEN rs.ma::numeric
                    ELSE rs.datahdr % rs.ma::numeric
                END)::double precision + rs.nullhdr2 + 4::double precision) / (rs.bs::double precision - 20::double precision)) AS otta, COALESCE(c2.relname, '?'::name) AS iname, COALESCE(c2.reltuples, 0::real) AS ituples, COALESCE(c2.relpages, 0) AS ipages, COALESCE(ceil(c2.reltuples * (rs.datahdr - 12::numeric)::double precision / (rs.bs::double precision - 20::double precision)), 0::double precision) AS iotta
           FROM ( SELECT foo.ma, foo.bs, foo.schemaname, foo.tablename, (foo.datawidth + (foo.hdr + foo.ma - 
                        CASE
                            WHEN (foo.hdr % foo.ma) = 0 THEN foo.ma
                            ELSE foo.hdr % foo.ma
                        END)::double precision)::numeric AS datahdr, foo.maxfracsum * (foo.nullhdr + foo.ma - 
                        CASE
                            WHEN (foo.nullhdr % foo.ma::bigint) = 0 THEN foo.ma::bigint
                            ELSE foo.nullhdr % foo.ma::bigint
                        END)::double precision AS nullhdr2
                   FROM ( SELECT s.schemaname, s.tablename, constants.hdr, constants.ma, constants.bs, sum((1::double precision - s.null_frac) * s.avg_width::double precision) AS datawidth, max(s.null_frac) AS maxfracsum, constants.hdr + (( SELECT 1 + count(*) / 8
                                   FROM pg_stats s2
                                  WHERE s2.null_frac <> 0::double precision AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename)) AS nullhdr
                           FROM pg_stats s, ( SELECT ( SELECT current_setting('block_size'::text)::numeric AS current_setting) AS bs, 
                                        CASE
                                            WHEN "substring"(foo.v, 12, 3) = ANY (ARRAY['8.0'::text, '8.1'::text, '8.2'::text]) THEN 27
                                            ELSE 23
                                        END AS hdr, 
                                        CASE
                                            WHEN foo.v ~ 'mingw32'::text THEN 8
                                            ELSE 4
                                        END AS ma
                                   FROM ( SELECT version() AS v) foo) constants
                          GROUP BY s.schemaname, s.tablename, constants.hdr, constants.ma, constants.bs) foo) rs
      JOIN pg_class cc ON cc.relname = rs.tablename
   JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'::name
   LEFT JOIN pg_index i ON i.indrelid = cc.oid
   LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid) sml
  ORDER BY 
        CASE
            WHEN sml.relpages::double precision < sml.otta THEN 0::numeric
            ELSE sml.bs * (sml.relpages::double precision - sml.otta)::bigint::numeric
        END DESC;


-- show connections to databases
create view show_connections as  SELECT pg_stat_activity.client_addr, pg_stat_activity.datname, pg_stat_activity.usename, count(*) AS count
   FROM pg_stat_activity
  GROUP BY pg_stat_activity.client_addr, pg_stat_activity.datname, pg_stat_activity.usename;

-- show size of indexes
create view show_indexes_size as  SELECT pg_size_pretty(pg_relation_size(((pg_indexes.schemaname::text || '.'::text) || pg_indexes.indexname::text)::regclass)) AS size, (pg_indexes.schemaname::text || '.'::text) || pg_indexes.tablename::text AS tablename, pg_indexes.indexname
   FROM pg_indexes
  WHERE pg_indexes.schemaname <> 'pg_catalog'::name AND pg_indexes.schemaname <> 'information_schema'::name
  ORDER BY pg_relation_size(((pg_indexes.schemaname::text || '.'::text) || pg_indexes.indexname::text)::regclass) DESC;

--shows size of tables

create view show_tables_size as SELECT pg_size_pretty(pg_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass)) AS size, pg_tables.tablename, pg_tables.schemaname
   FROM pg_tables
  WHERE pg_tables.schemaname <> 'pg_catalog'::name AND pg_tables.schemaname <> 'information_schema'::name
  ORDER BY pg_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass) DESC;

--shows size stats
create view show_sizes as  SELECT pg_tables.schemaname, pg_tables.tablename, pg_size_pretty(pg_total_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass)) AS total_relation_size, pg_size_pretty(pg_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass)) AS table_size, pg_size_pretty(pg_total_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass) - pg_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass) - COALESCE(( SELECT sum(pg_relation_size(((pg_indexes.schemaname::text || '.'::text) || pg_indexes.indexname::text)::regclass))::bigint AS sum
           FROM pg_indexes
          WHERE pg_indexes.schemaname = pg_tables.schemaname AND pg_indexes.tablename = pg_tables.tablename), 0::bigint)) AS pg_toast_size, pg_size_pretty(COALESCE(( SELECT sum(pg_relation_size(((pg_indexes.schemaname::text || '.'::text) || pg_indexes.indexname::text)::regclass))::bigint AS sum
           FROM pg_indexes
          WHERE pg_indexes.schemaname = pg_tables.schemaname AND pg_indexes.tablename = pg_tables.tablename), 0::bigint)) AS indexes_size
   FROM pg_tables
  WHERE pg_tables.schemaname <> 'pg_catalog'::name AND pg_tables.schemaname <> 'information_schema'::name
  ORDER BY pg_total_relation_size(((pg_tables.schemaname::text || '.'::text) || pg_tables.tablename::text)::regclass) DESC;



-- shows aproximated number of rows in table
create view show_row_count as  SELECT r.reltuples::bigint AS tuples, r.relpages::bigint AS pages, r.relname, n.nspname AS schemaname
   FROM pg_class r
   JOIN pg_namespace n ON r.relnamespace = n.oid;

-- very usefull function to change owner for database obcjects

CREATE FUNCTION set_db_owner(in_username name, in_really boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
    tmp record;
    sql text;
begin
    /* database */
    SELECT INTO tmp r.rolname from pg_catalog.pg_database d join pg_catalog.pg_roles r ON d.datdba = r.oid where d.datname = current_database();
    if tmp.rolname <> in_username then
        sql := 'ALTER DATABASE ' || current_database() || ' OWNER TO ' || in_username;
        raise notice 'SQL to run: [%]', sql;
        if in_really then 
            execute sql;
        end if;
    end if;
    /* schemas */
    for tmp in
        select n.nspname, r.rolname
        from pg_catalog.pg_namespace n join pg_catalog.pg_roles r on n.nspowner=r.oid
        where n.nspname != 'information_schema'
        and n.nspname != 'public'
        and n.nspname !~ '^pg_'
        and n.nspname !~ '^_'
        and r.rolname != in_username
        order by n.nspname
    loop
        sql := 'ALTER SCHEMA ' || tmp.nspname || ' OWNER TO ' || in_username;
        raise notice 'SQL to run: [%]', sql;
        if in_really then 
            execute sql;
        end if;
    end loop;
    /* tables */
    for tmp in
        select schemaname, tablename from pg_tables
        where schemaname != 'information_schema'
        and schemaname !~ '^pg_'
        and schemaname !~ '^_'
        and tableowner != in_username
        order by schemaname, tablename
    loop
        sql := 'ALTER TABLE ' || tmp.schemaname || '."' || tmp.tablename || '" OWNER TO ' || in_username;
        raise notice 'SQL to run: [%]', sql;
        if in_really then 
            execute sql;
        end if;
    end loop;
    /* views */
    for tmp in
        select schemaname, viewname from pg_views
        where schemaname != 'information_schema'
        and schemaname !~ '^pg_'
        and schemaname !~ '^_'
        and viewowner != in_username
        order by schemaname, viewname
    loop
        sql := 'ALTER /*view*/ TABLE ' || tmp.schemaname || '."' || tmp.viewname || '" OWNER TO ' || in_username;
        raise notice 'SQL to run: [%]', sql;
        if in_really then 
            execute sql;
        end if;
    end loop;
    /* functions */
    for tmp in
        select n.nspname as schemaname, p.proname as functionname, p.proargtypes
        from pg_catalog.pg_proc p
        join pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        join pg_catalog.pg_roles r ON r.oid = p.proowner
        join pg_catalog.pg_language l ON l.oid = p.prolang
        where l.lanname not in ( 'internal', 'c' )
        and n.nspname != 'information_schema'
        and n.nspname !~ '^pg_'
        and n.nspname !~ '^_'
        and r.rolname != in_username
        order by n.nspname, p.proname
    loop
        sql := 'ALTER FUNCTION ' || tmp.schemaname || '."' || tmp.functionname || '"(' || pg_catalog.oidvectortypes(tmp.proargtypes) || ') OWNER TO ' || in_username;
        raise notice 'SQL to run: [%]', sql;
        if in_really then
            execute sql;
        end if;
    end loop;

    return;
end;
$$;



