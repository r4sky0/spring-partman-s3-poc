-- Register events with pg_partman. p_type='range' is the v5 syntax for native
-- range partitioning. premake=4 keeps four days of partitions ready ahead of now.
SELECT partman.create_parent(
    p_parent_table => 'public.events',
    p_control      => 'created_at',
    p_type         => 'range',
    p_interval     => '1 day',
    p_premake      => 4
);

-- Deliberately leave retention NULL: pg_partman's maintenance is allowed to
-- create future partitions but MUST NOT detach old ones — otherwise
-- partitions disappear from pg_inherits before our archive job has a chance
-- to export them. The Spring archive job owns the full retention lifecycle:
-- discover, export to S3, DETACH, DROP.
UPDATE partman.part_config
   SET infinite_time_partitions = true,
       automatic_maintenance    = 'on'
 WHERE parent_table = 'public.events';
