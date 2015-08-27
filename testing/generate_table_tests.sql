CREATE TYPE tests.t_test AS (
  test_schema TEXT,
  test_name   TEXT,
  the_test    TEXT
);

CREATE OR REPLACE FUNCTION tests.generate_table_tests(p_schema name)
RETURNS SET OF tests.t_test LANGUAGE plpgsql AS $$
DECLARE
  v_script text;
  v_table_name name;
  v_table_oid oid;
  v_column_name name;
  v_primary_array text;
  v_con_name name;
  v_con_cols text;
  v_con_type char;
  v_test tests.t_test;
BEGIN
    FOR v_table_oid, v_table_name IN 
     SELECT r.oid, r.relname 
     FROM pg_class r 
     JOIN pg_namespace n ON (r.relnamespace = n.oid) 
     WHERE n.nspname = p_schema AND r.relkind = 'r' 
    LOOP
        v_script := '';
        v_script := v_script || 'CREATE OR REPLACE FUNCTION ' || quote_ident(p_schema) || '.' || quote_ident('test_' || v_table_name) || E'()\n';
        v_script := v_script || E'RETURNS SETOF TEXT LANGUAGE plpgsql AS \$\$\n';
        v_script := v_script || E'BEGIN\n';
        /* Table exists test */
        v_script := v_script || '  RETURN NEXT has_table(' || quote_literal(p_schema) || '::name, ' || 
                    quote_literal(v_table_name) || '::name, ' || 
                    quote_literal('Table ' || p_schema || '.' || v_table_name || ' exists') || E');\n';
        /* Column tests */
        FOR v_column_name IN 
         SELECT a.attname 
         FROM pg_attribute a 
         WHERE a.attrelid = v_table_oid AND a.attnum > 0 
         ORDER BY a.attnum ASC 
        LOOP
          v_script := v_script || '  RETURN NEXT has_column(' || quote_literal(p_schema) || '::name, ' || 
                      quote_literal(v_table_name) || '::name, ' || quote_literal(v_column_name) || '::name, ' || 
                      quote_literal('Column ' || v_column_name || ' exists on table ' || p_schema || '.' || v_table_name) || E');\n';
        END LOOP;
        /* Primary Key */
        SELECT string_agg(quote_literal(q.attname), ', ') INTO v_primary_array 
        FROM (
         SELECT * 
         FROM pg_attribute a 
         JOIN pg_constraint c ON (ARRAY[a.attnum] <@ c.conkey AND a.attrelid = c.conrelid) 
         WHERE c.conrelid = v_table_oid AND c.contype = 'p' 
         ORDER BY a.attnum
        ) q;
        IF (FOUND AND v_primary_array IS NOT NULL) THEN
          v_script := v_script || '  RETURN NEXT col_is_pk(' || quote_literal(p_schema) || '::name, ' || 
                      quote_literal(v_table_name) || '::name, ARRAY[' || v_primary_array || '], ' || 
                      quote_literal('Columns ' || v_primary_array || ' are the primary key of table ' || p_schema || '.' || v_table_name) || E');\n';
        END IF;
        /* Unique and Foreign Keys */
        FOR v_con_name, v_con_cols, v_con_type IN 
         SELECT c.conname, q.atts, c.contype
         FROM pg_constraint c, 
         LATERAL (
          SELECT string_agg(quote_literal(qq.attname), ', ') AS atts
          FROM (
           SELECT i.v AS idx, (SELECT a.attname FROM pg_attribute a WHERE a.attrelid = c.conrelid AND a.attnum = c.conkey[i.v]) AS attname
           FROM generate_series(array_lower(c.conkey, 1), array_upper(c.conkey, 1)) i(v)
           ORDER BY i.v ASC
          ) qq 
         ) q 
         WHERE c.conrelid = v_table_oid AND (c.contype = 'u' OR c.contype = 'f')
        LOOP
          -- Unique
          IF (v_con_type = 'u') THEN
            v_script := v_script || '  RETURN NEXT col_is_unique(' || quote_literal(p_schema) || '::name, ' || 
                quote_literal(v_table_name) || '::name, ARRAY[' || v_con_cols || '], ' || 
                quote_literal('Columns ' || v_con_cols || ' are unique on table ' || p_schema || '.' || v_table_name) || E');\n';
          END IF;
          -- Foreign
          IF (v_con_type = 'f') THEN
            v_script := v_script || '  RETURN NEXT col_is_fk(' || quote_literal(p_schema) || '::name, ' || 
                        quote_literal(v_table_name) || '::name, ARRAY[' || v_con_cols || '], ' || 
                        quote_literal('Columns ' || v_con_cols || ' are foriegn keys on table ' || p_schema || '.' || v_table_name) || E');\n';
          END IF;
        END LOOP;
        /* */
        v_script := v_script || E'END;\n';
        v_script := v_script || E'\$\$;\n\n';
        /* return */
        v_test.test_schema := p_schema::TEXT;
        v_test.test_name   := 'test_' || v_table_name;
        v_test.the_test    := v_script;
        RETURN NEXT v_test;
    END LOOP;
    RETURN;
END;
$$;
