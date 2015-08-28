CREATE TYPE tests.t_test AS (
  test_schema TEXT,
  test_name   TEXT,
  the_test    TEXT
);

CREATE OR REPLACE FUNCTION tests.generate_function_tests(p_schema name)
RETURNS SETOF tests.t_test LANGUAGE plpgsql AS $$
DECLARE
  v_script text;
  v_function_oid oid;
  v_function_name name;
  v_function_argtypes text;
  v_function_id text;
  v_test tests.t_test;
BEGIN
    v_script := '';
    -- Single function to check that schema functions exist
    v_script := v_script || 'CREATE OR REPLACE FUNCTION ' || quote_ident(p_schema) || '.' || quote_ident('test_' || p_schema || '_functions') || E'()\n';
    v_script := v_script || E'RETURNS SETOF TEXT LANGUAGE plpgsql AS \$\$\n';
    v_script := v_script || E'BEGIN\n';
    -- Loop through all functions
    FOR v_function_oid, v_function_name, v_function_argtypes, v_function_id IN 
     SELECT p.oid, p.proname, q.argtyps, pg_get_function_identity_arguments(p.oid)
     FROM pg_proc p
     JOIN pg_namespace n ON (p.pronamespace = n.oid),
     LATERAL (
      SELECT string_agg(quote_literal(qq.argtyp), ', ') AS argtyps
      FROM (
       SELECT i.v AS idx, (SELECT format_type(t.oid, NULL) FROM pg_type t WHERE t.oid = (p.proargtypes::oid[])[i.v]) AS argtyp
       FROM generate_series(array_lower(p.proargtypes::oid[], 1), array_upper(p.proargtypes::oid[], 1)) i(v)
       ORDER BY i.v ASC
      ) qq
     ) q
     WHERE n.nspname = p_schema
    LOOP
      IF v_function_argtypes IS NULL THEN
        v_script := v_script || '  RETURN NEXT has_function(' || quote_literal(p_schema) || '::name, ' || 
                    quote_literal(v_function_name) || '::name, ' || 
                    quote_literal('The function ' || p_schema || '.' || v_function_name || '() exists') || E');\n';
      ELSE
        v_script := v_script || '  RETURN NEXT has_function(' || quote_literal(p_schema) || '::name, ' || 
                    quote_literal(v_function_name) || '::name, ARRAY[' || v_function_argtypes || '], ' || 
                    quote_literal('The function ' || p_schema || '.' || v_function_name || '(' || v_function_id || ') exists') || E');\n';
      END IF;
    END LOOP;
    -- End function
    v_script := v_script || E'END;\n';
    v_script := v_script || E'\$\$;\n\n';
    -- return
    v_test.test_schema := p_schema::TEXT;
    v_test.test_name   := 'test_' || p_schema || '_functions';
    v_test.the_test    := v_script;
    RETURN v_test;
END;
$$;
