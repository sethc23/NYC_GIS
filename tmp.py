

def do_this(plpy,iter_round,DEBUG=False,EMBED=False):

    import os,re

    # import sys
    # sys.path.append('/usr/local/lib/python2.7/dist-packages/pycharm-debug.egg')
    # import pydevd
    # pydevd.settrace('10.0.1.53', port=50003, stdoutToServer=True, stderrToServer=True)

    # import IPython as I
    # I.embed_kernel()

    if DEBUG:
        os.system("echo '\\n\\n'`date --utc`")

    def log_to_file(msg):
        # for m in msg.split('\n'):
        # plpy.log(msg)
        os.system("echo '%s' >> /tmp/tmpfile" % msg)

    def run_query(**kwargs):

        locals().update(kwargs)

        qry_a = """ SELECT
                        UPPER(CONCAT_WS('_',
                            %(concat)s
                            )) a_str,
                        ts_uid a_idx,
                        ts_div_line
                    FROM str_matching
                    %(cond)s
                    ORDER BY uid
                """
        qry_b = """ SELECT DISTINCT
                        UPPER(CONCAT_WS('_',
                            %(concat)s
                            )) b_str,
                        uid b_idx
                    FROM sub_stations
                    %(cond)s
                    ORDER BY uid
                """

        _updated = False

        ra = plpy.execute( qry_a % {'concat':a_concat,'cond':a_str_idx_cond} )
        if not ra:
            return _updated,0

        for r in ra:
            if DEBUG:
                log_to_file('----------ts_uid: %(a_idx)s' % r)

            if EMBED:
                if '%(a_idx)s' % r=='385':
                    import ipdb
                    ipdb.set_trace()

            for k,v in r.iteritems():

                # FIRST
                # check if even number of single-quotes
                if str(v).count("'") % 2==0:
                    pass

                # HACK -- replace right-most single-quote with 2 single-quotes
                else:
                    pt = v.rfind("'")
                    r[k] = v[:pt] + "'" + v[pt:]

                # SECOND
                # since r is passed through 2 layers of substitution:
                #   (1) first when creating qry variable, and
                #   (2) within function z_string_matching called by qry
                # then:
                #   "'" --> "''''"
                if str(v).count("'"):
                    r[k] = re.sub(r"'{1}","''''",v)

            _t = {
                'f_qry_a':qry_a.replace("'","''")
                      % {'concat':a_concat.replace("'","''").replace('\\','\\\\'),
                         'cond':a_str_cond.replace("'","''") }
                            % r,

                'f_qry_b':qry_b.replace("'","''")
                      % {'concat':b_concat.replace("'","''").replace('\\','\\\\'),
                         'cond':b_str_cond.replace("'","''") }
                            % r,

                'res_cond':result_cond
                 }

            qry =   """
                    WITH qry AS (
                        SELECT (z).*
                        FROM z_string_matching(
                            e'%(f_qry_a)s'::text,
                            e'%(f_qry_b)s'::text) z
                    )
                    ,str_matching_form as (
                        select
                            _str.ts_uid,_str.ts_div_line,_str.ts_station
                            ,z.a_str ts_str,
                                z.jaro_score::DOUBLE PRECISION,
                                z.b_str match_str
                            ,_stat.station_name sub_station,
                                div_line sub_div_line,
                                z.b_idx::INTEGER sub_idx
                            ,z.other_matches
                        FROM qry z
                        INNER JOIN str_matching _str
                        ON _str.ts_uid = z.a_idx::INTEGER
                        INNER JOIN sub_stations _stat
                        ON _stat.uid = z.b_idx::INTEGER
                        %(res_cond)s
                    )
                    ,upd AS (
                        UPDATE str_matching _str
                        SET
                            ts_str=_res.ts_str,
                            jaro_score=_res.jaro_score,
                            match_str=_res.match_str,
                            sub_station=_res.sub_station,
                            sub_div_line=_res.sub_div_line,
                            sub_idx=_res.sub_idx
                        FROM  str_matching_form _res
                        WHERE _res.ts_uid=_str.ts_uid
                        AND _res.jaro_score > _str.jaro_score
                        RETURNING uid
                    )
                    SELECT * FROM upd
                    """ % _t

            # log_to_file(qry)
            q_res = plpy.execute(qry)

            if q_res:
                log_to_file("found")
                _updated = True

            if DEBUG:
                # log_to_file(qry)
                log_to_file(q_res)
                pass

            break

        return _updated,r

    def mark_unmatched(r):
        plpy.execute("UPDATE str_matching SET jaro_score=-1 WHERE ts_uid=%(a_idx)s"%r)
        if DEBUG:
            log_to_file('marked')
            log_to_file(r)

    def run_iter():

        # ITERATIONS:
        #   all permutations between a_str, b_str, and:
        #      station name,
        #      first half of station name split by "-",
        #      second half of station name split by "-",

        a_str_concat=['%s ts_station %s' % (a_prefix,a_suffix),
                      "%s REGEXP_REPLACE(ts_station,E'^([^-/]*)(-|/)(.*)$',E'\\1',E'g') %s" % (a_prefix,a_suffix),
                      "%s REGEXP_REPLACE(ts_station,E'^([^-/]*)(-|/)(.*)$',E'\\3',E'g') %s" % (a_prefix,a_suffix),
                      ]
        b_str_concat=['%s station_name %s' % (b_prefix,b_suffix),
                      "%s REGEXP_REPLACE(station_name,E'^([^-/]*)(-|/)(.*)$',E'\\1',E'g') %s" % (b_prefix,b_suffix),
                      "%s REGEXP_REPLACE(station_name,E'^([^-/]*)(-|/)(.*)$',E'\\3',E'g') %s" % (b_prefix,b_suffix),
                      ]

        # Note re: "\"
        #   each substitution interprets "\\" as "\"
        #   final log print of query before execution needs "\\\\" to read "\\"

        global a_concat,b_concat
        end = False
        while True:

            updated = False
            for i in range(len(a_str_concat)):
                a_concat = a_str_concat[i].replace('\\','\\\\')

                for j in range(len(b_str_concat)):
                    b_concat = b_str_concat[j].replace('\\','\\\\')

                    _t = {'a_concat' : a_concat,
                          'b_concat' : b_concat,}

                    res,r = run_query(**_t)
                    if res:
                        updated = True
                    elif not r:
                        end = True
                        break

                    if end:
                        break
                    elif (not updated
                        and i==len(a_str_concat)-1
                        and j==len(b_str_concat)-1):
                            log_to_file('MARKING UNMATCHED')
                            mark_unmatched(r)

                if end:
                    break
            if end:
                break





        #         if DEBUG:
        #             break
        #     if DEBUG:
        #         break


    a=0


    # First, all iterations are run where:
    #   (1) "a" queries limited to case where jaro_score = 0,
    #   (2) a_str and b_str are limited to station names
    #   (3) only b_str compared have same div_line as a_str ("strict_div_line"), and
    #   (4) only results with jaro_scores 0.95 and above are applied to str_matching


    # Second, similar to first except:
    #     no-emphasis on "division", and
    #     no limit on what results update str_matchingn


    # Third, all iteration run again, except where:
        #   (1) no strict matching for div_line
        #   (2) a_str and b_str now includes div_line
        #   (3) "a" queries limited to where jaro_score < 0.95
        #   (4) all results with higher scores replace in str_matching (default: 0.0)

    # TODO: Z.JARO_SCORE==SAME AND NEW RESULT HAS NUMBER


    if iter_round=='first':
        a_prefix,a_suffix = '',''
        b_prefix,b_suffix = '',''
        a_str_idx_cond = ' WHERE jaro_score=0'            #  this relates to first_iter matching single 'a' results
        a_str_cond =' WHERE ts_uid=%(a_idx)s AND jaro_score>=0'
        b_str_cond =" WHERE div_line='%(ts_div_line)s'"
        result_cond = 'WHERE NOT z.jaro_score::DOUBLE PRECISION < 0.95'

        run_iter()

    elif iter_round=='second':
        #import IPython as I
        #I.embed_kernel()
        res = plpy.execute("""WITH old AS (SELECT uid FROM str_matching WHERE jaro_score=-1) UPDATE str_matching s SET jaro_score=0 FROM old o WHERE o.uid = s.uid;""")
        a_prefix,a_suffix = '',''
        b_prefix,b_suffix = '',''
        a_str_idx_cond = ' WHERE jaro_score>=0 AND jaro_score<0.95'
        a_str_cond = ' WHERE ts_uid=%(a_idx)s AND jaro_score>=0' #    jaro_score --> -1 when no matches in round 2



        _t =        {'name' : "station_name ~* '%(a_str)s'",
                     'div' : "split_part(div_line,'_',1)=SPLIT_PART('%(ts_div_line)s','_',1)",
                     'line' : "SPLIT_PART(div_line,'_',2)~*SPLIT_PART('%(ts_div_line)s','_',2)",
                    }

        b_str_conditions = [' AND '.join([_t['name'],_t['div'],_t['line']]),
                            ' AND '.join([_t['name'],_t['div']]),
                            ' AND '.join([_t['name'],_t['line']]),
                            # ' AND '.join([_t['div'],_t['line']]),
                            # _t['line'],
                           ]

        result_cond = ' WHERE z.jaro_score::DOUBLE PRECISION > _str.jaro_score'

        for it in b_str_conditions:
            b_str_cond = ' WHERE ' + it
            if EMBED:
                import IPython as I
                I.embed_kernel()
            run_iter()

    elif iter_round=='third':
        a_prefix,a_suffix = '',',ts_div_line'
        b_prefix,b_suffix = '',',div_line'
        a_str_idx_cond = ' WHERE jaro_score >= 0 AND jaro_score < 0.95'
        a_str_cond = ' WHERE ts_uid=%(a_idx)s AND jaro_score>=0'
        b_str_cond = " "
        result_cond = ' '
        run_iter()

    if DEBUG:
        os.system("echo `date --utc`'\\nDONE' >> /tmp/tmpfile")


