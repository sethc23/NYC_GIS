
from ipdb import set_trace as i_trace
# i_trace()




class pgSQL_Functions:
    """

    NOTE: USE plpythonu and plluau for WRITE ACCESS

    """

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def exists(self,funct_name):
        qry                                 =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_proc
                                                    WHERE proname='%s');
                                                """ % funct_name
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Check:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def primary_key(self,table_name):
            qry                             =   """
                                                select relhasindex has_index
                                                from pg_class
                                                where relnamespace=2200
                                                and relkind='r'
                                                and relname=quote_ident('%s');
                                                """ % table_name
            x                               =   self.T.pd.read_sql(qry,self.T.eng)
            return                              True if len(x['has_index']) and x['has_index'][0]==True else False

    class Run:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def make_column_primary_serial_key(self,table_name,uid_col='uid',is_new_col=True):
            """
            Usage: make_column_primary_serial_key('table_name','uid_col',is_new_col=True)
            """
            if not self.F.functions_exists('z_make_column_primary_serial_key'):
                self.F.functions_run_make_column_primary_serial_key()
            T                               =   {'tbl'                  :   table_name,
                                                 'uid_col'              :   uid_col,
                                                 'is_new_col'           :   is_new_col}
            cmd                             =   """select z_make_column_primary_serial_key( '%(tbl)s',
                                                                                        '%(uid_col)s',
                                                                                         %(is_new_col)s );
                                                """ % T
            self.T.to_sql(                      cmd)

        def get_geocode_info(self,addr_queries):
            addr_queries                =   addr_queries if type(addr_queries)==list else [addr_queries]
            T                           =   {'req'                  :   str(addr_queries),
                                             'idx'                  :   str(range(len(addr_queries)))}
            cmd                         =   """select z_get_geocode_info( array%(idx)s,array%(req)s ) res;
                                            """ % T
            res                         =   self.T.pd.read_sql(cmd,self.T.eng).res
            return res

        def confirm_extensions(self):
            qry =   """
                    CREATE EXTENSION IF NOT EXISTS plpythonu;
                    CREATE EXTENSION IF NOT EXISTS pllua;
                    --CREATE EXTENSION IF NOT EXISTS plpgsql;
                    CREATE EXTENSION IF NOT EXISTS postgis;
                    --CREATE EXTENSION IF NOT EXISTS postgis_topology;
                    --CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
                    --CREATE EXTENSION IF NOT EXISTS pgrouting;
                    """
            self.T.to_sql(qry)
            print 'Extensions Confirmed'
            if not self.F.triggers_exists_event_trigger('missing_primary_key_trigger'):
                idx_trig = raw_input('add trigger to automatically create column "uid" as index col if table created without index column? (y/n)\t')
                if idx_trig=='y':
                    self.F.triggers_create_z_auto_add_primary_key()
            if not self.F.triggers_exists_event_trigger('missing_last_updated_field'):
                modified_trig = raw_input('add trigger to automatically create column "last_updated" for all new tables and update col/row when row modified? (y/n)\t')
                if modified_trig=='y':
                    self.F.triggers_create_z_auto_add_last_updated_field()
                    #### self.F.triggers_create_z_auto_update_timestamp()
            return

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_make_column_primary_serial_key(self):
            """

            Usage:

                select z_make_column_primary_serial_key({table_name}::text,
                                                        {col_name}::text,
                                                        {BOOL_is_new_col}::boolean)


            """
            self.F.functions_run_confirm_extensions()
            cmd="""

                CREATE OR REPLACE FUNCTION z_make_column_primary_serial_key(
                        table_name text,
                        col_name text,
                        new_col boolean)
                    RETURNS text AS
                    $$

                        T = {'tbl':table_name,'uid_col':col_name}
                        if new_col:
                            p0 = "ALTER TABLE %(tbl)s ADD COLUMN %(uid_col)s SERIAL;" % T
                            e = plpy.execute(p0)

                        p2 = \"\"\"

                                ALTER TABLE %(tbl)s ADD PRIMARY KEY (%(uid_col)s);


                            \"\"\" % T
                        e = plpy.execute(p2)

                        from time import sleep
                        sleep(2)

                        p2 = \"\"\"



                                UPDATE %(tbl)s SET %(uid_col)s =
                                    nextval(pg_get_serial_sequence('%(tbl)s','%(uid_col)s'));

                            \"\"\" % T

                        #plpy.log(p2)
                        e = plpy.execute(p2)

                        return 'ok'

                    $$
                    LANGUAGE plpythonu
                """
            cmd="""
                    DROP FUNCTION IF EXISTS z_make_column_primary_serial_key(text,text,boolean);

                    CREATE OR REPLACE FUNCTION z_make_column_primary_serial_key(
                        IN tbl text,
                        IN uid_col text,
                        IN new_col boolean)
                    RETURNS VOID AS
                    $$
                    DECLARE
                        _seq text;
                    BEGIN

                        IF (new_col=True)
                        THEN execute format('alter table %I add column %s serial primary key;',tbl,uid_col);
                        END IF;

                                --UPDATE %(tbl)s SET %(uid_col)s =
                                --    nextval(pg_get_serial_sequence('%(tbl)s','%(uid_col)s'));


                        execute format('alter table %I add primary key (%s);',tbl,uid_col);
                        _seq = format('%I_%s',tbl,uid_col);
                        execute format('alter table %I alter column %s set default z_next_free(''%s'',''%s'',''%s'')',
                                                   tbl,            uid_col,                    tbl,uid_col,_seq);
                        --execute format('alter table %I alter column %s set default
                        --                    nextval(pg_get_serial_sequence(''%I'',''%s''));',
                        --                ,tbl,uid_col,tbl,uid_col);

                    END;
                    $$
                    LANGUAGE plpgsql
                """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
            self.T.z_next_free(                 )
        def z_next_free(self):
            self.F.functions_run_confirm_extensions()
            cmd="""
                -- don't use drop ... cascade if tables depend on this function
                DROP FUNCTION IF EXISTS z_next_free(text, text, text) CASCADE;


                CREATE OR REPLACE FUNCTION z_next_free( table_name text,
                                                        uid_col text,
                                                        _seq text)
                RETURNS integer AS
                $BODY$
                stop=False
                T = {'tbl':table_name,'uid_col':uid_col,'_seq':_seq}
                # p = \"\"\"
                #
                #         SELECT _tbl_cnt,_seq_cnt
                #         FROM
                #             (SELECT count(*)>0 _tbl_cnt
                #             FROM information_schema.tables
                #             WHERE table_schema='public' AND table_name='%(tbl)s') f1,
                #             (SELECT count(*)>0 _seq_cnt FROM pg_class
                #              WHERE relname = concat_ws('_','%(tbl)s','%(uid_col)s','seq')) f2;
                #     \"\"\" % T
                # tbl = plpy.execute(p)[0]['_tbl_cnt']
                # seq = plpy.execute(p)[0]['_seq_cnt']
                # plpy.log(tbl)
                # plpy.log(seq)
                # if not seq and tbl:
                #     p = "create sequence %(tbl)s_%(uid_col)s_seq start with 1;"%T
                #     t = plpy.execute(p)
                # if tbl and seq:
                #     p = "alter table %(tbl)s alter column %(uid_col)s set DEFAULT z_next_free('%(tbl)s'::text, 'uid'::text, '%(tbl)s_uid_seq'::text);"%T
                #     t = plpy.execute(p)
                p = \"\"\"

                            select count(column_name) c
                            from INFORMATION_SCHEMA.COLUMNS
                            where table_name = '%(tbl)s'
                            and column_name = '%(uid_col)s';

                    \"\"\" % T
                cnt = plpy.execute(p)[0]['c']

                if cnt==0:
                    p = "create sequence %(tbl)s_%(uid_col)s_seq start with 1;"%T
                    t = plpy.execute(p)
                    p = "alter table %(tbl)s alter column %(uid_col)s set DEFAULT z_next_free('%(tbl)s'::text, 'uid'::text, '%(tbl)s_uid_seq'::text);"%T
                    t = plpy.execute(p)
                stop=False
                while stop==False:
                    p = "SELECT nextval('%(tbl)s_%(uid_col)s_seq') next_val"%T
                    try:
                        t = plpy.execute(p)[0]['next_val']
                    except plpy.spiexceptions.UndefinedTable:
                        p = "select max(%(uid_col)s) from %(tbl)s;" % T
                        try:
                            max_num = plpy.execute(p)[0]['max']
                            if max_num:
                                T.update({'max_num':str(max_num)})
                            else:
                                T.update({'max_num':str(1)})
                        except plpy.spiexceptions.UndefinedTable:
                            T.update({'max_num':str(1)})
                        p = "create sequence %(tbl)s_%(uid_col)s_seq start with %(max_num)s;" % T
                        t = plpy.execute(p)
                        p = "SELECT nextval('%(tbl)s_%(uid_col)s_seq') next_val"%T
                        t = plpy.execute(p)[0]['next_val']
                    T.update({'next_val':t})
                    # if tbl:
                    #     p = "SELECT count(%(uid_col)s) cnt from %(tbl)s where %(uid_col)s=%(next_val)s"%T
                    #     chk = plpy.execute(p)[0]['cnt']
                    # if not tbl or not chk:
                    #     stop=True
                    #     break
                    p = "SELECT count(%(uid_col)s) cnt from %(tbl)s where %(uid_col)s=%(next_val)s"%T
                    chk = plpy.execute(p)[0]['cnt']
                    if chk==0:
                        stop=True
                        break
                return T['next_val']
                $BODY$
                LANGUAGE plpythonu;
            """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
            print 'Added: f(x) z_next_free'
        def z_get_way_between_ways(self):
            cmd="""CREATE OR REPLACE FUNCTION
                    z_get_way_between_ways( IN get_way text, in ways1 text,
                                            IN ways2 text, out geom_res geometry(LineString,4326))
                     AS $$
                    begin
                        select st_line_substring(line1,arr[1],arr[2]) into geom_res
                        from (
                            select line1, array_sort(array[pt1, pt2]) arr
                            from (
                                select
                                    line1,
                                    st_line_locate_point(line1,z_intersection_point_bin(get_way,ways1)) pt1,
                                    st_line_locate_point(line1,z_intersection_point_bin(get_way,ways2)) pt2
                                from
                                    (select geom line1 from addr_idx where street = get_way limit 1) as l1
                            ) as t
                        ) as t;
                    end;
                    $$ language plpgsql;
                """.replace('\n','')
            engine.execute(cmd)
        def z_intersection_point(self):
            cmd="""
                DROP FUNCTION z_intersection_point(text,text);
                CREATE OR REPLACE FUNCTION z_intersection_point(IN way1 text, IN way2 text)
                  RETURNS text AS $$
                declare
                    _geom geometry; geom_type text;
                begin
                    select st_intersection(line1,line2) into _geom
                    from
                        (select geom line1 from addr_idx where street = way1 limit 1) as _line1,
                        (select geom line2 from addr_idx where street = way2 limit 1) as _line2;
                    select geometrytype(_geom) into geom_type;
                    if geom_type = 'LINESTRING' then
                        return st_astext(st_line_interpolate_point(_geom,0.5));
                    elsif geom_type = 'POINT' then
                        return st_astext(_geom);
                    end if;
                end;
                $$ language plpgsql;
                """.replace('\n','')
            engine.execute(cmd)
        def z_get_way_box(self):
            cmd="""
                CREATE OR REPLACE FUNCTION
                z_get_way_box( IN way1 text, in way2 text, in way3 text, in way4 text, out geom_res geometry(Polygon,4326))
                AS $$
                begin

                    select st_makepolygon(st_linemerge(st_collect(array[
                    z_get_way_between_ways(way1,way2,way4),
                    z_get_way_between_ways(way2,way1,way3),
                    z_get_way_between_ways(way3,way2,way4),
                    z_get_way_between_ways(way4,way1,way3)
                    ]))) into geom_res;

                end;
                $$ language plpgsql;
                """.replace('\n','')
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)

        def OLD_z_attempt_to_add_range_from_addr(self):
            """

            USAGE:

                select z_attempt_to_add_range_from_addr(num,predir,street_name,suftype,sufdir)
                from yelp where uid = 18486;
            """
            cmd="""

                drop function if exists z_attempt_to_add_range_from_addr(text,text,text,text,text) cascade;

                CREATE OR REPLACE FUNCTION z_attempt_to_add_range_from_addr(    IN new_street_num text,
                                                                                IN predir text,
                                                                                IN street_name text,
                                                                                IN suftype text,
                                                                                IN sufdir text)
                RETURNS text AS $$

                    if int(new_street_num) ## 2==0:
                        parity = '2'
                    else:
                        parity = '1'

                    T = {   'num'           :   new_street_num,
                            'parity'        :   parity,
                            'predir'        :   predir,
                            'name'          :   street_name,
                            'suftype'       :   suftype,
                            'sufdir'        :   sufdir}

                    p1 =    \"\"\"
                                select uid,max_num,min_num
                                from
                                    (select f1.uid,f1.max_num
                                        from
                                        (
                                        select
                                            uid,max_num,
                                            max(max_num) over (partition by block) as max_thing
                                        from pad_adr p
                                        where street_name = '##(name)s'
                                        and   predir = '##(predir)s'
                                        and   parity = '##(parity)s'
                                        and   min_num < ##(num)s
                                        order by min_num
                                        ) f1
                                    where f1.max_num=f1.max_thing) f2,
                                    (select min_num
                                    from
                                        (
                                        select block,billbbl,min_num,min(min_num) over (partition by block) as min_thing
                                        from pad_adr
                                        where street_name = '##(name)s'
                                        and   predir = '##(predir)s'
                                        and   parity = '##(parity)s'
                                        and   min_num > ##(num)s
                                        order by min_num
                                        ) f3
                                    where f3.min_num=f3.min_thing
                                    limit 1) f4
                            \"\"\" ## T

                    e1                      =   plpy.execute(p1)[0]
                    start_num               =   int(e1['max_num'])
                    end_num                 =   int(e1['min_num'])
                    for j in range(start_num+1,end_num+1):
                        if ((j ## 2==0) == (start_num ## 2==0)):
                            low_num         =   j
                            break
                    high_num = [it for it in range(low_num,end_num) if ((it ## 2==0) == (start_num ## 2==0))][-1:][0]

                    T.update(  {'uid'       :   e1['uid'],
                                'min_num'   :   low_num,
                                'max_num'   :   high_num    } )

                    p2 =    \"\"\"
                                insert into pad_adr
                                    (
                                    block,lot,bin,
                                    lhns,lcontpar,lsos,
                                    hhns,hcontpar,hsos,
                                    scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,segid,
                                    zipcode,bbl,stnum_w_letter,predir,street_name,suftype,sufdir,
                                    min_num,max_num,
                                    billbbl,tmp
                                    )
                                select
                                    block,lot,bin,
                                    lhns,lcontpar,lsos,
                                    hhns,hcontpar,hsos,
                                    scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,segid,
                                    zipcode,bbl,stnum_w_letter,predir,street_name,suftype,sufdir,
                                    ##(min_num)s,##(max_num)s,
                                    billbbl,tmp
                                from ( select * from pad_adr where uid = ##(uid)s ) f
                            \"\"\" ## T

                    plpy.execute(               p2)
                    return 'ok'

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return

        def OLD_z_run_string_functions(self):
            a="""

                -- USE USPS ABBREVIATION GUIDE
                UPDATE  yelp y SET street_name = f2.repl
                from    (
                        select  distinct on (f1.street_name) f1.uid,
                                regexp_replace(upper(f1.street_name),upper(u.usps_abbr),upper(u.pattern)) repl
                        from
                            usps u,
                            (select uid,street_name from yelp where geom is null and street_name is not null) f1
                        where u.usps_abbr ilike f1.street_name
                        and u.pattern is not null
                        ) f2
                WHERE f2.uid = y.uid

                #
                # -- USE STRING DISTANCE
                # UPDATE yelp y SET street_name = f2.jaro_b
                # FROM    (
                #         select (z).* from
                #             (
                #             select z_update_by_string_dist(array_agg(y.uid),array_agg(y.street_name),'pad_adr','street_name') z
                #             from yelp y where geom is null and street_name is not null
                #             ) f1
                #         ) f2
                # WHERE   f2.jaro > 0.8 and f2.jaro != 1.0
                #         and f2.idx = y.uid
                #
                # -- CROSS WITH SND
                # select z_attempt_to_add_range_from_addr(num,predir,street_name,suftype,sufdir)
                # from yelp where uid = 18486



            """
        def z_update_by_crossing_with_snd(self):
            cmd="""

                DROP FUNCTION IF EXISTS z_update_by_crossing_with_snd(integer,text,text);

                CREATE FUNCTION z_update_by_crossing_with_snd(                  idx              integer,
                                                                                tbl              text,
                                                                                gid_col          text)
                RETURNS text AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info


                T = {   'tbl'       :   tbl,
                        'gid_col'   :   gid_col,
                        'idx'       :   str(idx)     }


                #  <<  WITH BLDG. NUMBER  >>  #
                p = \"\"\"  WITH upd AS (
                                SELECT  distinct on (s.uid)
                                        f.uid::bigint src_gid,
                                        s.to_num,s.to_predir,s.to_street_name,s.to_suftype,s.to_sufdir
                                FROM    snd s,
                                        (   select ##(gid_col)s uid,concat_ws(' ',num,predir,street_name,suftype,sufdir ) concat_addr
                                            from ##(tbl)s where ##(gid_col)s = ##(idx)s   ) f
                                WHERE   concat_ws(' ',s.from_num,s.from_predir,s.from_street_name,s.from_suftype,s.from_sufdir ) = f.concat_addr
                                )
                            UPDATE ##(tbl)s t set
                                    num         =   u.to_num,
                                    predir      =   u.to_predir,
                                    street_name =   u.to_street_name,
                                    suftype     =   u.to_suftype,
                                    sufdir      =   u.to_sufdir
                            FROM    upd u
                            WHERE   u.src_gid   =   t.##(gid_col)s::bigint
                            RETURNING t.##(gid_col)s
                    \"\"\" ## T
                res                 =   plpy.execute(p)


                #  <<  WITHOUT BLDG. NUMBER  >>  #
                p = \"\"\"  WITH upd AS (
                                SELECT  distinct on (s.uid)
                                        f.uid::bigint src_gid,
                                        s.to_predir,s.to_street_name,s.to_suftype,s.to_sufdir
                                FROM    snd s,
                                        (   select ##(gid_col)s uid,concat_ws(' ',predir,street_name,suftype,sufdir ) concat_addr
                                            from ##(tbl)s where ##(gid_col)s = ##(idx)s   ) f
                                WHERE   concat_ws(' ',s.from_predir,s.from_street_name,s.from_suftype,s.from_sufdir ) = f.concat_addr
                                )
                            UPDATE ##(tbl)s t set
                                    predir      =   u.to_predir,
                                    street_name =   u.to_street_name,
                                    suftype     =   u.to_suftype,
                                    sufdir      =   u.to_sufdir
                            FROM    upd u
                            WHERE   u.src_gid   =   t.##(gid_col)s::bigint
                            RETURNING t.##(gid_col)s
                    \"\"\" ## T
                res                 =   plpy.execute(p)


                #  <<  REPLACING VARIATION WITH PRIMARY_NAME  >>  #
                p = \"\"\"  WITH upd AS (
                                SELECT  distinct on (s.variation)
                                        f.uid::bigint src_gid,
                                        s.primary_name
                                FROM    snd s,
                                        (   select ##(gid_col)s uid, street_name
                                            from ##(tbl)s where ##(gid_col)s = ##(idx)s   ) f
                                WHERE   s.variation = f.street_name AND s.variation is not null
                                )
                            UPDATE ##(tbl)s t set
                                    street_name =   u.primary_name,
                            FROM    upd u
                            WHERE   u.src_gid   =   t.##(gid_col)s::bigint
                            RETURNING t.##(gid_col)s
                    \"\"\" ## T
                res                 =   plpy.execute(p)

                $$ LANGUAGE plpythonu;

            """.replace('##','%')
            # print cmd
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_addr_with_string_dist_on_all(self):
            """
            NOTE:

                Comparing snd.{predir,name,suftype,sufdir} is used to keep system in check.
                For instance,
                    addr = 'West 4th Street and Mercer'

                    z_update_with_parsed_info (when validity_check = True [default]) will return nil,
                        b/c street num will be '0' and not valid.

                    by comparing snd.{predir,name,suftype,sufdir} along with the other strings,
                        a valid street name will not be transformed into the next closest string or series of close strings.

                ALSO NOTE:  only in this comparison are matching results allowed to equal 1.0, i.e., exact match.

            """
            cmd="""

                DROP FUNCTION IF EXISTS z_update_addr_with_string_dist_on_all(  integer,text,text,text,
                                                                                text);
                CREATE OR REPLACE FUNCTION z_update_addr_with_string_dist_on_all(       idx                 integer,
                                                                                        tbl                 text,
                                                                                        uid_col             text,
                                                                                        addr_col            text,
                                                                                        zip_col             text)
                RETURNS text AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                T       =   {   'idx'               :   str(idx),
                                'tbl'               :   tbl,
                                'uid_col'           :   uid_col,
                                'addr_col'          :   addr_col,
                                'zip_col'           :   zip_col,

                                'match_num_min'     :   str(0.875),

                                'comp_to_1'         :   'pad_adr',
                                'comp_from_1_cols'  :   'predir,name,suftype,sufdir,zip',
                                'comp_to_1_cols'    :   " 'predir','street_name','suftype','sufdir','zipcode' ",

                                'comp_to_2'         :   'usps',
                                'comp_from_2_cols'  :   'name',
                                'comp_to_2_cols'    :   " 'common_use' ",

                                'comp_to_3'         :   'usps',
                                'comp_from_3_cols'  :   'name',
                                'comp_to_3_cols'    :   " 'usps_abbr' ",

                                'comp_to_4'         :   'snd',
                                'comp_from_4_cols'  :   'name',
                                'comp_to_4_cols'    :   " 'variation' ",

                                'comp_to_5'         :   'snd',
                                'comp_from_5_cols'  :   'name',
                                'comp_to_5_cols'    :   " 'primary_name' ",

                                'comp_to_6'         :   'snd',
                                'comp_from_6_cols'  :   'predir,name,suftype,sufdir',
                                'comp_to_6_cols'    :   "'from_predir','from_street_name','from_suftype','from_sufdir'",
                                }

                #plpy.log(T)
                try:

                    p   =   \"\"\"

                            WITH
                                upd_1 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_1)s'::text,
                                                                            array[ ##(comp_to_1_cols)s ]) z
                                            from
                                                (
                                                select
                                                    src_gid::integer uid,
                                                    concat_ws(' ',##(comp_from_1_cols)s) comp_element
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                            select
                                                                ##(uid_col)s::bigint gid,
                                                                ##(addr_col)s::text address,
                                                                ##(zip_col)s::bigint zipcode
                                                            FROM ##(tbl)s
                                                            WHERE ##(uid_col)s = ##(idx)s
                                                            ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2

                                WHERE   f2.jaro > ##(match_num_min)s and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                            ),
                                upd_2 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_2)s'::text,
                                                                            array[ ##(comp_to_2_cols)s ]) z
                                            from
                                                (
                                                -- SELECT LAST ELEMENT FROM VALUE WHEN SPLIT BY ' '
                                                select
                                                    uid,
                                                    split_part(f_str, ' ', array_upper(regex_split,1)) comp_element
                                                from
                                                    (
                                                    select
                                                        uid,
                                                        regexp_split_to_array(comp_cols, ' ') regex_split,
                                                        comp_cols f_str
                                                    from
                                                        (
                                                        select
                                                            src_gid::integer uid,
                                                            ##(comp_from_2_cols)s comp_cols
                                                        from
                                                            (
                                                            select (z).*
                                                            from
                                                                (
                                                                    select z_parse_NY_addrs('
                                                                    select
                                                                        ##(uid_col)s::bigint gid,
                                                                        ##(addr_col)s::text address,
                                                                        ##(zip_col)s::bigint zipcode
                                                                    FROM ##(tbl)s
                                                                    WHERE ##(uid_col)s = ##(idx)s
                                                                        ') z
                                                                ) fe4
                                                            ) fe3
                                                        ) fe2
                                                    ) fe1
                                                ) f
                                            ) f1
                                        ) f2
                                WHERE   f2.jaro > ##(match_num_min)s and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                            ),
                                upd_3 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_3)s'::text,
                                                                            array[ ##(comp_to_3_cols)s ]) z
                                            from
                                                (
                                                -- SELECT LAST ELEMENT FROM VALUE WHEN SPLIT BY ' '
                                                select
                                                    uid,
                                                    split_part(f_str, ' ', array_upper(regex_split,1)) comp_element
                                                from
                                                    (
                                                    select
                                                        uid,
                                                        regexp_split_to_array(comp_cols, ' ') regex_split,
                                                        comp_cols f_str
                                                    from
                                                        (
                                                        select
                                                            src_gid::integer uid,
                                                            ##(comp_from_3_cols)s comp_cols
                                                        from
                                                            (
                                                            select (z).*
                                                            from
                                                                (
                                                                    select z_parse_NY_addrs('
                                                                    select
                                                                        ##(uid_col)s::bigint gid,
                                                                        ##(addr_col)s::text address,
                                                                        ##(zip_col)s::bigint zipcode
                                                                    FROM ##(tbl)s
                                                                    WHERE ##(uid_col)s = ##(idx)s
                                                                        ') z
                                                                ) fe4
                                                            ) fe3
                                                        ) fe2
                                                    ) fe1
                                                ) f
                                            ) f1
                                        ) f2
                                WHERE   f2.jaro > ##(match_num_min)s and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                            ),
                                upd_4 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_4)s'::text,
                                                                            array[ ##(comp_to_4_cols)s ]) z
                                            from
                                                (
                                                select
                                                    src_gid::integer uid,
                                                    concat_ws(' ',##(comp_from_4_cols)s) comp_element
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                            select
                                                                ##(uid_col)s::bigint gid,
                                                                ##(addr_col)s::text address,
                                                                ##(zip_col)s::bigint zipcode
                                                            FROM ##(tbl)s
                                                            WHERE ##(uid_col)s = ##(idx)s
                                                            ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2

                                WHERE   f2.jaro > ##(match_num_min)s and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                                ),
                                upd_5 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_5)s'::text,
                                                                            array[ ##(comp_to_5_cols)s ]) z
                                            from
                                                (
                                                select
                                                    src_gid::integer uid,
                                                    concat_ws(' ',##(comp_from_5_cols)s) comp_element
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                            select
                                                                ##(uid_col)s::bigint gid,
                                                                ##(addr_col)s::text address,
                                                                ##(zip_col)s::bigint zipcode
                                                            FROM ##(tbl)s
                                                            WHERE ##(uid_col)s = ##(idx)s
                                                            ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2

                                WHERE   f2.jaro > ##(match_num_min)s and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                                ),
                                upd_6 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_6)s'::text,
                                                                            array[ ##(comp_to_6_cols)s ]) z
                                            from
                                                (
                                                select
                                                    src_gid::integer uid,
                                                    concat_ws(' ',##(comp_from_6_cols)s) comp_element
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                            select
                                                                ##(uid_col)s::bigint gid,
                                                                ##(addr_col)s::text address,
                                                                ##(zip_col)s::bigint zipcode
                                                            FROM ##(tbl)s
                                                            WHERE ##(uid_col)s = ##(idx)s
                                                            ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2

                                WHERE   f2.jaro > ##(match_num_min)s
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                                )

                            UPDATE ##(tbl)s t SET
                                address = regexp_replace(   upper(t.address),
                                                            repl_array[1],
                                                            repl_array[2],
                                                            'g')
                            FROM
                                (
                                SELECT
                                    idx,
                                    ARRAY[a,b] repl_array
                                FROM
                                    (
                                    select
                                        idx,
                                        regexp_split_to_table(repl_from,' ') a,
                                        regexp_split_to_table(repl_to,' ') b
                                    from

                                        (

                                        SELECT arr[1] idx,arr[2] repl_from,arr[3] repl_to
                                        FROM
                                            (

                                            SELECT string_to_array(res,',') arr
                                            FROM
                                                (
                                                SELECT
                                                    CASE    WHEN f1.res=greatest(f1.res,f2.res,f3.res,f4.res,f5.res,f6.res) THEN f1.repl[1]
                                                            WHEN f2.res=greatest(f1.res,f2.res,f3.res,f4.res,f5.res,f6.res) THEN f2.repl[1]
                                                            WHEN f3.res=greatest(f1.res,f2.res,f3.res,f4.res,f5.res,f6.res) THEN f3.repl[1]
                                                            WHEN f4.res=greatest(f1.res,f2.res,f3.res,f4.res,f5.res,f6.res) THEN f4.repl[1]
                                                            WHEN f5.res=greatest(f1.res,f2.res,f3.res,f4.res,f5.res,f6.res) THEN f5.repl[1]
                                                            WHEN f6.res=greatest(f1.res,f2.res,f3.res,f4.res,f5.res,f6.res) THEN f6.repl[1]
                                                    END res
                                                FROM
                                                    (select array_agg(jaro) res,array_agg(concat_ws(',',idx,orig_str,jaro_b)) repl from upd_1) f1,
                                                    (select array_agg(jaro) res,array_agg(concat_ws(',',idx,orig_str,jaro_b)) repl from upd_2) f2,
                                                    (select array_agg(jaro) res,array_agg(concat_ws(',',idx,orig_str,jaro_b)) repl from upd_3) f3,
                                                    (select array_agg(jaro) res,array_agg(concat_ws(',',idx,orig_str,jaro_b)) repl from upd_4) f4,
                                                    (select array_agg(jaro) res,array_agg(concat_ws(',',idx,orig_str,jaro_b)) repl from upd_5) f5,
                                                    (select array_agg(jaro) res,array_agg(concat_ws(',',idx,orig_str,jaro_b)) repl from upd_6) f6
                                                ) f1
                                            ) f2
                                        ) f3
                                    ) f4
                                WHERE a!=b
                                ) f
                            WHERE f.idx::integer = t.##(uid_col)s::integer
                            RETURNING t.##(uid_col)s

                            \"\"\" ## T

                    #plpy.log(p)
                    res = plpy.execute(p)
                    #plpy.log(res)
                    if len(res)==1:
                        return "OK"
                    else:
                        return 'nothing updated'

                except Exception as e:
                    plpy.log(                       "ERROR: z_update_addr_with_string_dist_on_snd")
                    plpy.log(                       tb_format_exc())
                    plpy.log(                       sys_exc_info()[0])
                    plpy.log(                       e)
                    return "ERROR"

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_addr_with_string_dist_on_usps(self):
            cmd="""

                DROP FUNCTION IF EXISTS z_update_addr_with_string_dist_on_usps(integer,text,text,text,text[],text,text[]);
                CREATE OR REPLACE FUNCTION z_update_addr_with_string_dist_on_usps(       idx                integer,
                                                                                         tbl                text,
                                                                                         uid_col            text,
                                                                                         addr_col           text,
                                                                                         zip_col            text,
                                                                                         compare_from_cols  text[],
                                                                                         compare_to_tbl     text,
                                                                                         compare_to_cols    text[])
                RETURNS text AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                T       =   {   'idx'           :   str(idx),
                                'tbl'           :   tbl,
                                'uid_col'       :   uid_col,
                                'addr_col'      :   addr_col,
                                'zip_col'       :   zip_col,
                                'comp_from_cols':   ','.join(compare_from_cols),
                                'comp_to_tbl'   :   compare_to_tbl,
                                'comp_to_cols'  :   ','.join(["'"+it+"'" for it in compare_to_cols]),   }

                #plpy.log(T)
                try:

                    p   =   \"\"\"

                            WITH upd AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_tbl)s'::text,
                                                                            array[ ##(comp_to_cols)s ]) z
                                            from
                                                (
                                                -- SELECT LAST ELEMENT FROM VALUE WHEN SPLIT BY ' '
                                                select
                                                    uid,
                                                    split_part(f_str, ' ', array_upper(regex_split,1)) comp_element
                                                from
                                                    (
                                                    select
                                                        uid,
                                                        regexp_split_to_array(comp_cols, ' ') regex_split,
                                                        comp_cols f_str
                                                    from
                                                        (
                                                        select
                                                            src_gid::integer uid,
                                                            ##(comp_from_cols)s comp_cols
                                                        from
                                                            (
                                                            select (z).*
                                                            from
                                                                (
                                                                    select z_parse_NY_addrs('
                                                                    select
                                                                        ##(uid_col)s::bigint gid,
                                                                        ##(addr_col)s::text address,
                                                                        ##(zip_col)s::bigint zipcode
                                                                    FROM ##(tbl)s
                                                                    WHERE ##(uid_col)s = ##(idx)s
                                                                        ') z
                                                                ) fe4
                                                            ) fe3
                                                        ) fe2
                                                    ) fe1
                                                ) f
                                            ) f1
                                        ) f2
                                WHERE   f2.jaro > 0.8 and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                                )
                            UPDATE ##(tbl)s t SET
                                ##(addr_col)s = regexp_replace(upper(t.address),u.orig_str,u.jaro_b,'g')
                            FROM upd u
                            WHERE u.idx = t.##(uid_col)s
                            RETURNING t.##(uid_col)s uid

                            \"\"\" ## T

                    plpy.log(p)
                    res = plpy.execute(p)
                    # plpy.log(res)
                    if len(res)==1:
                        return "OK"
                    else:
                        return 'nothing updated'

                except Exception as e:
                    plpy.log(                       "ERROR: z_update_addr_with_string_dist_on_usps")
                    plpy.log(                       tb_format_exc())
                    plpy.log(                       sys_exc_info()[0])
                    plpy.log(                       e)
                    return "ERROR"

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_addr_with_string_dist_on_snd(self):
            cmd="""

                DROP FUNCTION IF EXISTS z_update_addr_with_string_dist_on_snd(  integer,text,text,text,
                                                                                text,text[]);
                CREATE OR REPLACE FUNCTION z_update_addr_with_string_dist_on_snd(       idx                 integer,
                                                                                        tbl                 text,
                                                                                        uid_col             text,
                                                                                        addr_col            text,

                                                                                        zip_col             text,
                                                                                        compare_from_cols   text[])
                RETURNS text AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                T       =   {   'idx'           :   str(idx),
                                'tbl'           :   tbl,
                                'uid_col'       :   uid_col,
                                'addr_col'      :   addr_col,
                                'zip_col'       :   zip_col,
                                'comp_from_cols':   ','.join(compare_from_cols),

                                'comp_to_tbl'   :   'snd',
                                'comp_to_cols_1':   'primary_name',
                                'comp_to_cols_2':   'variation',}

                #plpy.log(T)
                try:

                    p   =   \"\"\"

                            WITH upd_1 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_tbl)s'::text,
                                                                            array[ '##(comp_to_cols_1)s' ]) z
                                            from
                                                (
                                                select
                                                    src_gid::integer uid,
                                                    concat_ws(' ',##(comp_from_cols)s) comp_element
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                            select
                                                                ##(uid_col)s::bigint gid,
                                                                ##(addr_col)s::text address,
                                                                ##(zip_col)s::bigint zipcode
                                                            FROM ##(tbl)s
                                                            WHERE ##(uid_col)s = ##(idx)s
                                                            ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2

                                WHERE   f2.jaro > 0.8 and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                            ),
                            upd_2 AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.comp_element),
                                                                            '##(comp_to_tbl)s'::text,
                                                                            array[ '##(comp_to_cols_2)s' ]) z
                                            from
                                                (
                                                select
                                                    src_gid::integer uid,
                                                    concat_ws(' ',##(comp_from_cols)s) comp_element
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                            select
                                                                ##(uid_col)s::bigint gid,
                                                                ##(addr_col)s::text address,
                                                                ##(zip_col)s::bigint zipcode
                                                            FROM ##(tbl)s
                                                            WHERE ##(uid_col)s = ##(idx)s
                                                            ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2

                                WHERE   f2.jaro > 0.8 and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                                )

                            UPDATE ##(tbl)s t SET
                                address = regexp_replace(upper(t.address),repl_array[1],repl_array[2],'g')
                            FROM
                                (
                                SELECT
                                    idx,
                                    ARRAY[a,b] repl_array
                                FROM
                                    (
                                    select
                                        idx,
                                        regexp_split_to_table(repl_from,' ') a,
                                        regexp_split_to_table(repl_to,' ') b
                                    from
                                        (
                                        SELECT
                                            (select * from string_to_array(arr,','))[1] idx,
                                            (select * from string_to_array(arr,','))[2] repl_from,
                                            (select * from string_to_array(arr,','))[3] repl_to
                                        FROM
                                            (
                                            select CASE WHEN f1.jaro>f2.jaro THEN f1.arr ELSE f2.arr END
                                            from
                                                (select concat_ws(',',idx,orig_str,jaro_b) arr,jaro from upd_1) f1,
                                                (select concat_ws(',',idx,orig_str,jaro_b) arr,jaro from upd_2) f2
                                            ) f3
                                        ) f4
                                    ) f5
                                WHERE a!=b
                                ) f
                            WHERE f.idx::integer = t.##(uid_col)s::integer
                            RETURNING t.##(uid_col)s

                            \"\"\" ## T

                    plpy.log(p)
                    res = plpy.execute(p)
                    #plpy.log(res)
                    if len(res)==1:
                        return "OK"
                    else:
                        return 'nothing updated'

                except Exception as e:
                    plpy.log(                       "ERROR: z_update_addr_with_string_dist_on_snd")
                    plpy.log(                       tb_format_exc())
                    plpy.log(                       sys_exc_info()[0])
                    plpy.log(                       e)
                    return "ERROR"

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_addr_with_string_dist_on_pad_adr(self):
            cmd="""

                DROP FUNCTION IF EXISTS z_update_addr_with_string_dist_on_pad_adr(integer,text,text,text,text,text[]);
                CREATE OR REPLACE FUNCTION z_update_addr_with_string_dist_on_pad_adr(       idx             integer,
                                                                                            tbl             text,
                                                                                            uid_col         text,
                                                                                            addr_col        text,
                                                                                            zip_col         text,
                                                                                            compare_cols    text[])
                RETURNS text AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                T       =   {   'idx'           :   str(idx),
                                'tbl'           :   tbl,
                                'uid_col'       :   uid_col,
                                'addr_col'      :   addr_col,
                                'zip_col'       :   zip_col,
                                'comp_cols'     :   ','.join(["'"+it+"'" for it in compare_cols]),  }

                #plpy.log(T)
                try:

                    p   =   \"\"\"

                            WITH upd AS (

                                SELECT f2.*
                                FROM
                                        (
                                        select (z).* from
                                            (
                                            select z_get_string_dist(       array_agg(f.uid),
                                                                            array_agg(f.concat_addr),
                                                                            'pad_adr'::text,
                                                                            array[ ##(comp_cols)s ]) z
                                            from
                                                (

                                                select src_gid::integer uid,concat_ws(  ' ',predir,name,
                                                                                        suftype,sufdir) concat_addr
                                                from
                                                    (
                                                    select (z).*
                                                    from
                                                        (
                                                            select z_parse_NY_addrs('
                                                                select
                                                                    ##(uid_col)s::bigint gid,
                                                                    ##(addr_col)s::text address,
                                                                    ##(zip_col)s::bigint zipcode
                                                                FROM ##(tbl)s
                                                                WHERE ##(uid_col)s = ##(idx)s
                                                                ') z
                                                        ) fe1
                                                    ) fe
                                                ) f
                                            ) f1
                                        ) f2
                                WHERE   f2.jaro > 0.8 and f2.jaro != 1.0
                                ORDER BY f2.jaro DESC
                                LIMIT 1
                                )
                            UPDATE ##(tbl)s t SET
                                ##(addr_col)s = regexp_replace(upper(t.address),u.orig_str,u.jaro_b,'g')
                            FROM upd u
                            WHERE u.idx = t.##(uid_col)s
                            RETURNING t.##(uid_col)s uid

                            \"\"\" ## T

                    plpy.log(p)
                    res = plpy.execute(p)
                    # plpy.log(res)
                    if len(res)==1:
                        return "OK"
                    else:
                        return 'nothing updated'

                except Exception as e:
                    plpy.log(                       "ERROR: z_update_addr_with_string_dist_on_pad_adr")
                    plpy.log(                       tb_format_exc())
                    plpy.log(                       sys_exc_info()[0])
                    plpy.log(                       e)
                    return "ERROR"

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_get_string_dist(self):
            """
            compare_col is concat_ws(' ',...)


            """
            if not self.F.types_exists('string_dist_results'):
                self.F.types_create_string_dist_results()
            cmd="""
                DROP FUNCTION IF EXISTS     z_get_string_dist(      integer[],
                                                                    text,
                                                                    text,
                                                                    text[],
                                                                    boolean,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean);

                CREATE OR REPLACE FUNCTION  z_get_string_dist(      idx             integer[],
                                                                    string_set      text[],
                                                                    compare_tbl     text,
                                                                    compare_col     text[],
                                                                    jaro            boolean default true,
                                                                    leven           boolean default true,
                                                                    nysiis          boolean default true,
                                                                    rating_codex    boolean default true,
                                                                    usps_repl_first boolean default true)
                RETURNS SETOF string_dist_results AS $$

                    from jellyfish              import cjellyfish as J
                    from traceback              import format_exc       as tb_format_exc
                    from sys                    import exc_info         as sys_exc_info

                    class string_dist_results:

                        def __init__(self,upd=None):
                            if upd:
                                self.__dict__.update(upd)


                    important_cols          =   [   'street_name','from_street_name',
                                                    'variation','primary_name',
                                                    'common_use','usps_abbr','pattern']

                    T                       =   {   'tbl'           :   compare_tbl,
                                                    'concat_col'    :   ''.join(["concat_ws(' ',",
                                                                                 ",".join(compare_col),
                                                                                 ")"]),
                                                    'not_null_cols' :   'WHERE ' + ' is not null and '.join([it for it in compare_col
                                                                                            if important_cols.count(it)>0]) + ' is not null',
                                                                                 }


                    if T['not_null_cols']=='WHERE  is not null':
                        T['not_null_cols']  =   ''

                    #plpy.log(T)
                    try:

                        p                   =   "select distinct ##(concat_col)s comparison from ##(tbl)s ##(not_null_cols)s;" ## T
                        res                 =   plpy.execute(p)
                        if len(res)==0:
                            plpy.log(           "string_dist_results: NO DATA AVAILABLE FROM ##(tbl)s IN ##(tbl)s" ## T)
                            return
                        else:
                            # plpy.log(res)
                            res             =   map(lambda s: unicode(s['comparison']),res)

                        #plpy.log("about to start")
                        for i in range(len(idx)):
                            #plpy.log("started")
                            _word           =   unicode(string_set[i].upper())
                            if not _word:
                                plpy.log(       string_set)
                                plpy.log(       "not word")
                                plpy.log(       _word)
                                yield(          None)

                            else:

                                t           =   {   'idx'           :   idx[i],
                                                    'orig_str'      :   _word   }
                                if jaro:
                                    # plpy.log(t)
                                    t.update(   dict(zip(['jaro','jaro_b'],
                                                     sorted(map(lambda s: (J.jaro_distance(_word,s),s),res ) )[-1:][0])))
                                if leven:
                                    t.update(   dict(zip(['leven','leven_b'],
                                                     sorted(map(lambda s: (J.levenshtein_distance(_word,s),s),res ) )[0:][0])))
                                if nysiis:
                                    t.update(   {   'nysiis'            :   J.nysiis(_word)                 })

                                if rating_codex:
                                    t.update(   {   'rating_codex'      :   J.match_rating_codex(_word)     })

                                # plpy.log(t)
                                r           =   string_dist_results(t)
                                yield(          r)

                        return

                    except Exception as e:
                        plpy.log(               tb_format_exc())
                        plpy.log(               sys_exc_info()[0])
                        plpy.log(               e)
                        return

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return

        def z_jellyfish(self):
            """
            compare_col is concat_ws(' ',...)


            """
            if not self.F.types_exists('string_dist_results'):
                self.F.types_create_string_dist_results()
            cmd="""
                DROP FUNCTION IF EXISTS     z_jellyfish(            text,
                                                                    text,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean,
                                                                    boolean) CASCADE;

                CREATE OR REPLACE FUNCTION  z_jellyfish(            from_str_idx_tuples_qry     text,                   -- having header: | from_tuples    |
                                                                    against_str_idx_tuples_qry  text,                   -- having header: | against_tuples |
                                                                    all_results                 boolean default false,
                                                                    best_result                 boolean default true,
                                                                    jaro                        boolean default true,
                                                                    leven                       boolean default true,
                                                                    nysiis                      boolean default true,
                                                                    rating_codex                boolean default true,
                                                                    usps_repl_first             boolean default true)
                RETURNS SETOF string_dist_results AS $$

                    \"\"\"

                    General Idea:

                        Given a list of tuples comprising strings and corresponding index values ("from_tuples"), and
                        Given another list of tuples comprising strings and corresponding index values ("against_tuples");

                        for _string,_idx in from_tuple.iteritems():
                            find closest string match between _string and [ all strings in against_tuples ]


                    Comments:

                        input queries must use double single quotes ('') in place of normal single quotes (') used to indicate text types.

                    Usage:

                        Given:
                            qry_1 = "select array[(1::integer,regexp_replace(''part_a-part_b'',''^([^-]*)-(.*)$'',''\\2'',''g''))] from_tuples"
                            qry_2 = "select array[(101::integer,''no match here''),
                                                  (102::integer,''partial match -part_b''),
                                                  (103::integer,''part_b''),] against_tuples"

                        Query:

                            select z_jellyfish(qry_1,qry_2)

                        Produces Results with Header:

                            | from_str | from_idx | against_str | against_idx | jaro_b | etc ...


                    \"\"\"


                    from jellyfish              import cjellyfish as J
                    from traceback              import format_exc       as tb_format_exc
                    from sys                    import exc_info         as sys_exc_info

                    class string_dist_results:

                        def __init__(self,upd=None):
                            if upd:
                                self.__dict__.update(upd)


                    T                       =   {   'from_qry'              :   from_str_idx_tuples_qry,
                                                    'against_qry'           :   against_str_idx_tuples_qry,
                                                }
                    plpy.log(                   T)

                    try:

                        p                   =   \"\"\"
                                                SELECT from_tuples,against_tuples
                                                FROM
                                                    (##(from_qry)s) f1,
                                                    (##(against_qry)s) f2
                                                \"\"\" ## T
                        plpy.log(               p)
                        res                 =   plpy.execute(p)
                        if len(res)==0:
                            plpy.log(           "string_dist_results: NO DATA AVAILABLE FROM ##(tbl)s IN ##(tbl)s" ## T)
                            return
                        else:
                            plpy.log(           res)
                            return
                            res             =   map(lambda s: unicode(s['comparison']),res)

                        #plpy.log("about to start")
                        for i in range(len(idx)):
                            #plpy.log("started")
                            _word           =   unicode(string_set[i].upper())
                            if not _word:
                                plpy.log(       string_set)
                                plpy.log(       "not word")
                                plpy.log(       _word)
                                yield(          None)

                            else:

                                t           =   {   'idx'           :   idx[i],
                                                    'orig_str'      :   _word   }
                                if jaro:
                                    # plpy.log(t)
                                    t.update(   dict(zip(['jaro','jaro_b'],
                                                     sorted(map(lambda s: (J.jaro_distance(_word,s),s),res ) )[-1:][0])))
                                if leven:
                                    t.update(   dict(zip(['leven','leven_b'],
                                                     sorted(map(lambda s: (J.levenshtein_distance(_word,s),s),res ) )[0:][0])))
                                if nysiis:
                                    t.update(   {   'nysiis'            :   J.nysiis(_word)                 })

                                if rating_codex:
                                    t.update(   {   'rating_codex'      :   J.match_rating_codex(_word)     })

                                # plpy.log(t)
                                r           =   string_dist_results(t)
                                yield(          r)

                        return

                    except Exception as e:
                        plpy.log(               tb_format_exc())
                        plpy.log(               sys_exc_info()[0])
                        plpy.log(               e)
                        return

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.to_sql(                     cmd)
            return
        def z_str_comp_jaro(self):
            cmd="""
                DROP FUNCTION IF EXISTS         z_str_comp_jaro(text,text,boolean,boolean,boolean);
                CREATE OR REPLACE FUNCTION      z_str_comp_jaro(s1              text,
                                                                s2              text,
                                                                winklerize      boolean default true,
                                                                long_tolerance  boolean default true,
                                                                verbose         boolean default false)
                RETURNS                         double precision
                AS $BODY$


                function round (n)
                    return math.floor((math.floor(n*2) + 1)/2)
                end

                function cjson_encode (tbl, verbose)
                    local res
                    if verbose then
                        local cjson         =   require "cjson"
                        res                 =   cjson.encode(tbl)
                    else
                        res                 =   " "
                    end
                    return res
                end

                function to_log (msg, verbose)
                    if verbose then             log(msg) end
                end
                to_log(                         "NEW EXECUTION\\n\\n", verbose)


                if #s1==0 or #s2==0 then
                    log(                        "s1 or s2 has no length!")
                end

                -- set #a>#b
                local a,b,m                 =   "","",0
                if #s1<#s2 then     b,a     =   s1,s2
                else                a,b     =   s1,s2   end
                a,b                         =   a:upper(),b:upper()
                to_log(                         "a: "..a, verbose)

                -- define max distance where character will be considered matching (despite tranposition)
                local match_dist            =   round( (#a/2) - 1 )
                if match_dist<0 then            match_dist=0 end
                to_log(                         "match_dist="..match_dist, verbose)

                -- create letter and flags tables
                local a_tbl,b_tbl           =   {},{}
                local a_flags,b_flags       =   {},{}
                for i=1,#a do
                    table.insert(               a_tbl,a:sub(i,i))
                    table.insert(               a_flags,false)

                    table.insert(               b_tbl,b:sub(i,i))
                    table.insert(               b_flags,false)
                end
                for i=#a+1, #b do
                    table.insert(               b_tbl,b:sub(i,i))
                    table.insert(               b_flags,false)
                end
                to_log(                         "a_tbl "..cjson_encode(a_tbl, verbose) , verbose)
                to_log(                         "b_tbl "..cjson_encode(b_tbl, verbose) , verbose)
                to_log(                         "b_tbl[3] "..b_tbl[3] , verbose)

                -- verify tables are proper length
                if (not #a==#a_tbl==#a_flags) or (not #b==#b_tbl==#b_flags) then
                    log(                        "issue with length of string/tbl/flags: "..#a.."/"..#a_tbl.."/"..#a_flags)
                end

                -- looking only within the match distance, count & flag matched pairs
                local low,hi,common         =   0,0,0
                local i
                for _i,v in ipairs(a_tbl) do
                    i = _i-1

                    local cursor            =   v
                    to_log(                     "cursor_1="..cursor, verbose)

                    if i>match_dist then
                        low                 =   i-match_dist
                    else
                        low                 =   0
                    end
                    if i+match_dist<=#b then
                        hi                  =   i+match_dist
                    else
                        hi                  =   #b
                    end

                    to_log(                     "low_hi "..low.." "..hi, verbose)

                    for _j=low+1, hi+1 do
                        j                   =   _j-1

                        to_log(                 "ij "..i.." "..j, verbose)
                        to_log(                 "cursor "..cursor, verbose)
                        to_log(                 "b_tbl[j+1] "..b_tbl[j+1], verbose)

                        if not b_flags[j+1] and b_tbl[j+1]==cursor then
                            to_log(             "BREAK_HERE", verbose)
                            a_flags[i+1]    =   true
                            b_flags[j+1]    =   true
                            common          =   common+1
                            break
                        end
                    end
                end
                to_log(                         "a_flags="..cjson_encode(a_flags, verbose) , verbose)
                to_log(                         "b_flags="..cjson_encode(b_flags, verbose) , verbose)

                -- return nil if no exact or transpositional matches
                if common==0 then               return nil end
                to_log(                         "common = "..common, verbose)

                -- count transpositions
                local first,k,trans_count   =   true,1,0
                local _j
                for _i,v in ipairs(a_tbl) do
                    i                       =   _i - 1

                    if a_flags[i+1] then

                        for j=k, #b do
                            _j              =   j - 1

                            to_log(            "i,j,_j= "..i..","..j..",".._j, verbose)
                            to_log(            "b_flags[j]= "..cjson_encode({b_flags[j]}, verbose) , verbose)

                            if b_flags[j] then
                                k           =   j+1
                                break
                            end
                        end

                        to_log(                 "k= "..k, verbose)
                        to_log(                 "a_tbl[i+1]= "..a_tbl[i+1], verbose)

                        if not j and first then
                            _j,first        =   1,false
                        else
                            _j              =   _j + 1
                        end

                        to_log(                 "b_tbl[_j]= "..b_tbl[_j], verbose)
                        if a_tbl[i+1]~=b_tbl[_j] then
                            if (not trans_count or trans_count==0) then
                                trans_count =   1
                            else
                                trans_count =   trans_count+1
                            end
                        end

                    end
                end
                trans_count                 =   trans_count/2
                to_log(                         "trans_count = "..trans_count, verbose)

                -- adjust for similarities in nonmatched characters
                local weight                =   0
                weight                      =   ( ( common/#a + common/#b +
                                                    (common-trans_count)/common ) )/3
                to_log(                         "weight = "..weight, verbose)

                -- winkler modification: continue to boost if strings are similar
                local i,_i,j                =   0,0,0
                if winklerize and weight>0.7 and #a>3 and #b>3 then

                    -- adjust for up to first 4 chars in common

                    if #a<4 then                j = #a
                    else                        j = 4 end
                    to_log(                     "i,j_1= "..i..","..j, verbose)

                    for _i=1, j-1 do
                        if _i==1 then           i = _i-1 end
                        if a_tbl[_i]==b_tbl[_i] and #b>=_i then
                            if not i then       i = 1
                            else                i = i+1 end
                            to_log(             "i,_i,j_2= "..i..",".._i..","..j, verbose)
                        end
                        if i>j then             break end
                    end
                    to_log(                     "i,_i,j_3= "..i..",".._i..","..j, verbose)

                    if i-1>0 then
                        i = i-1
                        weight              =   weight + ( i * 0.1 * (1.0 - weight) )
                    end
                    to_log(                     "new weight_1 = "..weight, verbose)

                    -- optionally adjust for long strings
                    -- after agreeing beginning chars, at least two or more must agree and
                    -- agreed characters must be > half of remaining characters
                    if ( long_tolerance and
                         #a>4 and
                         common>i+1 and
                         2*common>=#a+i ) then
                        weight              =   weight + ((1.0 - weight) * ( (common-i-1) / (#a+#b-i*2+2)))
                    end

                    to_log(                     "new weight_2 = "..weight, verbose)

                end

                return weight

                $BODY$ LANGUAGE plluau;
            """
            self.T.to_sql(                      cmd)

        def z_update_with_geom_from_coords(self):
            """

            Usage:

                SELECT z_update_with_geom_from_coords(13277,'yelp','uid','latitude','longitude')
                SELECT z_update_with_geom_from_coords(uid_grp[1:5],'yelp','uid','latitude','longitude')

                SELECT z_update_with_geom_from_coords(
                    'where (age(now(),last_updated) < interval ''15 minutes'' and street_name is not null limit 10',
                    'yelp'::text,'uid'::text,'latitude'::text,'longitude'::text)

            """
            cmd="""
                DROP FUNCTION IF EXISTS z_update_with_geom_from_coords(integer,text,text,text,text);

                CREATE OR REPLACE FUNCTION z_update_with_geom_from_coords(  idx         integer,
                                                                            tbl         text,
                                                                            gid_col     text,
                                                                            lat_col     text,
                                                                            lon_col     text)
                RETURNS text AS $$

                    T = {   'idx'       :   str(idx),
                            'tbl'       :   tbl,
                            'gid_col'   :   gid_col,
                            'lat_col'   :   lat_col,
                            'lon_col'   :   lon_col,
                            'search_rad':   0.0175     }

                    p = \"\"\"

                            WITH upd AS (
                                SELECT uid,bbl,geom
                                FROM
                                    (
                                    SELECT  uid,p.bbl bbl,p.geom geom,
                                            ST_Distance_Spheroid(
                                                    p.geom,
                                                    txt_pt,
                                                    'SPHEROID["WGS 84",6378137,298.257223563]')  dist
                                    FROM
                                        pluto p,
                                        (SELECT uid,st_geomfromtext(concat_ws('','POINT (',lon,' ',lat,')'),4326) txt_pt
                                        FROM
                                            (
                                            select ##(gid_col)s uid,##(lat_col)s lat,##(lon_col)s lon
                                            from ##(tbl)s where ##(gid_col)s = ##(idx)s
                                            and ##(lat_col)s is not null
                                            and ##(lon_col)s is not null
                                            ) f2
                                        WHERE lat is not null and lon is not null
                                        ) f3
                                    WHERE st_dwithin(p.geom::geography,txt_pt::geography,##(search_rad)f*1609.34)
                                    ) f4
                                order by dist
                                limit 1
                                )

                            UPDATE ##(tbl)s t SET
                                bbl = u.bbl,
                                geom= pc.geom
                            FROM upd u,pluto_centroids pc
                            WHERE u.uid = t.##(gid_col)s
                            and u.bbl = pc.bbl
                            RETURNING u.uid ##(gid_col)s;

                        \"\"\"

                    res,cnt,stopped = [],10,False
                    while len(res)==0:
                        res = plpy.execute(p ## T)
                        cnt -= 1
                        if len(res)>0 or cnt<=0:
                            stopped = True
                            break
                        else:
                            T.update({'search_rad':T['search_rad']+0.005})


                    if len(res)>0:
                        return 'OK'
                    elif stopped==True:
                        return 'nothing updated'
                    else:
                        return 'unknown result'

                $$ LANGUAGE plpythonu;

                -- UID ARRAY FUNCTION

                DROP FUNCTION IF EXISTS z_update_with_geom_from_coords(integer[],text,text,text,text);
                DROP FUNCTION IF EXISTS z_update_with_geom_from_coords(text,text,text,text,text);
                CREATE OR REPLACE FUNCTION z_update_with_geom_from_coords(  limitations text default '',
                                                                            tbl         text default '',
                                                                            gid_col     text default '',
                                                                            lat_col     text default '',
                                                                            lon_col     text default '')
                RETURNS text AS $$

                    T = {   'tbl'       :   tbl,
                            'gid_col'   :   gid_col,
                            'lat_col'   :   lat_col,
                            'lon_col'   :   lon_col,
                            'limits'    :   'where ' + limitations.lower().lstrip('where ').replace("''","'"),
                            'search_rad':   0.0175     }

                    p = \"\"\"

                            WITH upd AS (
                                SELECT *
                                FROM
                                    (
                                    SELECT  uid,bbl,geom,dist,
                                            min(dist) over (partition by uid) as min_thing
                                    FROM
                                        (
                                        SELECT  uid,p.bbl bbl,p.geom geom,
                                                ST_Distance_Spheroid(
                                                        p.geom,
                                                        txt_pt,
                                                        'SPHEROID["WGS 84",6378137,298.257223563]')  dist
                                        FROM
                                            pluto p,
                                            (SELECT uid,st_geomfromtext(concat_ws('','POINT (',lon,' ',lat,')'),4326) txt_pt
                                            from
                                                (
                                                select ##(gid_col)s uid,##(lat_col)s lat,##(lon_col)s lon
                                                from ##(tbl)s
                                                ##(limits)s
                                                ) f2
                                            ) f3
                                        WHERE st_dwithin(p.geom::geography,txt_pt::geography,##(search_rad)f*1609.34)
                                        ) f4
                                    ) f5
                                WHERE dist = min_thing
                                )

                            UPDATE ##(tbl)s t SET
                                bbl = u.bbl,
                                geom= pc.geom
                            FROM upd u,pluto_centroids pc
                            WHERE u.uid = t.##(gid_col)s
                            and u.bbl = pc.bbl
                            RETURNING u.uid ##(gid_col)s;

                        \"\"\"

                    cnt = 10
                    while len(idx)>0:
                        res = plpy.execute(p ## T)

                        for it in res:
                            z=idx.pop(  idx.index( it[ '##(gid_col)s' ## T ] )  )

                        cnt -= 1
                        if len(idx)==0 or cnt<=0:
                            break

                        T.update({  'idx_arr'       :   str(idx).replace("u'","'").replace("'",'').strip('[]'),
                                    'search_rad'    :   T['search_rad']+0.005    })


                    return 'ok'

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_with_geom_from_parsed(self):
            cmd="""
                DROP FUNCTION IF EXISTS z_update_with_geom_from_parsed(integer,text,text);

                CREATE OR REPLACE FUNCTION z_update_with_geom_from_parsed(idx integer,tbl text,gid_col text)
                RETURNS text
                AS $$

                    from traceback                      import format_exc       as tb_format_exc
                    from sys                            import exc_info         as sys_exc_info

                    T = {   'idx'       :   str(idx),
                            'tbl'       :   tbl,
                            'gid_col'   :   gid_col,
                            'idx'       :   str(idx),     }

                    p = \"\"\"  WITH upd AS (
                                    SELECT  p.billbbl,f.uid::bigint src_gid
                                    FROM    pad_adr p,
                                            (   select ##(gid_col)s uid,num,concat_ws(' ',predir,street_name,suftype,sufdir ) concat_addr
                                                from ##(tbl)s where ##(gid_col)s = ##(idx)s   ) f
                                    WHERE   concat_ws(' ',p.predir,p.street_name,p.suftype,p.sufdir ) = f.concat_addr
                                        -- street number within range
                                    AND     (
                                            -- <<
                                            -- street number within range
                                            (   (p.min_num is not null AND p.max_num is not null)
                                                AND (p.min_num <= f.num::double precision and f.num::double precision <= p.max_num)
                                                AND p.parity::integer = (case when mod((f.num::double precision)::integer,2)=1 THEN 1 ELSE 2 END)

                                                    -- parity=2 means even, parity=1 means odd
                                            )
                                            OR
                                            -- street number equals min_max num
                                            (   (p.min_num is null OR p.max_num is null)
                                                AND (p.min_num = f.num::double precision OR p.max_num = f.num::double precision)
                                            )
                                            -- >>
                                            )
                                    )

                                UPDATE ##(tbl)s t set
                                        bbl         =   u.billbbl,
                                        geom        =   pc.geom
                                FROM    upd u, pluto_centroids pc
                                WHERE   u.src_gid   =   t.##(gid_col)s::bigint
                                AND     u.billbbl   =   pc.bbl
                                RETURNING u.src_gid uid

                        \"\"\" ## T

                    try:
                        plpy.log(p)
                        res = plpy.execute(p)
                        if len(res)>0:
                            return 'OK'
                        else:
                            return 'nothing updated'
                    except:
                        plpy.log("f(x) z_update_with_parsed_info FAILED")
                        plpy.log(p)
                        plpy.log(                       tb_format_exc())
                        plpy.log(                       sys_exc_info()[0])
                        return 'ERROR'

                $$ LANGUAGE plpythonu;



                DROP FUNCTION IF EXISTS z_update_with_geom_from_parsed(integer[],text,text);
                CREATE FUNCTION z_update_with_geom_from_parsed(idx integer[],tbl text,gid_col text)
                RETURNS text
                AS $$

                    from traceback                      import format_exc       as tb_format_exc
                    from sys                            import exc_info         as sys_exc_info

                    T = {   'idx'       :   str(idx),
                            'tbl'       :   tbl,
                            'gid_col'   :   gid_col,
                            'idx_arr'   :   str(idx).replace("u'","'").replace("'",'').strip('[]'),     }

                    p = \"\"\"  WITH upd AS (
                                    SELECT  p.billbbl,f.uid::bigint src_gid
                                    FROM    pad_adr p,
                                            (   select ##(gid_col)s uid,num,concat_ws(' ',predir,street_name,suftype,sufdir ) concat_addr
                                                from ##(tbl)s where ##(gid_col)s = any ( array[##(idx_arr)s] )   ) f
                                    WHERE   concat_ws(' ',p.predir,p.street_name,p.suftype,p.sufdir ) = f.concat_addr
                                        -- street number within range
                                    AND     (
                                            -- <<
                                            -- street number within range
                                            (   (p.min_num is not null AND p.max_num is not null)
                                                AND (p.min_num <= f.num::double precision and f.num::double precision <= p.max_num)
                                                AND p.parity::integer = (case when mod((f.num::double precision)::integer,2)=1 THEN 1 ELSE 2 END)

                                                    -- parity=2 means even, parity=1 means odd
                                            )
                                            OR
                                            -- street number equals min_max num
                                            (   (p.min_num is null OR p.max_num is null)
                                                AND (p.min_num = f.num::double precision OR p.max_num = f.num::double precision)
                                            )
                                            -- >>
                                            )
                                    )

                                UPDATE ##(tbl)s t set
                                        bbl         =   u.billbbl,
                                        geom        =   pc.geom
                                FROM    upd u, pluto_centroids pc
                                WHERE   u.src_gid   =   t.##(gid_col)s::bigint
                                AND     u.billbbl   =   pc.bbl

                        \"\"\" ## T

                    try:
                        plpy.execute(p)
                        return 'ok'
                    except:
                        plpy.log("f(x) z_update_with_parsed_info FAILED")
                        plpy.log(p)
                        plpy.log(                       tb_format_exc())
                        plpy.log(                       sys_exc_info()[0])
                        return 'error'

                $$ LANGUAGE plpythonu;

            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return

        def z_update_with_geocode_info(self):
            """

            Usage:

                select z_update_with_geocode_info('yelp','uid','address','postal_code')
                select z_update_with_geocode_info('seamless','id','address','zipcode')

            """
            cmd="""
                DROP FUNCTION IF EXISTS z_update_with_geocode_info(text,text,text,text);
                DROP FUNCTION IF EXISTS z_update_with_geocode_info(integer,text,text,text,text);

                CREATE OR REPLACE FUNCTION z_update_with_geocode_info(  idx         integer,
                                                                        tbl         text,
                                                                        gid_col     text,
                                                                        addr_col    text,
                                                                        zip_col     text)
                RETURNS text AS $$

                    T = {   'idx'       :   idx,
                            'tbl'       :   tbl,
                            'gid_col'   :   gid_col,
                            'addr_col'  :   addr_col,
                            'zip_col'   :   zip_col,   }

                    p = \"\"\"

                            WITH upd AS (
                                SELECT (z).*  --,arr_uid,arr_addr
                                FROM
                                    (
                                    select z_get_geocode_info(array[uid],array[address]) z
                                                --,array_agg(uid) arr_uid,array_agg(address) arr_addr
                                    from
                                        (
                                        select ##(gid_col)s uid,concat_ws(', ',##(addr_col)s,'New York, NY',##(zip_col)s) address
                                        from ##(tbl)s
                                        where ##(gid_col)s = ##(idx)s
                                        and ##(addr_col)s  is not null and address != ''
                                        and ##(zip_col)s  is not null
                                        order by ##(gid_col)s
                                        ) f1
                                    ) f2
                                )
                            UPDATE ##(tbl)s t SET
                                gc_lat = u.lat,
                                gc_lon = u.lon,
                                gc_addr = u.std_addr,
                                gc_zip = u.zipcode,
                                gc_full_addr = u.form_addr
                            FROM upd u
                            WHERE u.addr_valid is true
                            and u.idx = t.##(gid_col)s
                            RETURNING t.##(gid_col)s;

                        \"\"\" ## T

                    #plpy.log(p)
                    res = plpy.execute(p)
                    if len(res)==1:
                        return 'OK'
                    else:
                        return 'nothing updated'

                $$ LANGUAGE plpythonu;
            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_yelp_address_from_valid_display_addr(self):
            cmd="""
                DROP FUNCTION IF EXISTS z_update_yelp_address_from_valid_display_addr();

                CREATE OR REPLACE FUNCTION z_update_yelp_address_from_valid_display_addr()
                RETURNS VOID AS $$
                begin

                    WITH upd as (
                        select src_gid uid, orig_addr new_addr
                        from z_parse_NY_addrs('
                            select uid::bigint gid,repl address,postal_code::bigint zipcode from
                                (select
                                    uid,
                                    regexp_replace(trim(leading address||'', '' from display_address),''^([0-9]+[^,]+)(.*)$'',''\\1'',''g'') repl,
                                    regexp_matches(trim(leading address||'', '' from display_address),''^([0-9]+[^,]+)(.*)$'') matches,
                                    postal_code
                                from yelp
                                where street_name is null and geom is null and address is not null and postal_code is not null) f
                            where length(f.repl)>0
                            order by uid
                            ') z
                        where z.num is not null and z.num != '0' and z.num ~ '^([0-9]+|[.][0-9]+|[0-9]+[.][0-9]+)$'
                        and (z.suftype is not null or ( z.name ilike '%broadway%'
                                                        or z.name ilike '%bowery%'
                                                        or z.name ilike '%slip%' )   )

                        )
                    UPDATE yelp y set address = u.new_addr
                    FROM upd u
                    WHERE y.uid = u.uid;

                end;
                $$ LANGUAGE plpgsql;

            """
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_update_with_parsed_info(self):
            cmd="""
                DROP FUNCTION IF EXISTS z_update_with_parsed_info(integer,text,text,text,text,text[],boolean);
                CREATE OR REPLACE FUNCTION z_update_with_parsed_info(   idx             integer,
                                                                        tbl             text,
                                                                        gid_col         text,
                                                                        addr_col        text,
                                                                        zip_col         text,
                                                                        update_cols     text[]
                                                                            default array[   'num','predir',
                                                                                             'street_name','suftype',
                                                                                             'sufdir'],
                                                                        validity_check  boolean
                                                                            default true)
                RETURNS text AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                T = {   'tbl'       :   tbl,
                        'gid_col'   :   gid_col,
                        'addr_col'  :   addr_col,
                        'zip_col'   :   zip_col,
                        'update'    :   ','.join( ['##s = u.##s' ## (it,it) for it in update_cols] ),
                        'idx'       :   str(idx),
                        'and_valid' :   '',}

                if validity_check:
                    T['and_valid']  =   ' '.join(["AND u.num ~ '^([0-9]+|[.][0-9]+|[0-9]+[.][0-9]+|[0-9]+[A-Z])$'",
                                                  "AND u.num != '0'"
                                                  "AND u.street_name is not null",
                                                  "AND (",
                                                    "u.suftype is not null",
                                                    "or (",
                                                        "u.street_name ilike '####broadway####'",
                                                        "or u.street_name ilike '####bowery####'"
                                                        "or u.street_name ilike '####slip####'",
                                                    ")",
                                                  ")"])

                p = \"\"\"  WITH upd AS (
                                SELECT  src_gid,bldg,box,unit,num,predir,name street_name,suftype,sufdir
                                FROM    z_parse_NY_addrs('
                                                        select
                                                            ##(gid_col)s::bigint gid,
                                                            ##(addr_col)s::text address,
                                                            ##(zip_col)s::bigint zipcode
                                                        FROM ##(tbl)s
                                                        WHERE ##(gid_col)s = ##(idx)s
                                                        ')
                                )

                            UPDATE ##(tbl)s t set
                                ##(update)s
                            FROM  upd u
                            WHERE u.src_gid = t.##(gid_col)s::bigint
                            ##(and_valid)s
                            RETURNING u.src_gid

                    \"\"\" ## T

                try:

                    res = plpy.execute(p)

                    if len(res)>0:
                        if res[0]['src_gid']==idx:
                            return 'OK'
                    else:
                        return 'nothing updated'
                    #plpy.log(res)

                # except UndefinedColumn as e:
                #
                #     new_col_q = \"\"\"
                #                     select regexp_replace('"+e+"',
                #                         '(column [[:alnum:]]+[[:period:]])([^%s])([%s][does not exist]',
                #                         '%1')
                #                 \"\"\"
                #     T.update({ 'new_col': plpy.execute(new_col_q) })
                #
                #     ps1 = 'alter table ##(tbl)s add column %(new_col)s %(new_col_info)s' % T
                except:
                    plpy.log(tbl)
                    plpy.log(                       "f(x) z_update_with_parsed_info FAILED")
                    plpy.log(                       tb_format_exc())
                    plpy.log(                       sys_exc_info())
                    return 'WHAT2'
                    # return '\\n\\n'.join([ 'ERROR:'] + **tb_format_exc() + **sys_exc_info() ])


                $$ LANGUAGE plpythonu;

                -- << INTEGER [] >> --

                DROP FUNCTION IF EXISTS z_update_with_parsed_info(integer[],text,text,text,text,text[],boolean);

                CREATE OR REPLACE FUNCTION z_update_with_parsed_info(   idx             integer[],
                                                                        tbl             text,
                                                                        gid_col         text,
                                                                        addr_col        text,
                                                                        zip_col         text,
                                                                        update_cols     text[],
                                                                        validity_check  boolean
                                                                            default     false)
                RETURNS text
                AS $$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                T = {   'tbl'       :   tbl,
                        'gid_col'   :   gid_col,
                        'addr_col'  :   addr_col,
                        'zip_col'   :   zip_col,
                        'update'    :   ','.join( ['##s = u.##s' ## (it,it) for it in update_cols] ),
                        'idx_arr'   :   str(idx).replace("u'","'").replace("'",'').strip('[]'),
                        'and_valid' :   '',}

                if validity_check:
                    T['and_valid']  =   ' '.join(["AND u.num ~ '^([0-9]+|[.][0-9]+|[0-9]+[.][0-9]+)$'",
                                                  "AND u.num != '0'"
                                                  "AND u.street_name is not null",
                                                  "AND (",
                                                    "u.suftype is not null",
                                                    "or (",
                                                        "u.street_name ilike '####broadway####'",
                                                        "or u.street_name ilike '####bowery####'"
                                                        "or u.street_name ilike '####slip####'",
                                                    ")",
                                                  ")"])

                error_occurred      =   False

                p = \"\"\"  WITH upd AS (
                                SELECT  src_gid,bldg,box,unit,num,predir,name street_name,suftype,sufdir
                                FROM    z_parse_NY_addrs('
                                                        select
                                                            ##(gid_col)s::bigint gid,
                                                            ##(addr_col)s::text address,
                                                            ##(zip_col)s::bigint zipcode
                                                        FROM ##(tbl)s
                                                        WHERE ##(gid_col)s = any ( array[##(idx_arr)s] )
                                                        ')
                                )
                            UPDATE ##(tbl)s t set
                                ##(update)s
                            FROM  upd u
                            WHERE u.src_gid = t.##(gid_col)s::bigint
                            ##(and_valid)s
                            RETURNING u.src_gid

                    \"\"\" ## T

                try:
                    res = plpy.execute(p)
                    #plpy.log(res)
                    #t = str([it['src_gid'] for it in res[0]])
                    #plpy.log(t)

                except:
                    plpy.log(                       "f(x) z_update_with_parsed_info FAILED")
                    #plpy.log(                       "##(gid_col)s =  ##(idx_arr)s" ## T)
                    plpy.log(                       tb_format_exc())
                    plpy.log(                       sys_exc_info())
                    error_occurred = True

                if error_occurred:
                    return 'Finished, but errors logged'
                else:
                    return




                $$ LANGUAGE plpythonu;

            """.replace('##','%')
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return

        def z_parse_NY_addrs(self):
            T = {'fct_name'                 :   'z_parse_NY_addrs',
                 'fct_args_types'           :   [ ['IN','query_str','text'],],
                 'fct_return'               :   'SETOF parsed_addr',
                 'fct_lang'                 :   'plpythonu',
                 'cmt'                      :   '\n'.join([ "Example:",
                                                            "select * from z_parse_NY_addrs(",
                                                            "    ''select gid,address,zipcode",
                                                            "     from pluto",
                                                            "     where address is not null order by gid''",
                                                            ");"])}
            T.update( {'fct_args'           :   ', '.join([' '.join(j) for j in T['fct_args_types']]),
                       'fct_types'          :   ', '.join([j[2] for j in T['fct_args_types'] if j[0].upper()!='OUT']),
                       })

            for it in T['fct_args'].split(','):
                arg=it.strip().split(' ')[0]
                T.update({arg:arg})

            a="""

                DROP TYPE IF EXISTS parsed_addr CASCADE;
                DROP FUNCTION IF EXISTS %(fct_name)s( %(fct_types)s );

                CREATE TYPE parsed_addr AS (
                    src_gid bigint,
                    orig_addr text,
                    bldg text,
                    box text,
                    unit text,
                    num text,
                    pretype text,
                    qual text,
                    predir text,
                    name text,
                    suftype text,
                    sufdir text,
                    city text,
                    state text,
                    zip text
                );


                CREATE OR REPLACE FUNCTION public.%(fct_name)s( %(fct_args)s )
                RETURNS %(fct_return)s
                AS $$

                    from traceback                      import format_exc       as tb_format_exc
                    from sys                            import exc_info         as sys_exc_info

                    query_str = args[0]
                    qs,res = query_str.lower().replace('\\n',' ').replace('####','##').replace("''","'").split(' '),[]
                    drop_items = []

                    if qs.count('offset')>0:
                        q_offset = int(qs[qs.index('offset')+1])
                        drop_items.extend(['offset',str(q_offset)])
                    else:
                        q_offset = 0

                    if qs.count('limit')>0:
                        q_max = int(qs[qs.index('limit')+1])
                        drop_items.extend(['limit',str(q_max)])
                    else:
                        q_max = -1

                    if 0<q_max<=100:
                        q_lim = q_max
                    else:
                        q_lim = 100

                    drop_idx = sorted([qs.index(it) for it in drop_items])
                    drop_idx.reverse()
                    for it in drop_idx:
                        z=qs.pop(it)

                    query_str = ' '.join(qs)

                    stop=False

                    pt=0
                    while stop==False:
                        pt+=1

                        if q_max>0:
                            if q_offset + q_lim > q_max:
                                q_lim = q_max - q_offset

                        q_range = 'OFFSET ##s LIMIT ##s' ## (q_offset,q_lim)

                        query_dict = {   '_QUERY_STR'        :   query_str + ' ' + q_range, }


                        #plpy.log(str(pt)+' '+query_dict['_QUERY_STR'])

                        q=\"\"\"
                            select (res).*
                            from
                                (
                                select  z_custom_addr_post_filter(

                                            standardize_address('tiger.pagc_lex','tiger.pagc_gaz',
                                                                    'tiger.pagc_rules',f2.addr_zip),
                                            f2.orig_addr,
                                            f2.src_gid
                                            ) res
                                from
                                    (
                                    select
                                        z_custom_addr_pre_filter( f1.address,f1.zipcode ) addr_zip,
                                        f1.address orig_addr,
                                        f1.gid src_gid
                                    from
                                        (
                                        ##(_QUERY_STR)s
                                        ) as f1
                                    ) as f2
                                ) as f3
                        \"\"\" ## query_dict
                        #plpy.log(q)
                        try:
                            q_res = plpy.execute(q)
                            #plpy.log(q)

                            res.extend(q_res)

                            if len(q_res)<q_lim or len(res)==q_max:
                                #plpy.log('exit 1623')
                                stop=True
                                break
                            else:
                                q_offset = q_offset + q_lim
                        except:
                            plpy.log(                       "z_parse_NY_addrs FAILED")
                            plpy.log(                       tb_format_exc())
                            plpy.log(                       sys_exc_info()[0])
                            break


                    return res


                $$ LANGUAGE %(fct_lang)s;

            COMMENT ON FUNCTION public.%(fct_name)s(%(fct_types)s) IS '%(cmt)s';
            """ % T
            cmd                         =   a.replace('##','%')
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
        def z_custom_addr_post_filter(self):
            cmd="""

                DROP FUNCTION IF EXISTS z_custom_addr_post_filter(stdaddr,text,integer);
                DROP FUNCTION IF EXISTS z_custom_addr_post_filter(stdaddr[],text[],integer[]);
                CREATE OR REPLACE FUNCTION z_custom_addr_post_filter(   res stdaddr,
                                                                        orig_addr text,
                                                                        src_gid bigint)
                RETURNS parsed_addr AS $$

                    function nocase (s)
                        s                       =   string.gsub(s, "%a", function (c)
                                                        return string.format(   "[%s%s]",
                                                                                string.lower(c),
                                                                                string.upper(c))
                                                        end)
                        return s
                    end


                    local some_src_cols         =   {   "building","house_num","predir","qual",
                                                        "pretype","name","suftype","sufdir",
                                                        "city","state","postcode","box","unit",     }

                    local some_dest_cols        =   {   "bldg","num","predir","qual","pretype",
                                                        "name","suftype","sufdir",
                                                        "city","state","zip","box","unit"           }

                    tmp_pt                      =   {}
                    local tmp                   =   res
                    local tmp_col               =   ""

                    for k,v in pairs(some_dest_cols) do
                        tmp_col                 =   some_src_cols[k]
                        tmp_pt[v]               =   tmp[tmp_col]
                    end

                    tmp_pt["src_gid"]           =   src_gid
                    orig_addr                   =   orig_addr:upper()
                    tmp_pt["orig_addr"]         =   orig_addr
                    tmp_pt["zip"]               =   tmp.postcode


                    -- CLEAN UP TEMP SUBSTITUTION {Qx5 = no space, Qx4 = space} < -- OUTPUT

                    if tmp_pt["name"] then
                        if tmp_pt["name"]:find("QQQQQ") then
                            tmp_pt["name"]      =   tmp_pt["name"]:gsub("(QQQQQ)","")
                        end
                        if tmp_pt["name"]:find("QQQQ") then
                            tmp_pt["name"]      =   tmp_pt["name"]:gsub("(QQQQ)"," ")
                        end
                        if tmp_pt["name"]:find("AVENUE OF THE") then
                            tmp_pt["suftype"]   =   "AVE"
                        end
                    end


                    local t                     =   ""
                    local s1,e1,s2,e2           =   0,0,0,0

                    -- DISCARD PRETYPES, MOVE THEM BACK TO 'NAME', UPDATE SUFTYPE
                    if tmp_pt["pretype"] then

                        if tmp_pt["num"]==0 then
                            s1,e1               =   0,0
                        else
                            s1,e1               =   orig_addr:find(tmp_pt["num"])
                        end

                        if e1==nil then
                            log(                    "ERROR FOUND: z_custom_addr_post_filter 1")
                            log(                    "orig_addr: "..orig_addr)
                            log(                    "GID: "..tostring(src_gid))
                            if true then return end
                        end
                        if not orig_addr then
                            log(                    "ERROR FOUND: z_custom_addr_post_filter 2")
                            log(                    "orig_addr: "..orig_addr)
                            log(                    "GID: "..tostring(src_gid))
                            if true then return end
                        end

                        s2,e2                   =   orig_addr:find(tmp_pt["name"])

                        if s2==nil then
                            log(                    "ERROR FOUND: z_custom_addr_post_filter 3")
                            log(                    "orig_addr: "..orig_addr)
                            log(                    "pretype: "..tmp_pt["pretype"])
                            log(                    "name: "..tmp_pt["name"])
                            log(                    "GID: "..tostring(src_gid))
                            if true then return end
                        end

                        t                       =   orig_addr:sub(e1+2,s2-2)

                        -- if this string has a space, meaning at least two words, take only last word
                        if t:find("[%s]")==nil then
                            tmp_pt["name"]      =   t.." "..tmp_pt["name"]
                        else
                            t                   =   t:gsub("(.*)%s([a-zA-Z0-9]+)$","%2")
                            tmp_pt["name"]      =   t.." "..tmp_pt["name"]
                        end
                        tmp_pt["pretype"] = nil

                        t                       =   orig_addr:sub(e2+2)

                        cmd                     =   string.format([[    select usps_abbr abbr
                                                                        from usps
                                                                        where common_use ilike '%s'
                                                                    ]],t)
                        for row in server.rows(cmd) do
                            t                   =   row.abbr
                            break
                        end
                        tmp_pt["suftype"]       =   t:upper()

                    end

                    -- FOR ANY PREDIR NOT 'E' OR 'W', MOVE BACK TO 'NAME'
                    if tmp_pt["predir"] and (tmp_pt["predir"]~="E" and tmp_pt["predir"]~="W") then

                        if tmp_pt["predir"]=='N' then t = "NORTH" end
                        if tmp_pt["predir"]=='S' then t = "SOUTH" end

                        tmp_pt["predir"]        =   nil
                        tmp_pt["name"]          =   t.." "..tmp_pt["name"]
                    end

                    -- WHEN UNIT CONTAINS 'PIER', e.g., 'PIER-15 SOUTH STREET' (filtered as '0 PIER-15 SOUTH STREET')
                    if tmp_pt["unit"] then
                        if tmp_pt["unit"]:find('# 0 PIER') and tmp_pt["bldg"]==nil then
                            tmp_pt["bldg"]      =   "PIER "..tmp_pt["num"]
                            tmp_pt["num"]       =   0
                            tmp_pt["unit"]      =   nil
                        end
                    end

                    -- IF num,predir, and name==number but no suftype, add it.
                    if not tmp_pt["suftype"] then
                        if type(tmp_pt["name"])==type(11) then
                            if ( tmp_pt["predir"]=='E' or tmp_pt["predir"]=='W' ) then
                                tmp_pt["suftype"]       =   "ST"
                                -- NOT DOING AVENUES BECAUSE SO FEW AND HIGHER ODDS OF THIS BEING A MISTAKE
                            end
                        end
                    end

                    -- FOR ANY BUILDING {letter}, MOVE BACK TO END OF NUM, e.g., BUILDING A, 1665 3RD AVENUE
                    --                                                              --> 1665A 3RD AVENUE
                    if tmp_pt["bldg"] then
                        if tmp_pt["bldg"]:find("BUILDING [A-Z]") then
                            tmp_pt["num"] = tmp_pt["num"]..tmp_pt["bldg"]:gsub("(BUILDING )([A-Z])","%2")
                        end
                        tmp_pt["bldg"] = nil
                    end


                    if tmp_pt["name"] then

                    --  Return Abbreviated Street Names Back to Original Name, e.g., 'W' for West St., 'S' for South St.
                        if #tmp_pt["name"]==1 then
                            if      tmp_pt["name"]=='N' then tmp_pt["name"]='NORTH'
                            elseif  tmp_pt["name"]=='E' then tmp_pt["name"]='EAST'
                            elseif  tmp_pt["name"]=='S' then tmp_pt["name"]='SOUTH'
                            elseif  tmp_pt["name"]=='W' then tmp_pt["name"]='WEST'
                            end
                        end

                    --  Remove suftype for certain 'streets' that shouldn't have a suftype, e.g., broadway,bowery,'a slip'
                        if (    tmp_pt["name"]:find("BROADWAY$")    or
                                tmp_pt["name"]:find("BOWERY$")      or
                                tmp_pt["name"]:find(" SLIP$")       )   then
                            tmp_pt["suftype"]           =   nil
                        end

                    --  LAST CHANGE HERE

                    --  Return null result if street name i_contains 'and'
                        local a = tmp_pt["name"]
                        if (   a:find("^(.*)[%s]AND[%s](.*)$")
                            or a:find("^AND[%s].*$")
                            or a:find("^.*[%s]AT[%s].*$")
                            or a:find("^.*[%s]&[%s].*$")
                            or a:find("^[0-9]+.*[%s]+.*[%s]+[0-9]+.*$")
                            ) then
                           return nil
                        end

                    end

                    return tmp_pt

                $$ LANGUAGE plluau;
            """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
        def z_custom_addr_post_filter_with_iter(self):
            a=0
            # cmd="""
            #
            #     DROP FUNCTION IF EXISTS z_custom_addr_post_filter_with_iter(stdaddr[],text[],integer[]);
            #     CREATE OR REPLACE FUNCTION z_custom_addr_post_filter_with_iter(   res stdaddr[],
            #                                                             orig_addr text[],
            #                                                             src_gid integer[])
            #     RETURNS SETOF parsed_addr AS $$
            #         return _U(res,orig_addr,src_gid)
            #     end
            #     do
            #         _U = function(res,orig_addr,src_gid)
            #
            #         local some_src_cols = {"building","house_num","predir","qual","pretype","name","suftype","sufdir",
            #                                 "city","state","postcode","box","unit",}
            #
            #         local some_dest_cols = {"bldg","num","predir","qual","pretype","name","suftype","sufdir",
            #                                 "city","state","zip","box","unit"}
            #
            #
            #         for i=1, #res do
            #
            #             local tmp = res[i]
            #             local tmp_pt = {}
            #             local tmp_col = ""
            #
            #             for k,v in pairs(some_dest_cols) do
            #                 tmp_col = some_src_cols[k]
            #                 tmp_pt[v]=tmp[tmp_col]
            #             end
            #             tmp_pt["src_gid"] = src_gid[i]
            #             tmp_pt["orig_addr"] = orig_addr[i]
            #             tmp_pt["zip"]=tmp.postcode
            #
            #             -- CLEAN UP TEMP SUBSTITUTION {Qx5 = no space, Qx4 = space} < -- OUTPUT
            #             if tmp_pt["name"] then
            #                 if tmp_pt["name"]:find("QQQQQ") then
            #                     tmp_pt["name"] = tmp_pt["name"]:gsub("(QQQQQ)","")
            #                 end
            #                 if tmp_pt["name"]:find("QQQQ") then
            #                     tmp_pt["name"] = tmp_pt["name"]:gsub("(QQQQ)"," ")
            #                 end
            #                 if tmp_pt["name"]:find("AVENUE OF THE") then
            #                     tmp_pt["suftype"] = "AVE"
            #                 end
            #             end
            #
            #             local t = ""
            #             local s1,e1,s2,e2 = 0,0,0,0
            #
            #             -- DISCARD PRETYPES, MOVE THEM BACK TO 'NAME', UPDATE SUFTYPE
            #             if tmp_pt["pretype"] then
            #                 --log("1702")
            #                 if tmp_pt["num"]==0 then
            #                     s1,e1=0,0
            #                 else
            #                     s1,e1 = orig_addr[i]:find(tmp_pt["num"])
            #                 end
            #
            #                 if e1==nil then
            #                     log(tmp_pt["num"])
            #                     log(tmp_pt["name"])
            #                     log(orig_addr[i])
            #                     log(tmp_pt["pretype"])
            #                     log(src_gid[i])
            #                     if true then return end
            #                 end
            #
            #                 s2,e2 = orig_addr[i]:find(tmp_pt["name"])
            #
            #                 if s2==nil then
            #                     log(tmp_pt["num"])
            #                     log(tmp_pt["name"])
            #                     log(orig_addr[i])
            #                     log(tmp_pt["pretype"])
            #                     log(src_gid[i])
            #                     if true then return end
            #                 end
            #
            #                 t = orig_addr[i]:sub(e1+2,s2-2)
            #
            #                 -- if this string has a space, meaning at least two words, take only last word
            #                 if t:find("[%s]")==nil then
            #                     tmp_pt["name"] = t.." "..tmp_pt["name"]
            #                 else
            #                     t = t:gsub("(.*)%s([a-zA-Z0-9]+)$","%2")
            #                     tmp_pt["name"] = t.." "..tmp_pt["name"]
            #                 end
            #                 tmp_pt["pretype"] = nil
            #
            #                 t = orig_addr[i]:sub(e2+2)
            #
            #                 cmd = string.format([[  select usps_abbr abbr
            #                                         from usps where common_use ilike '%s']],t)
            #
            #                 for row in server.rows(cmd) do
            #                     t = row.abbr
            #                     break
            #                 end
            #
            #                 tmp_pt["suftype"] = t:upper()
            #
            #             end
            #
            #             -- FOR ANY PREDIR NOT 'E' OR 'W', MOVE BACK TO 'NAME'
            #             if tmp_pt["predir"] and (tmp_pt["predir"]~="E" and tmp_pt["predir"]~="W") then
            #                 --log("1742")
            #                 if tmp_pt["predir"]=='N' then t = "NORTH" end
            #                 if tmp_pt["predir"]=='S' then t = "SOUTH" end
            #
            #                 tmp_pt["predir"] = nil
            #                 tmp_pt["name"] = t.." "..tmp_pt["name"]
            #             end
            #
            #             -- WHEN UNIT CONTAINS 'PIER', e.g., 'PIER-15 SOUTH STREET' (filtered as '0 PIER-15 SOUTH STREET')
            #             if tmp_pt["unit"] then
            #                 if tmp_pt["unit"]:find('# 0 PIER') and tmp_pt["bldg"]==nil then
            #                     tmp_pt["bldg"] = "PIER "..tmp_pt["num"]
            #                     tmp_pt["num"] = 0
            #                     tmp_pt["unit"] = nil
            #                 end
            #             end
            #
            #             coroutine.yield(tmp_pt)
            #         end
            #
            #     end
            #
            #     $$ LANGUAGE plluau;
            # """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
        def z_custom_addr_pre_filter(self):
            """

            Most of these should really be systematically created from the NYC Street Name Dictionary (SND)

            See here: http://www.nyc.gov/html/dcp/html/bytes/applbyte.shtml#geocoding_application

            """
            cmd="""
                drop function if exists z_custom_addr_pre_filter(text);
                CREATE OR REPLACE FUNCTION z_custom_addr_pre_filter(addr text, zipcode bigint)
                RETURNS text AS $$

                    if addr==nil then
                        return
                    else
                        addr = addr:upper()
                    end

                    local cnt = addr:find( "^([0-9]*)([%-]*)([a-zA-Z0-9]*)%s([a-zA-Z0-9]*)(.*)" )

                    local no_num_cnt = addr:find("^([0-9]+)(.*)")
                    -- when first character not digit, EAST 76 STREET --> num=E 76,NAME=NEW YORK
                    -- when first character not digit, MARGINAL STREET --> NAME=ST NEW YORK

                    local no_num_but_avenue_cnt = addr:find("^([1]?[0-9][A-Z]?[A-Z]?[A-Z]? AVEN?U?E?)(.*)")
                    -- e.g., "5TH AVE CENTRAL PARK S"


                    if ( cnt == 0 or cnt == nil or no_num_cnt == nil or no_num_but_avenue_cnt~=nil ) then
                        addr = "0|"..addr
                    else
                        addr = addr:gsub("^([0-9]*)([%-]*)([a-zA-Z0-9]*)[%s]*([a-zA-Z0-9]*)[%s]*(.*)",
                                         "%1%2%3|%4 %5")
                    end

                    local cmd = [[select repl_from,repl_to
                            from regex_repl
                            where tag = 'custom_addr_pre_filter'
                            and is_active is true
                            order by run_order ASC]]

                    for row in server.rows(cmd) do
                        addr = string.gsub(addr,row.repl_from,row.repl_to)
                    end

                    if zipcode==nil or zipcode==0 then
                        zipcode=11111
                    end

                    return addr..", New York, NY, "..tostring(zipcode)
                $$ LANGUAGE plluau;
            """
            self.T.to_sql(                      cmd)

        def OLD_z_add_geom_through_addr_idx(self):
            a="""
                DROP FUNCTION if exists z_add_geom_through_addr_idx(text,text) cascade;

                CREATE OR REPLACE FUNCTION z_add_geom_through_addr_idx(tbl text,uid_col text)
                RETURNS text AS $funct$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                try:

                    p = \"\"\"

                        -- COPY BBL VALUE WHERE A MATCH EXISTS AND VALUE OF ADDRESS NUMBER EQUAL/BETWEEN EXISTING DB PTS
                        update %(tbl)s s set bbl = l.bbl
                            from lot_pts l
                            where
                                char_length(l.bbl::text)        >=  10
                                and s.bldg_street_idx is not null
                                and s.num is not null
                                and (
                                    to_number(concat(s.bldg_street_idx,'.',to_char(s.num::integer,'00000')),'00000.00000')
                                    between l.lot_idx_start and l.lot_idx_end
                                    );
                                and s.bbl is null


                        -- COPY BBL VALUE WHERE A MATCH EXISTS BUT THE NEW ADDRESS HAS STREET NUMBER EXCEEDING THE DB IDX
                        with upd as (   select *
                                        from
                                            (
                                            select
                                                %(uid)s,bldg_street_idx,l.bbl,l.lot_idx_end,
                                                max(l.lot_idx_end) over (partition by bldg_street_idx) as max_thing
                                            from    lot_pts l,
                                                    %(tbl)s t
                                            where t.bldg_street_idx is not null
                                                and regexp_replace(lot_idx_end::text,'^([0-9]{1,5})\.([0-9]{1,5})\$',
                                                            '\\1','g')::numeric
                                                            = t.bldg_street_idx::numeric
                                             ) f1
                                        where lot_idx_end = max_thing       )
                        update %(tbl)s t set bbl = d.bbl from upd d where t.%(uid)s = d.%(uid)s;


                        -- COPY OVER LOT COUNTS
                        update %(tbl)s s set lot_cnt = (select count(l.bbl) from %(tbl)s l
                                                             where s.bbl is not null
                                                             and l.bbl is not null
                                                             and l.bbl=s.bbl);


                        -- COPY OVER GEOM FOR MATCHING BBL
                        update %(tbl)s s set geom = pc.geom
                            from pluto_centroids pc
                            where   pc.bbl = s.bbl
                                    and pc.bbl is not null;

                        \"\"\"  ## {"tbl"                       :   tbl,
                                    "uid"                       :   uid_col  }

                    plpy.execute(p)

                except Exception, Err:
                    plpy.log('z_add_geom_through_addr_idx FUNCTION FAILED')
                    plpy.log("table: " + tbl)
                    plpy.log(tb_format_exc())
                    plpy.log(sys_exc_info()[0])
                    return
                return

                $funct$ language "plpythonu";

            """

            cmd                                 =   a.replace("##","%")
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            print cmd
            return
        def z_make_rows_with_alpha_range_lua_not_working(self):
            """
                z_make_rows_with_alpha_range('idx_col','start_range_col','end_range_col')

                EXAMPLE:
                    select z_make_rows_with_alpha_range(array[1,2,3],array['A','A','C'],array['B','C','E']);


                RETURNS:

                        idx_col =  [1           rows_ranges     =  [A,
                                    1,                              B,
                                    2,                              A,
                                    2,                              B,
                                    2,                              C,
                                    3,                              B,
                                    3,                              C,
                                    3,                              D,
                                    3]                              E]
            """
            cmd="""
                drop function if exists z_make_rows_with_alpha_range(int[],text[],text[]) cascade;
                drop function if exists z_make_rows_with_alpha_range(int,text,text) cascade;
                drop type if exists alpha_range_type;
                CREATE TYPE alpha_range_type as (
                    uid int,
                    alpha_range text
                );
                CREATE FUNCTION z_make_rows_with_alpha_range(IN uid int,IN start_r text,IN end_r text,
                                                             OUT uid_r int[], OUT alphas text[])
                AS $$
                    alphas={}
                    uid_r = {}
                    j=0
                    for j=start_r:byte(1), end_r:byte(1) do

                        if not alphas then
                            uid_r[1] = uid
                            alphas[1]=string.char(j,1)
                        else
                            uid_r[#uid_r+1] = uid
                            alphas[#alphas+1] = string.char(j,1)
                        end

                    end
                    log(#uid_r)
                    log(#alphas)
                    res = {}
                    res.uid = uid_r
                    res.alpha_range = alphas
                    --res[1] = uid_r
                    --res[2] = alphas
                    return --{uid_r,alphas}
                $$ LANGUAGE plluau;
            """
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            print cmd
            return
        def z_make_rows_with_numeric_range(self):
            """
                z_make_rows_with_numeric_range('idx_col','start_range_col','end_range_col')

                EXAMPLE:
                    select numeric_range(array[1,2,3],array[8,9,10],array[10,10,10]);


                RETURNS:

                        idx_col =  [1           rows_ranges     =  [8,
                                    1,                              9,
                                    1,                              10,
                                    2,                              9,
                                    2,                              10]
            """
            cmd="""

                drop function if exists z_make_rows_with_numeric_range(int,int,int,boolean) cascade;

                drop type if exists numeric_range_type;

                CREATE TYPE numeric_range_type as (
                    uid int,
                    res_i int
                );

                CREATE OR REPLACE FUNCTION z_make_rows_with_numeric_range(  IN uid integer,
                                                                            IN start_num integer,
                                                                            IN end_num integer,
                                                                            IN w_parity boolean default false)
                RETURNS SETOF numeric_range_type AS $$

                    class numeric_range_type:
                        def __init__(self,uid,res_i):
                            self.uid = uid
                            self.res_i = res_i


                    for j in range(start_num,end_num+1):
                        if w_parity:
                            if ((j % 2==0) == (start_num % 2==0)):
                                yield( numeric_range_type(uid,j) )
                        else:
                            yield( numeric_range_type(uid,j) )

                    return

                $$ LANGUAGE plpythonu;
            """
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_make_rows_with_alpha_range(self):
            """
                z_make_rows_with_alpha_range('idx_col','start_range_col','end_range_col')

                z_make_rows_with_alpha_range(uid,base_str,start_r,end_r,first_empty)

                EXAMPLE:
                    select z_make_rows_with_alpha_range(uid,base_str,'A','C',false)

                RETURNS:

                        idx_col =  [1           rows_ranges     =  [A,
                                    1,                              B,
                                    2,                              A,
                                    2,                              B,
                                    2,                              C,
                                    3,                              B,
                                    3,                              C,
                                    3,                              D,
                                    3]                              E]
            """
            cmd="""
                drop function if exists z_make_rows_with_alpha_range(int,text,text,text,boolean) cascade;
                drop type if exists alpha_range_type;
                CREATE TYPE alpha_range_type as (
                    uid int,
                    alpha_range text
                );
                CREATE OR REPLACE FUNCTION z_make_rows_with_alpha_range(uid int,base_str text,start_r text,end_r text,first_empty boolean)
                RETURNS SETOF alpha_range_type AS $$

                    class alpha_range_type:
                        def __init__(self,uid,alpha_range):
                            self.uid = uid
                            self.alpha_range = alpha_range

                    if first_empty:
                        yield ( alpha_range_type(uid,base_str) )


                    for j in range(ord(start_r),ord(end_r)+1):
                        yield ( alpha_range_type(uid,base_str + chr(j) ) )

                $$ LANGUAGE plpythonu;
            """
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_get_geocode_info(self):
            cmd="""
                DROP TYPE IF EXISTS geocode_results cascade;
                CREATE TYPE geocode_results as (
                    idx integer,
                    addr_valid boolean,
                    partial_match boolean,
                    form_addr text,
                    std_addr text,
                    zipcode bigint,
                    lat double precision,
                    lon double precision

                );

                drop function if exists z_get_geocode_info(integer[],text[]);
                CREATE FUNCTION z_get_geocode_info(     uids            integer [],
                                                        addr_queries    text[])
                RETURNS SETOF geocode_results AS $$

                from os                             import environ as os_environ
                from sys                            import path             as py_path
                py_path.append(                     os_environ['PWD'] +
                                                        '/SERVER2/ipython/ENV/lib/python2.7/site-packages/')
                from pygeocoder                     import Geocoder,GeocoderError
                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                class geocode_results:

                    def __init__(self,upd=None):
                        if upd:
                            self.__dict__.update(       upd)


                def get_gc_info(it):

                    try:
                        r                           =   Geocoder.geocode(it)
                        return 'ok',r
                    except GeocoderError as e:
                        return 'failed',[]
                    except:
                        plpy.log(                       tb_format_exc())
                        plpy.log(                       sys_exc_info()[0])
                        plpy.log(                       e)
                        return 'failed',[]

                try:

                    for j in range(len(addr_queries)):
                        idx                         =   uids[j]
                        it                          =   addr_queries[j]
                        status,results              =   get_gc_info(it)
                        _out                        =   None

                        if not results:
                            _out                    =   None

                        elif results.len > 1:

                            found = False
                            for i in range(0,results.len):

                                res                 =   results[i]

                                if not res.valid_address:
                                    _out            =   None
                                    break


                                else:
                                    r_data          =   res.data[0]
                                    r_data_c        =   r_data['address_components']
                                    component_list  =   map(lambda s: s['types'][0],r_data_c)
                                    found           =   True
                                    t               =   {'idx'                  :   idx,
                                                         'addr_valid'           :   res.valid_address,
                                                         'partial_match'        :   False if not r_data.has_key('partial_match') else True if res.valid_address else r_data['partial_match'],
                                                         'form_addr'            :   res.formatted_address,
                                                         'std_addr'             :   ' '.join([  r_data_c[component_list.index('street_number')]['long_name'],
                                                                                                r_data_c[component_list.index('route')]['long_name'] ]),
                                                         'zipcode'              :   int(r_data_c[component_list.index('postal_code')]['long_name']),
                                                         'lat'                  :   float(res.latitude),
                                                         'lon'                  :   float(res.longitude),
                                                         }

                                    r               =   geocode_results(t)
                                    yield(              r)

                            if not found:
                                _out                =   None



                        else:
                            res                     =   results
                            if not res.valid_address:
                                _out                =   None
                            else:
                                r_data              =   res.data[0]
                                r_data_c            =   r_data['address_components']
                                component_list      =   map(lambda s: s['types'][0],r_data_c)
                                partial_option      =   True if r_data.keys().count('partial_match') != 0 else False
                                t                   =   {'idx'                  :   idx,
                                                         'addr_valid'           :   res.valid_address,
                                                         'partial_match'        :   False if not r_data.has_key('partial_match') else r_data['partial_match'],
                                                         'form_addr'            :   res.formatted_address,
                                                         'std_addr'             :   ' '.join([  r_data_c[component_list.index('street_number')]['long_name'],
                                                                                                r_data_c[component_list.index('route')]['long_name'] ]),
                                                         'zipcode'              :   int(r_data_c[component_list.index('postal_code')]['long_name']),
                                                         'lat'                  :   float(res.latitude),
                                                         'lon'                  :   float(res.longitude),
                                                         }
                                r                   =   geocode_results(t)
                                _out                =   r


                        yield(                          _out)

                except plpy.SPIError, e:
                    plpy.log(                           'GEOCODING FAILED')
                    plpy.log(                           tb_format_exc())
                    plpy.log(                           sys_exc_info()[0])
                    plpy.log(                           e)
                    yield(                              None)



                $$ LANGUAGE plpythonu;
            """
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return

class pgSQL_Triggers:

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    class Exists:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def event_trigger(self,trigger_name):
            qry                             =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_event_trigger
                                                    WHERE evtname='%s'
                                                    AND evtenabled='O');
                                                """ % trigger_name
            return                              self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Enabled:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def event_trigger(self,trigger_name):
            qry                             =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM pg_event_trigger
                                                    WHERE evtname='%s');
                                                """ % trigger_name
            return                              self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Create:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_auto_add_primary_key(self):
            self.T.z_next_free()
            c                           =   """
                DROP FUNCTION if exists z_auto_add_primary_key() CASCADE;

                CREATE OR REPLACE FUNCTION z_auto_add_primary_key()
                    RETURNS event_trigger AS
                $BODY$
                DECLARE
                    has_index boolean;
                    tbl_name text;
                    primary_key_col text;
                    missing_primary_key boolean;
                    has_uid_col boolean;
                    _seq text;
                BEGIN
                    select relhasindex,relname into has_index,tbl_name
                        from pg_class
                        where relnamespace=2200
                        and relkind='r'
                        order by oid desc limit 1;
                    IF (
                        pg_trigger_depth()=0
                        and has_index=False )
                    THEN
                        --RAISE NOTICE 'NOT HAVE INDEX';
                        EXECUTE format('SELECT a.attname
                                        FROM   pg_index i
                                        JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                                             AND a.attnum = ANY(i.indkey)
                                        WHERE  i.indrelid = ''%s''::regclass
                                        AND    i.indisprimary',tbl_name)
                        INTO primary_key_col;

                        missing_primary_key = (select primary_key_col is null);

                        IF missing_primary_key=True
                        THEN
                            --RAISE NOTICE 'IS MISSING PRIMARY KEY';
                            _seq = format('%I_uid_seq',tbl_name);
                            EXECUTE format('select count(*)!=0
                                        from INFORMATION_SCHEMA.COLUMNS
                                        where table_name = ''%s''
                                        and column_name = ''uid''',tbl_name)
                            INTO has_uid_col;
                            IF (has_uid_col=True)
                            THEN
                                --RAISE NOTICE 'HAS UID COL';
                                execute format('alter table %I
                                                    alter column uid type integer,
                                                    alter column uid set not null,
                                                    alter column uid set default z_next_free(
                                                        ''%I''::text,
                                                        ''uid''::text,
                                                        ''%I''::text),
                                                    ADD PRIMARY KEY (uid)',tbl_name,tbl_name,'_seq');
                            ELSE
                                --RAISE NOTICE 'NOT HAVE UID COL';
                                _seq = format('%I_uid_seq',tbl_name);
                                execute format('alter table %I add column uid integer primary key
                                                default z_next_free(
                                                        ''%I''::text,
                                                        ''uid''::text,
                                                        ''%I''::text)',tbl_name,tbl_name,'_seq');
                            END IF;

                        END IF;

                    END IF;

                END;
                $BODY$
                    LANGUAGE plpgsql;

                DROP EVENT TRIGGER if exists missing_primary_key_trigger;
                CREATE EVENT TRIGGER missing_primary_key_trigger
                ON ddl_command_end
                WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
                EXECUTE PROCEDURE z_auto_add_primary_key();
                                                """
            self.T.to_sql(                      c)
            print 'Added: f(x) z_auto_add_primary_key'
        def z_auto_add_last_updated_field(self):
            c                           =   """
                DROP FUNCTION if exists z_auto_add_last_updated_field() cascade;

                CREATE OR REPLACE FUNCTION z_auto_add_last_updated_field()
                    RETURNS event_trigger AS
                $BODY$
                DECLARE
                    last_table TEXT;
                    has_last_updated BOOLEAN;
                BEGIN
                    last_table := ( SELECT relname FROM pg_class
                                    WHERE relnamespace=2200
                                    AND relkind='r'
                                    ORDER BY oid DESC LIMIT 1);

                    EXECUTE 'SELECT EXISTS ('
                        || ' SELECT 1'
                        || ' FROM information_schema.columns'
                        || ' WHERE table_name='''
                        || quote_ident(last_table)
                        || ''' AND column_name=''last_updated'''
                        || ' )'
                        INTO has_last_updated;

                    -- RAISE EXCEPTION 'has_last_updated is %', has_last_updated;


                    IF (
                        pg_trigger_depth()=0
                        AND has_last_updated=False
                        AND position('tmp_' in last_table)=0  --exclude public.tmp_*
                        )
                    THEN
                        EXECUTE FORMAT('ALTER TABLE %I DROP COLUMN IF EXISTS last_updated',last_table);
                        EXECUTE FORMAT('ALTER TABLE %I ADD COLUMN last_updated timestamp WITH TIME ZONE',last_table);
                        EXECUTE FORMAT('DROP FUNCTION IF EXISTS z_auto_update_timestamp_on_%s_in_last_updated() CASCADE',last_table);
                        EXECUTE FORMAT('DROP TRIGGER IF EXISTS update_timestamp_on_%s_in_last_updated ON %s',last_table,last_table);

                        EXECUTE FORMAT('CREATE OR REPLACE FUNCTION z_auto_update_timestamp_on_%s_in_last_updated()'
                                        || ' RETURNS TRIGGER AS $$'
                                        || ' BEGIN'
                                        || '     NEW.last_updated := now();'
                                        || '     RETURN NEW;'
                                        || ' END;'
                                        || ' $$ language ''plpgsql'';'
                                        || '',last_table);

                        EXECUTE FORMAT('CREATE TRIGGER update_timestamp_on_%s_in_last_updated'
                                        || ' BEFORE UPDATE OR INSERT ON %I'
                                        || ' FOR EACH ROW'
                                        || ' EXECUTE PROCEDURE z_auto_update_timestamp_on_%s_in_last_updated();'
                                        || '',last_table,last_table,last_table);

                    END IF;

                END;
                $BODY$
                    LANGUAGE plpgsql;

                DROP EVENT TRIGGER if exists missing_last_updated_field;
                CREATE EVENT TRIGGER missing_last_updated_field
                ON ddl_command_end
                WHEN TAG IN ('CREATE TABLE','CREATE TABLE AS')
                EXECUTE PROCEDURE z_auto_add_last_updated_field();
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)
        def z_auto_update_timestamp(self,tbl,col):
            a="""
                DROP FUNCTION if exists z_auto_update_timestamp_on_%(tbl)s_in_%(col)s() cascade;
                DROP TRIGGER if exists update_timestamp_on_%(tbl)s_in_%(col)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_auto_update_timestamp_on_%(tbl)s_in_%(col)s()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.last_updated := now();
                    RETURN NEW;
                END;
                $$ language 'plpgsql';

                CREATE TRIGGER update_timestamp_on_%(tbl)s_in_%(col)s
                BEFORE UPDATE OR INSERT ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_auto_update_timestamp_on_%(tbl)s_in_%(col)s();

            """ % {'tbl':tbl,'col':col}

            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    a)
            return
        def NOT_USING_z_update_with_geom_from_coords(self,tbl,uid_col):
            a="""
                DROP FUNCTION if exists z_update_with_geom_from_coords_on_%(tbl)s() cascade;
                DROP TRIGGER if exists update_with_geom_from_coords_on_%(tbl)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_update_with_geom_from_coords_on_%(tbl)s()
                RETURNS TRIGGER AS $funct$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                try:
                    if (TD["new"]["trigger_step"] != 'get_geom_from_coords'):
                        return

                    T = TD['new']

                    p = \"\"\"

                        SELECT z_update_with_geom_from_coords(   t.%(uid_col)s,
                                                                '%(tbl)s'::text,
                                                                '%(uid_col)s'::text,
                                                                'latitude'::text,
                                                                'longitude'::text)
                        FROM %(tbl)s t
                        WHERE %(uid_col)s = ##(uid)s

                        \"\"\" ## T

                    # plpy.log(p)

                    TD["new"]["trigger_step"] = 'geom_added_via_trigger_get_geom_from_coords'

                    plpy.execute(p)

                except plpy.SPIError:
                    plpy.log('z_update_with_geom_from_coords FAILED')
                    plpy.log("table: " + TD["table_name"] + '; %(uid_col)s:' + str(T["%(uid_col)s"]))
                    plpy.log(tb_format_exc())
                    plpy.log(sys_exc_info()[0])
                    return
                return

                $funct$ language "plpythonu";

                CREATE TRIGGER update_with_geom_from_coords_on_%(tbl)s
                AFTER UPDATE OR INSERT ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_update_with_geom_from_coords_on_%(tbl)s();

            """ % {"tbl"                        :   tbl,
                   "uid_col"                    :   uid_col}

            cmd                                 =   a.replace("##","%")
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return

        def z_trigger_after_update_and_new_addr_trigger(self,tbl,uid_col,addr_col,zip_col):
            cmd="""
                DROP FUNCTION if exists z_trigger_after_update_and_new_addr_trigger_on_%(tbl)s_in_%(addr_col)s() cascade;
                DROP TRIGGER if exists trigger_after_update_and_new_addr_trigger_on_%(tbl)s_in_%(addr_col)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_trigger_after_update_and_new_addr_trigger_on_%(tbl)s_in_%(addr_col)s()
                RETURNS TRIGGER AS $funct$

                from os                             import system           as os_cmd
                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                try:

                    T                           =   TD["new"]
                    trig_term                   =   'new_address'

                    if (T["%(addr_col)s"] == 'not_provided' or
                        T["%(addr_col)s"] == '' or
                        T["trigger_step"].rfind(trig_term) + len(trig_term) != len(T["trigger_step"]) ):
                        return
                    else:

                        p = \"\"\"

                                UPDATE %(tbl)s set
                                    trigger_step = 'new_address.ngx'
                                WHERE %(uid_col)s = ##s

                            \"\"\" ## T['%(uid_col)s']

                        plpy.execute(p)

                        cmd = ''.join([ "curl -X POST ",
                                        "'",
                                        '&'.join([  "http://0.0.0.0:14401?",
                                                    "table=%(tbl)s",
                                                    "trigger=" + T["trigger_step"],
                                                    "uid_col=%(uid_col)s",
                                                    "addr_col=%(addr_col)s",
                                                    "zip_col=%(zip_col)s",
                                                    "idx=##s" ## T['%(uid_col)s'] ]),
                                        "'",
                                        #" > /dev/null 2>&1",
                                        " &",
                                         ])
                        plpy.log(cmd)
                        os_cmd(cmd)

                        return

                except plpy.SPIError:
                    plpy.log('set_trigger_when_new_addr_on_%(tbl)s_in_%(addr_col)s FAILED')
                    plpy.log(tb_format_exc())
                    plpy.log(sys_exc_info()[0])
                    return


                $funct$ language "plpythonu";

                CREATE TRIGGER trigger_after_update_and_new_addr_trigger_on_%(tbl)s_in_%(addr_col)s
                AFTER UPDATE ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_trigger_after_update_and_new_addr_trigger_on_%(tbl)s_in_%(addr_col)s();
            """ % { "tbl"                       :   tbl,
                    "uid_col"                   :   uid_col,
                    "addr_col"                  :   addr_col,
                    "zip_col"                   :   zip_col}
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd.replace('##','%'))
            return
        def z_trigger_after_inserting_new_addr(self,tbl,uid_col,addr_col,zip_col):
            cmd="""
                DROP FUNCTION if exists z_trigger_after_inserting_new_addr_on_%(tbl)s_in_%(addr_col)s() cascade;
                DROP TRIGGER if exists trigger_after_inserting_new_addr_on_%(tbl)s_in_%(addr_col)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_trigger_after_inserting_new_addr_on_%(tbl)s_in_%(addr_col)s()
                RETURNS TRIGGER AS $funct$

                from os                             import system           as os_cmd
                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                try:

                    T                           =   TD["new"]
                    trig_term                   =   'new_address'

                    if (T["%(addr_col)s"] == 'not_provided' or
                        T["%(addr_col)s"] == '' or
                        T["trigger_step"].rfind(trig_term) + len(trig_term) != len(T["trigger_step"]) ):
                        return
                    else:

                        p = \"\"\"

                                UPDATE %(tbl)s set
                                    trigger_step = 'new_address.ngx',
                                    orig_addr = %(addr_col)s
                                WHERE %(uid_col)s = ##s

                            \"\"\" ## T['%(uid_col)s']

                        plpy.execute(p)

                        cmd = ''.join([ "curl -X POST ",
                                        "'",
                                        '&'.join([  "http://0.0.0.0:14401?",
                                                    "table=%(tbl)s",
                                                    "trigger=new_address",
                                                    "uid_col=%(uid_col)s",
                                                    "addr_col=%(addr_col)s",
                                                    "zip_col=%(zip_col)s",
                                                    "idx=##s" ## T['%(uid_col)s'] ]),
                                        "'",
                                        #" > /dev/null 2>&1",
                                        " &",
                                         ])
                        plpy.log(cmd)
                        os_cmd(cmd)

                        return

                except plpy.SPIError:
                    plpy.log('set_trigger_when_new_addr_on_%(tbl)s_in_%(addr_col)s FAILED')
                    plpy.log(tb_format_exc())
                    plpy.log(sys_exc_info()[0])
                    return


                $funct$ language "plpythonu";

                CREATE TRIGGER trigger_after_inserting_new_addr_on_%(tbl)s_in_%(addr_col)s
                AFTER INSERT ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_trigger_after_inserting_new_addr_on_%(tbl)s_in_%(addr_col)s();
            """ % { "tbl"                       :   tbl,
                    "uid_col"                   :   uid_col,
                    "addr_col"                  :   addr_col,
                    "zip_col"                   :   zip_col}
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd.replace('##','%'))
            return

        def z_yelp_set_trigger_on_addr_not_provided(self):
            cmd="""
                DROP FUNCTION if exists z_yelp_set_trigger_on_addr_not_provided() cascade;
                DROP TRIGGER if exists yelp_set_trigger_on_addr_not_provided ON yelp;

                CREATE OR REPLACE FUNCTION z_yelp_set_trigger_on_addr_not_provided()
                RETURNS TRIGGER AS $funct$

                from os                             import system           as os_cmd
                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                try:
                    if (TD["new"]["address"] == TD["old"]["address"] or
                        TD["new"]["address"] != 'not_provided'):
                        return
                    else:
                        cmd = ''.join([ "curl 'http://0.0.0.0:14401?table=yelp&trigger=get_geom_from_coords&",
                                        "idx_col=uid&idx=%s'" % TD['new']['uid'],
                                        ])
                        plpy.log(cmd)
                        os_cmd(cmd)
                        return

                except plpy.SPIError:
                    plpy.log('z_yelp_set_trigger_on_addr_not_provided FAILED')
                    plpy.log(tb_format_exc())
                    plpy.log(sys_exc_info()[0])
                    return


                $funct$ language "plpythonu";

                CREATE TRIGGER yelp_set_trigger_on_addr_not_provided
                AFTER UPDATE OR INSERT ON yelp
                FOR EACH ROW
                EXECUTE PROCEDURE z_yelp_set_trigger_on_addr_not_provided();
            """
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            return
        def z_parse_address_on_gid_addr_zip(self,tbl,
                                                gid_col='gid',
                                                addr_col='address',
                                                zip_col='zipcode',
                                                verbose=False):
            """
            alter table tmp_5e244d5
                add column bldg text,
                add column box text,
                add column unit text,
                add column num text,
                add column predir text,
                add column street_name text,
                add column suftype text,
                add column sufdir text,
                add column bldg_street_idx text,
                add column sm integer DEFAULT 0,
                add column ls integer DEFAULT 0,
                add column gc_lat double precision,
                add column gc_lon double precision,
                add column gc_addr text,
                add column ai integer DEFAULT 0,
                add column pa text;
            """
            a="""
                DROP FUNCTION if exists z_parse_address_on_gid_addr_zip_on_%(tbl)s_in_%(addr_col)s() cascade;
                DROP TRIGGER if exists parse_address_on_gid_addr_zip_on_%(tbl)s_in_%(addr_col)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_parse_address_on_gid_addr_zip_on_%(tbl)s_in_%(addr_col)s()
                RETURNS TRIGGER AS $funct$

                #from traceback                      import format_exc       as tb_format_exc
                #from sys                            import exc_info         as sys_exc_info
                #import                                  inspect             as I

                #try:

                def adjust_single_quote(text_var,repl_var):
                    if text_var.count("'")>0:
                        text_var = ''.join(["concat('",
                                                     "',$single_quote$'$single_quote$,'".join(text_var.split("'")),
                                                     "')"])
                        text_var = text_var.replace("'","''")

                    return text_var


                p = \"\"\"  SELECT  z_update_with_parsed_info(  array_agg(t.%(gid_col)s),
                                                                '%(tbl)s',
                                                                '%(gid_col)s','%(addr_col)s',
                                                                '%(zip_col)s',
                                                                array['num','predir','street_name','suftype','sufdir'])
                            FROM    %(tbl)s t
                            WHERE   t.%(addr_col)s is not null
                            AND     t.%(zip_col)s is not null
                            AND     t.bbl is null
                            AND     t.geom is null
                    \"\"\"

                plpy.execute(p)

                return 'ok'

                $funct$ language "plpythonu";

                CREATE TRIGGER parse_address_on_gid_addr_zip_on_%(tbl)s_in_%(addr_col)s
                AFTER UPDATE OR INSERT ON %(tbl)s
                EXECUTE PROCEDURE z_parse_address_on_gid_addr_zip_on_%(tbl)s_in_%(addr_col)s();

            """ % {"tbl"                        :   tbl,
                   "addr_col"                   :   addr_col,
                   "gid_col"                    :   gid_col,
                   "zip_col"                    :   zip_col}

            cmd                                 =   a.replace("##","%")
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            if verbose:                             print cmd
            return
        def z_match_simple(self,tbl,uid_col):
            a="""
                DROP FUNCTION if exists z_match_simple_on_%(tbl)s() cascade;
                DROP TRIGGER if exists match_simple_on_%(tbl)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_match_simple_on_%(tbl)s()
                RETURNS TRIGGER AS $funct$

                #from traceback                      import format_exc       as tb_format_exc
                #from sys                            import exc_info         as sys_exc_info

                try:
                    if (TD["new"]["street_name"] == TD["old"]["street_name"] or
                        TD["new"]["sm"] == 1):
                        return

                    T = TD['new']

                    if not T["street_name"]:
                        T['sm'] = 99
                        return "MODIFY"

                    if not T["predir"]:
                        T["predir"] = ""

                    if not T["suftype"]:
                        T["suftype"] = ""

                    p = \"\"\"
                        SELECT t.%(uid_col)s,a.bldg_street_idx
                        FROM
                            %(tbl)s t,
                            (SELECT regexp_replace(concat(  predir,
                                                            street_name,
                                                            suftype,
                                                            sufdir),'\\s','','g') addr,
                                    bldg_street_idx
                                FROM addr_idx WHERE street_name IS NOT NULL) a
                        WHERE a.bldg_street_idx IS NOT NULL
                        AND a.addr ilike regexp_replace(concat( '##(predir)s',
                                                                '##(street_name)s',
                                                                '##(suftype)s',
                                                                '##(sufdir)s'),'\\s','','g')
                        AND t.%(uid_col)s = ##(%(uid_col)s)s
                        \"\"\" ## T

                    res = plpy.execute(p)
                    if not res:
                        TD['new']['sm'] = 99
                    else:
                        TD['new']['sm'] = 1
                        TD['new']['bldg_street_idx'] = res[0]['bldg_street_idx']
                    return "MODIFY"

                except plpy.SPIError:
                    plpy.log('SIMPLE MATCH TRIGGER FAILED')
                    plpy.log("table: " + TD["table_name"] + '; %(uid_col)s:' + str(T["%(uid_col)s"]))
                    #plpy.log(tb_format_exc())
                    #plpy.log(sys_exc_info()[0])
                    return
                return

                $funct$ language "plpythonu";

                CREATE TRIGGER match_simple_on_%(tbl)s
                BEFORE UPDATE OR INSERT ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_match_simple_on_%(tbl)s();

            """ % {"tbl"                        :   tbl,
                   "uid_col"                    :   uid_col}

            cmd                                 =   a.replace("##","%")
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            print cmd
            return
        def z_add_geom_through_addr_idx(self,tbl,uid_col):
            a="""
                DROP FUNCTION if exists z_add_geom_through_addr_idx_on_%(tbl)s() cascade;
                DROP TRIGGER if exists add_geom_through_addr_idx_on_%(tbl)s ON %(tbl)s;

                CREATE OR REPLACE FUNCTION z_add_geom_through_addr_idx_on_%(tbl)s()
                RETURNS TRIGGER AS $funct$

                from traceback                      import format_exc       as tb_format_exc
                from sys                            import exc_info         as sys_exc_info

                try:
                    if (TD["new"]["bldg_street_idx"] == TD["old"]["bldg_street_idx"]):
                        return

                    p = "select z_add_geom_through_addr_idx('%(tbl)s','%(uid_col)s');"

                    plpy.execute(p)

                except plpy.SPIError:
                    plpy.log('add_geom_through_addr_idx_on_%(tbl)s TRIGGER FAILED')
                    plpy.log(tb_format_exc())
                    plpy.log(sys_exc_info()[0])
                    return
                return

                $funct$ language "plpythonu";

                CREATE TRIGGER add_geom_through_addr_idx_on_%(tbl)s
                AFTER UPDATE OR INSERT ON %(tbl)s
                FOR EACH ROW
                EXECUTE PROCEDURE z_add_geom_through_addr_idx_on_%(tbl)s();

            """ % {"tbl"                        :   tbl,
                   'uid_col'                    :   uid_col}

            cmd                                 =   a.replace("##","%")
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
            print cmd
            return

    class Destroy:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def z_auto_add_primary_key(self):
            c                           =   """
            DROP FUNCTION if exists
                z_auto_add_primary_key() cascade;

            DROP EVENT TRIGGER if exists missing_primary_key_trigger cascade;
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)
        def z_auto_add_last_updated_field(self):
            c                               =   """
            DROP FUNCTION if exists
                z_auto_add_last_updated_field() cascade;

            DROP EVENT TRIGGER if exists missing_last_updated_field;
                                            """
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute(c)

    class Operate:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def disable_tbl_trigger(self,tbl,trigger_name):
            cmd = "ALTER TABLE %(tbl)s DISABLE TRIGGER %(trig)s;" % {'tbl':tbl,'trig':trigger_name}
            self.T.conn.set_isolation_level(        0)
            self.T.cur.execute(                     cmd)
        def enable_tbl_trigger(self,tbl,trigger_name):
            if trigger_name=='z_auto_add_primary_key':
                trigger_name                =   'missing_primary_key_trigger'
            cmd = "ALTER TABLE %(tbl)s ENABLE TRIGGER %(trig)s;" % {'tbl':tbl,'trig':trigger_name}
            self.T.to_sql(                      cmd)
        def disable_event_trigger(self,trigger_name):
            if trigger_name=='z_auto_add_primary_key':
                trigger_name                =   'missing_primary_key_trigger'
            cmd                             =   'ALTER EVENT TRIGGER %s DISABLE' % trigger_name
            self.T.to_sql(                      cmd)
        def enable_event_trigger(self,trigger_name):
            cmd                             =   'ALTER EVENT TRIGGER %s ENABLE' % trigger_name
            self.T.to_sql(                      cmd)

class pgSQL_Tables:
    """

    Pluto:

        update pluto set address = regexp_replace(address,'F\sD\sR','FDR','g') where address ilike '%f d r%';


    """

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def exists(self,table_name):
        qry                                 =   """
                                                SELECT EXISTS (SELECT 1
                                                    FROM information_schema.tables
                                                    WHERE table_schema='public'
                                                    AND table_name='%s');
                                                """ % table_name
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Update:
        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def prep_vendor_data_for_adding_geom(self,data_type,data_set,purpose,args=None):
            """
            This is a custom function for preparing data in a particular way.

            Uses:

                df = prep_data(data_type='db',data_set='seamless',purpose='get_bldg_street_idx')

                df = prep_data(data_type='db',data_set='mnv',purpose='get_bldg_street_idx')

                df = prep_data(data_type='db',data_set='yelp',purpose='google_geocode')


            """
            self.T.py_path.append(              self.T.os_environ['BD'] + '/geolocation')
            from f_geolocation                  import Addr_Parsing
            self.Parsing                    =   Addr_Parsing()

            # from pygeocoder                   import Geocoder
            # from time                         import sleep            as delay

            def format_db(db,purpose,args):

                # ------------------
                # ------------------
                if   (db=='seamless' or db=='seamless_geom_error'):
                    T                       =   {'db_tbl'           :   db,
                                                 'id_col'           :   'vend_id',
                                                 'vend_name'        :   'vend_name',
                                                 'address_col'      :   'address',
                                                 'zip_col'          :   'zipcode',
                                                }
                elif (db=='yelp' or db=='yelp_geom_error'):
                    T                       =   {'db_tbl'           :   db,
                                                 'id_col'           :   'gid',
                                                 'vend_name'        :   'vend_name',
                                                 'address_col'      :   'address',
                                                 'zip_col'          :   'postal_code',
                                                    }
                elif db=='mnv':
                    T                       =   {'db_tbl'           :   db,
                                                 'id_col'           :   'id',
                                                 'vend_name'        :   'vend_name',
                                                 'address_col'      :   'address',
                                                 'zip_col'          :   'zipcode',
                                                    }
                # ------------------
                # ------------------

                if purpose=='google_geocode':
                    cmd                     =   """
                                                    select %(id_col)s id,%(vend_name)s vend_name,
                                                        %(address_col)s address,%(zip_col)s zipcode
                                                    from %(db_tbl)s
                                                    where address is not null and geom is null
                                                    and (char_length(bbl::text)!=10 or bbl is null)
                                                """%T
                    df                      =   self.T.pd.read_sql(cmd,self.T.eng)
                    return df,T

                if purpose=='get_bldg_street_idx':
                    cmd                     =   """
                                                    select %(id_col)s id,%(address_col)s address,%(zip_col)s zipcode
                                                    from %(db_tbl)s
                                                    where address is not null and geom is null
                                                    and (bbl::text='NaN' or bbl is null or char_length(bbl::text)<10)
                                                """%T
                    df                      =   self.T.pd.read_sql(cmd,self.T.eng)
                    df                      =   self.Parsing.clean_street_names(df,'address','address')
                    df['address']           =   df.address.map(lambda s: s.decode('ascii','ignore').encode('utf-8','ignore'))
                    df['bldg_num']          =   df.address.map(lambda s: s.split(' ')[0])
                    df['clean_bldg_num']    =   df.bldg_num.map(lambda s: None if ''==''.join([it for it in str(s)
                                                                                               if str(s).isdigit()])
                                                          else int(''.join([it for it in str(s) if str(s).isdigit()])))
                    df                      =   df[df.clean_bldg_num.map(lambda s: True if str(s)[0].isdigit()
                                                                                    else False)].reset_index(drop=True)
                    df['bldg_street']       =   df.address.map(lambda s: ' '.join(s.split(' ')[1:]))
                    df['clean_bldg_num']    =   df.clean_bldg_num.map(int)
                    df['zipcode']           =   df.zipcode.map(int)
                    df['addr_set']          =   map(lambda s: ' '.join(map(str,s.tolist())),
                                                            df.ix[:,['clean_bldg_num','bldg_street']].as_matrix())
                    df.rename(                  columns={'clean_bldg_num':'addr_num','bldg_street':'addr_street'},
                                                inplace=True)
                    return df,T

            if data_type=='db':
                return format_db(               data_set,purpose,args)
        def add_geom_using_address(self,working_table):
            """

            Usages:

                working_table = 'seamless' | 'yelp' | 'seamless_geom_error' | 'mnv'

            """
            # 1. load NYC TABLE -- Libraries & F(x)s
            # 2. format data for and run 'get_bldg_street_idx' function
            df,T = self.prep_vendor_data_for_adding_geom(data_type      =   'db',
                                                         data_set       =   working_table,
                                                         purpose        =   'get_bldg_street_idx')
            addr_tot                        =   len(df)
            addr_uniq                       =   len(df.addr_set.unique().tolist())
            self.T.__init__(                    T)
            recognized,not_recog,TCL        =   self.Parsing.get_bldg_street_idx(   df,
                                                         addr_set_col   =   'addr_set',
                                                         addr_num_col   =   'addr_num',
                                                         addr_street_col=   'addr_street',
                                                         zipcode_col    =   'zipcode',
                                                         show_info      =   False  )

            to_check_later                  =   TCL
            addr_recog                      =   len(recognized)
            addr_unrecog                    =   len(not_recog)
            addr_TCL                        =   len(to_check_later)

            # from ipdb import set_trace as i_trace; i_trace()

            if not len(recognized):
                self.T.conn.set_isolation_level(0)
                self.T.cur.execute(             "DROP TABLE IF EXISTS %(tmp_tbl)s;" % self.T)
                print '\tTABLE:',working_table
                print addr_tot,'\t\ttotal addresses without geom [%s]'%working_table
                print addr_uniq,'\t\t# of unique addresses [%s]'%working_table
                print addr_recog,'\t\t\trecognized'
                print addr_unrecog,'\t\t\tnot_recog'
                print addr_TCL,'\t\t\tto_check_later'
                # print tmp_rows,'\t\trows in %(tmp_tbl)s' % self.T
                return


            # push data to table 'tmp'
            z                               =   self.T.pd.merge(df,recognized.ix[:, ['addr_set','bldg_street_idx']], on='addr_set',how='outer')
            z                               =   z.drop(['addr_set'],axis=1)
            self.T.conn.set_isolation_level(    0)
            self.T.cur.execute(                 'drop table if exists %(tmp_tbl)s' % self.T)
            z.to_sql(                           self.T.tmp_tbl,self.T.eng,index=False)

            # update table $working_table and delete table 'tmp'
            self.T.conn.set_isolation_level(0)
            self.T.cur.execute("""

                alter table %(tmp_tbl)s
                    add column camis integer,
                    add column bbl integer,
                    add column lot_cnt integer DEFAULT 1,
                    add column geom geometry(Point,4326);


                select z_add_geom_through_addr_idx('%(tmp_tbl)s','%(id_col)s');


                -- COPY ALL NEW DATA BACK TO WORKING TABLE (i.e., seamless, yelp)
                update %(db_tbl)s l set geom = t.geom
                    from %(tmp_tbl)s t
                    where t.geom is not null and t.id = l.%(id_col)s;


                -- DROP TMP TABLE
                DROP TABLE IF EXISTS %(tmp_tbl)s;

            """ % self.T)

            # no_geoms = self.T.pd.read_sql('select count(*) c from %(tmp_tbl)s where geom is null'%self.T,self.T.eng).c[0]

            # 6. provide result info
            print '\tTABLE:',working_table
            print addr_tot,'\t\ttotal addresses without geom [%s]'%working_table
            print addr_uniq,'\t\t# of unique addresses [%s]'%working_table
            print addr_recog,'\t\t\trecognized'
            print addr_unrecog,'\t\t\tnot_recog'
            print addr_TCL,'\t\t\tto_check_later'
            # print tmp_rows,'\t\trows in %(tmp_tbl)s' % self.T
            # print no_geoms,'\t\trows without geoms'

        def add_geom_using_external(self,working_tbl,print_gps=True):
            """

            Usages:

                 add_geom_using_geocoding(self,working_tbl=('seamless' | 'yelp' | 'mnv'))

            """
            from time                           import sleep            as delay
            from pygeocoder                     import Geocoder



            df,T                            =   self.prep_vendor_data_for_adding_geom(
                                                    data_type   =   'db',
                                                    data_set    =   working_tbl,
                                                    purpose     =   'google_geocode')
            self.T.__init__(                    T)
            addr_start_cnt                  =   len(df)

            from ipdb import set_trace as i_trace; i_trace()

            df['zipcode']                   =   df.zipcode.map(lambda s: '' if str(s).lower()=='nan' else str(int(s)))
            df['chk_addr']                  =   df.ix[:,['address','zipcode']].apply(lambda s:
                                                        unicode(s[0]+', New York, NY, '+str(s[1])).strip(),axis=1)
            uniq_addr_start_cnt             =   len(df.chk_addr.unique().tolist())

            # get google geocode results
            all_chk_addr                    =   df.chk_addr.tolist()
            uniq_addr                       =   self.T.pd.DataFrame({'addr':all_chk_addr}).addr.unique().tolist()
            uniq_addr_dict                  =   dict(zip(uniq_addr,range(len(uniq_addr))))
            _iter                           =   self.T.pd.Series(uniq_addr).iterkv()

            # if two vendors have same address: only one id will be associated with address

            y,z                             =   [],[]
            pt,s                            =   0,'Address\tZip\tLat.\tLong.\r'
            #    print '\n"--" means only one result found.\nOtherwise, numbered results will be shown.'
            if print_gps==True: print s
            for k,it in _iter:
                try:
                    results                 =   Geocoder.geocode(it)

                    if results.count > 1:
                        for i in range(0,results.count):

                            res             =   results[i]
                            r_data          =   res.data[0]
                            t               =   {'res_i'                :   i,
                                                 'orig_addr'            :   it.rstrip(),
                                                 'addr_valid'           :   res.valid_address,
                                                 'partial_match'        :   r_data['partial_match']
                                                                                if res.valid_address != True else False,
                                                 'form_addr'            :   res.formatted_address,
                                                 'geometry'             :   r_data['geometry'],
                                                 'res_data'             :   str(r_data),
                                                 }

                            y.append(           t)
                            z.append(           k)
                            a               =   '\t'.join([str(i),str(it.rstrip()),str(res.postal_code),
                                                           str(res.coordinates[0]),str(res.coordinates[1])])
                            s              +=   a+'\r'
                            if print_gps==True: print a

                    else:

                        res                 =   results
                        r_data              =   res.data[0]
                        partial_option      =   True if r_data.keys().count('partial_match') != 0 else False
                        t                   =   {'res_i'                :   -1,
                                                 'orig_addr'            :   it.rstrip(),
                                                 'addr_valid'           :   res.valid_address,
                                                 'partial_match'        :   r_data['partial_match'] if partial_option else False,
                                                 'form_addr'            :   res.formatted_address,
                                                 'geometry'             :   r_data['geometry'],
                                                 'res_data'             :   str(r_data),
                                                 }

                        y.append(               t)
                        z.append(               k)
                        a                   =   '--'+'\t'.join([str(it.rstrip()),str(results.postal_code),
                                                                str(results.coordinates[0]),
                                                                str(results.coordinates[1])])
                        s                  +=   a+'\r'
                        if print_gps==True: print a

                except:
                    pass

                pt+=1
                if pt==5:
                    delay(                      2.6)
                    pt                      =   0

            d                               =   self.T.pd.DataFrame(y)
            d['iter_keys']                  =   z
            d['lat'],d['lon']               =   zip(*d.geometry.map(lambda s: (s['location']['lat'],s['location']['lng'])))
            tbl_dict                        =   dict(zip(df.chk_addr.tolist(),df.id.tolist()))
            d['%(db_tbl)s_id' % self.T]     =   d.orig_addr.map(tbl_dict)

            # push orig_df to pgSQL
            d['geometry']                   =   d.geometry.map(str)
            d['res_data']                   =   d.res_data.map(str)
            self.T.conn.set_isolation_level(    0)
            self.T.cur.execute(                 "drop table if exists %(tmp_tbl)s;" % self.T)
            d.to_sql(                           self.T.tmp_tbl,self.T.eng,index=False)
            self.T.conn.set_isolation_level(    0)

            from ipdb import set_trace as i_trace; i_trace()

            # update 'geocoded' and $tbl
            cmd                             =   """
                                                    alter table %(tmp_tbl)s
                                                        add column to_parse_addr text,
                                                        add column zipcode bigint;

                                                    -- PULL OUT ONLY VALID, NEW YORK ADDRESSES
                                                    update %(tmp_tbl)s t set
                                                        to_parse_addr = regexp_replace( n.address,
                                                                                        '(.*,\\s)([a-zA-Z0-9\\s]*)$',
                                                                                        '\\2'),
                                                        zipcode=n.zipcode::bigint
                                                    from
                                                        (
                                                        select
                                                            t2.uid gid,
                                                            regexp_replace(t2.form_addr,'(.*)(, New York, NY)(.*)',
                                                                            '\\1') address,
                                                            regexp_replace(t2.form_addr,'(.*)([0-9]{5})(.*)',
                                                                            '\\2') zipcode
                                                        from %(tmp_tbl)s t2
                                                        where t2.addr_valid is true
                                                        ) as n
                                                    where length(n.zipcode::text)=5
                                                    and position(n.zipcode in n.address)=0
                                                    and n.gid = t.uid;


                                                    with upd as (
                                                                update geocoded g
                                                                set
                                                                    addr_valid = t.addr_valid,
                                                                    form_addr = t.form_addr,
                                                                    geometry = t.geometry,
                                                                    orig_addr = t.orig_addr,
                                                                    partial_match = t.partial_match,
                                                                    res_data = t.res_data,
                                                                    res_i = t.res_i,
                                                                    lat = t.lat,
                                                                    lon = t.lon,
                                                                    %(db_tbl)s_id = t.%(db_tbl)s_id
                                                                from %(tmp_tbl)s t
                                                                where g.orig_addr = t.orig_addr
                                                                returning t.orig_addr orig_addr
                                                            )
                                                    insert into geocoded (
                                                                            addr_valid,
                                                                            form_addr,
                                                                            geometry,
                                                                            orig_addr,
                                                                            partial_match,
                                                                            res_data,
                                                                            res_i,
                                                                            lat,
                                                                            lon,
                                                                            %(db_tbl)s_id
                                                                        )
                                                    select
                                                            t.addr_valid,
                                                            t.form_addr,
                                                            t.geometry,
                                                            t.orig_addr,
                                                            t.partial_match,
                                                            t.res_data,
                                                            t.res_i,
                                                            t.lat,
                                                            t.lon,
                                                            t.%(db_tbl)s_id
                                                    from
                                                        %(tmp_tbl)s t,
                                                        (select array_agg(f.orig_addr) upd_addrs from upd f) as f1
                                                        where (not upd_addrs && array[t.orig_addr]
                                                                or upd_addrs is null);

                                                    UPDATE %(db_tbl)s t set
                                                            geom = st_setsrid(st_makepoint(g.lon,g.lat),4326)
                                                        FROM geocoded g
                                                        WHERE g.addr_valid is true
                                                        and g.%(db_tbl)s_id = t.%(id_col)s
                                                        and t.geom is null;

                                                """ % self.T
            self.T.conn.set_isolation_level(    0)
            self.T.cur.execute(                 cmd)

            # provide result info
            uniq_search_queries             =   len(d['%(db_tbl)s_id' % self.T].unique().tolist())
            search_query_res_cnt            =   len(d)
            single_res_cnt                  =   len(d[d.res_i==-1])
            remaining_no_addr               =   self.T.pd.read_sql("""  select count(*) c from %(db_tbl)s
                                                                        where geom is null"""%self.T,self.T.eng).c[0]

            print '\tTABLE:',self.T.db_tbl
            print addr_start_cnt,'\t total addresses in %(db_tbl)s without geom' % self.T
            print uniq_addr_start_cnt,'\t unique addresses '
            print uniq_search_queries,'\t # of unique Search Queries'
            print search_query_res_cnt,'\t # of Search Query Results'
            print single_res_cnt,'\t # of Query Results with a Single Result'
            print remaining_no_addr,'\t # of Vendors in %(db_tbl)s still without geom' % self.T
            return

        def update_lot_pt_idx(self):
            a="""
            update lot_pts set lot_idx_start =
                (to_char(regexp_replace(lot_idx_start::text,
                                        '^([0-9]{1,5})\.([0-9]{1,5})$',
                                        '\\1','g')::integer,'00000')
                    ||'.'|| trim(leading ' ' from to_char(bldg_num_start,'00000')))::numeric(10,5)
            where regexp_replace(lot_idx_start::text,'^([0-9]{1,5})\.([0-9]{1,5})$','\\2','g')::integer
                != bldg_num_start::integer
            """
            return

        class NYC:

            def __init__(self,_parent):
                self                        =   _parent.T.To_Sub_Classes(self,_parent)

            def update_mvn(self):
                pluto_file                  =   self.T.PLUTO_TBL

                url                         =   'https://data.cityofnewyork.us/api/views/xx67-kt59/rows.xlsx?accessType=DOWNLOAD'
                save_path                   =   'restaurant_data.xlxs'
                self.T.download_file(           url, save_path)

                v                           =   self.T.pd.read_excel(save_path)

                v.columns                   =   [ str(it).lower().strip().replace(' ','_') for it in v.columns.tolist() ]
                m                           =   v[v.boro.str.contains('MANHATTAN')==True].copy()
                m['inspection_date']        =   self.T.pd.to_datetime(m.inspection_date)

                #  -->>  Records from "mn_vendor" are limited to the last 18 months from now.
                back_18_months              =   self.T.dt.datetime.now() - self.T.pd.DateOffset(months=18)
                mn_a                        =   m[m.inspection_date>back_18_months].sort('inspection_date',ascending=False).reset_index(drop=True)

                z                           =   mn_a.groupby('camis')
                grps                        =   z.groups.keys()
                takeCols                    =   ['dba','cuisine_description','building','street','zipcode','phone',
                                                 'inspection_date','inspection_type','grade','grade_date','record_date',
                                                 'violation_code','violation_description'] # and 'camis' which is the grps[i]
                mv                          =   self.T.pd.DataFrame(columns=['camis']+takeCols)
                g_cnt                       =   len(grps)
                for i in range(g_cnt):
                    vend_id                 =   grps[i]
                    x                       =   z.get_group(vend_id).reset_index(drop=True).ix[0,takeCols]
                    x['camis']              =   vend_id
                    mv                      =   mv.append(x)

                self.T.Addr_Parsing(            self.T)
                mv                          =   self.T.clean_street_names(mv,'dba','vend_name')
                mv                          =   self.T.clean_street_names(mv,'street','clean_street')
                mv['dba']                   =   mv.dba.map(lambda s: s.decode('ascii','ignore').encode('utf-8','ignore'))
                mv['phone']                 =   mv.phone.map(lambda s: None if s.is_integer()==False else int(s))
                mv['zipcode']               =   mv.zipcode.map(lambda s: None if s.is_integer()==False else int(s))

                self.T.to_sql(                  'drop table if exists mnv_tmp')
                mv.to_sql(                      'mnv_tmp',self.T.eng,index=False)

                if not self.F.triggers_enabled_event_trigger('missing_primary_key_trigger'):
                    self.T.to_sql(              """
                                                alter table mnv_tmp add column uid serial primary key;
                                                update mnv_tmp set uid = nextval(pg_get_serial_sequence('mnv_tmp','uid'));
                                                """)

                if not self.F.tables_exists('mnv'):
                    self.F.tables_create_nyc_mnv()

                # upsert 'mnv'
                cmd="""
                    with upd as (
                        update mnv m
                        set
                            building        = t.building,
                            camis           = t.camis,
                            clean_street    = t.clean_street,
                            cuisine_description = t.cuisine_description,
                            grade           = t.grade,
                            grade_date      = t.grade_date,
                            inspdate        = t.inspection_date,
                            inspection_type = t.inspection_type,
                            phone           = t.phone,
                            record_date     = t.record_date,
                            street          = t.street,
                            vend_name       = t.vend_name,
                            violation_code  = t.violation_code,
                            violation_description = t.violation_description,
                            zipcode         = t.zipcode
                        from mnv_tmp t
                        where
                            m.camis         = t.camis
                            and m.inspdate  = t.inspection_date
                            and m.record_date = t.record_date
                        returning t.uid uid
                    )
                    insert into mnv (
                        building,
                        camis,
                        clean_street,
                        cuisine_description,
                        dba,
                        grade,
                        grade_date,
                        inspdate,
                        inspection_type,
                        phone,
                        record_date,
                        street,
                        vend_name,
                        violation_code,
                        violation_description,
                        zipcode
                    )
                    select
                        t.building,
                        t.camis,
                        t.clean_street,
                        t.cuisine_description,
                        t.dba,
                        t.grade,
                        t.grade_date,
                        t.inspection_date,
                        t.inspection_type,
                        t.phone,
                        t.record_date,
                        t.street,
                        t.vend_name,
                        t.violation_code,
                        t.violation_description,
                        t.zipcode
                    from
                        mnv_tmp t,
                        (select array_agg(f.uid) upd_recs from upd f) as f1
                        where (not upd_recs && array[t.uid]
                                or upd_recs is null);

                    drop table mnv_tmp;
                """
                self.T.to_sql(                  cmd)

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def scrape_lattice(self,pt_buff_in_miles,lattice_table_name):
            meters_in_one_mile              =   1609.34

            z                               =   self.T.pd.read_sql("select min(lat) a,max(lat) b,min(lon) c,max(lon) d from lws_vertices_pgr",self.eng)
            lat_min,lat_max,lon_min,lon_max =   z.a[0],z.b[0],z.c[0],z.d[0]
            lat_mid,lon_mid                 =   lat_min+((lat_max-lat_min)/float(2)),lon_min+((lon_max-lon_min)/float(2))
            lat_cmd                         =   """
                                                SELECT ST_Distance_Sphere(ptA,ptB) lat_dist
                                                from (SELECT ST_GeomFromText('POINT(%s %s)',4326) as ptA,
                                                             ST_GeomFromText('POINT(%s %s)',4326) as ptB) as foo;
                                                """%(str(lon_mid),str(lat_max),str(lon_mid),str(lat_min))
            lon_cmd                         =   """
                                                SELECT ST_Distance_Sphere(ptA,ptB) lon_dist
                                                from (SELECT ST_GeomFromText('POINT(%s %s)',4326) as ptA,
                                                             ST_GeomFromText('POINT(%s %s)',4326) as ptB) as foo;
                                                """%(str(lon_max),str(lat_mid),str(lon_min),str(lat_mid))
            lat_range                       =   self.T.pd.read_sql(lat_cmd,self.T.eng).lat_dist[0] + (pt_buff_in_miles * meters_in_one_mile)
            lon_range                       =   self.T.pd.read_sql(lon_cmd,self.T.eng).lon_dist[0] + (pt_buff_in_miles * meters_in_one_mile)
            lat_segs                        =   int(round(lat_range/float(pt_buff_in_miles * meters_in_one_mile),))
            lon_segs                        =   int(round(lon_range/float(pt_buff_in_miles * meters_in_one_mile),))

            lat_mid_distances               =   np.arange(0,lat_range,
                                                        pt_buff_in_miles * meters_in_one_mile)+((pt_buff_in_miles * meters_in_one_mile)/2)
            lon_mid_distances               =   np.arange(0,lon_range,
                                                        pt_buff_in_miles * meters_in_one_mile)+((pt_buff_in_miles * meters_in_one_mile)/2)

            # set starting point
            lat_d                           =   lat_mid_distances[0]
            lon_d                           =   lon_mid_distances[0]
            self.T.update(                      {   'latt_tbl'          :   lattice_table_name,
                                                    'X'                 :   str(lon_min),
                                                    'Y'                 :   str(lat_min),
                                                    'sw_dist'           :   str(np.sqrt(lat_d**2 + lon_d**2)),
                                                    'sw_rad'            :   str(225)   } )
            cmd                             =   """
                                                    select
                                                        st_x(sw_geom::geometry(Point,4326))  min_x,
                                                        st_y(sw_geom::geometry(Point,4326))  min_y
                                                    FROM
                                                        (select
                                                            ST_Project( st_geomfromtext('Point(%(X)s %(Y)s)',4326),
                                                                        %(sw_dist)s,
                                                                        radians(%(sw_rad)s)) sw_geom) as foo;

                                                """ % self.T
            min_x,min_y                     =   self.T.pd.read_sql(cmd,self.T.eng).ix[0,['min_x','min_y']]

            # create lattice table
            self.T.conn.set_isolation_level(           0)
            self.T.cur.execute(                        """
                                                    DROP TABLE IF EXISTS %(latt_tbl)s;

                                                    CREATE TABLE %(latt_tbl)s (
                                                        gid             serial primary key,
                                                        x               double precision,
                                                        y               double precision,
                                                        bbl             numeric,
                                                        address         text,
                                                        zipcode         integer,
                                                        yelp_cnt        integer DEFAULT 0,
                                                        yelp_updated    timestamp with time zone,
                                                        sl_open_cnt     integer DEFAULT 0,
                                                        sl_closed_cnt   integer DEFAULT 0,
                                                        sl_updated      timestamp with time zone,
                                                        geom            geometry(Point,4326));

                                                    UPDATE %(latt_tbl)s
                                                    SET gid = nextval(pg_get_serial_sequence('%(latt_tbl)s','gid'));

                                                """ % self.T )



            # create and push X/Y points to pgsql -- set geom at end
            pt                              =   0
            for i in range(0,lat_segs):
                lat_d                       =   lat_mid_distances[i]
                for j in range(0,lon_segs):
                    lon_d                   =   lon_mid_distances[j]
                    self.T.update(              {   'table_name'        :lattice_table_name,
                                                    'X'                 :str(min_x),
                                                    'Y'                 :str(min_y),
                                                    'n_dist'            :str(lat_d),
                                                    'ne_dist'           :str(np.sqrt(lat_d**2 + lon_d**2)),
                                                    'e_dist'            :str(lon_d),
                                                    'n_rad'             :str(0),
                                                    'ne_rad'            :str(45),
                                                    'e_rad'             :str(90)   } )
                    # (lat_min,lon_min) is southwest most point
                    # lattice created by moving northeast

                    ## North     azimuth 0       (0)
                    ## East      azimuth 90      (pi/2)
                    ## South     azimuth 180     (pi)
                    ## West      azimuth 270     (pi*1.5)
                    cmd                     =   """
                                                INSERT INTO %(table_name)s(x,y)
                                                    select
                                                        st_x(e_geom::geometry(Point,4326))  e_geom_x,
                                                        st_y(n_geom::geometry(Point,4326))  n_geom_y
                                                    FROM
                                                        (select
                                                            ST_Project( st_geomfromtext('Point(%(X)s %(Y)s)',4326),
                                                                        %(n_dist)s,
                                                                        radians(%(n_rad)s)) n_geom) as foo1,
                                                        (select
                                                            ST_Project( st_geomfromtext('Point(%(X)s %(Y)s)',4326),
                                                                        %(e_dist)s,
                                                                        radians(%(e_rad)s)) e_geom) as foo2;

                                                """.replace('\n','') % self.T
                    self.T.conn.set_isolation_level(   0)
                    self.T.cur.execute(                cmd)

            self.T.update(                      {  'latt_tbl'           :   lattice_table_name,
                                                   'tmp_tbl'            :   'tmp_'+INSTANCE_GUID,
                                                   'tmp_tbl_2'          :   'tmp_'+INSTANCE_GUID+'_2',
                                                   'tmp_tbl_3'          :   'tmp_'+INSTANCE_GUID+'_3',
                                                   'buf_rad'            :   str(int((pt_buff_in_miles *
                                                                              meters_in_one_mile)/2.0))} )

            self.T.conn.set_isolation_level(           0)
            self.T.cur.execute(                        """
                                                UPDATE %(latt_tbl)s SET geom = ST_SetSRID(ST_MakePoint(x,y), 4326);

                                                -- 1. Remove points outside geographic land boundary of manhattan

                                                DROP TABLE IF EXISTS %(tmp_tbl)s;

                                                CREATE TABLE %(tmp_tbl)s as
                                                select st_buffer(st_concavehull(all_pts,50)::geography,%(buf_rad)s)::geometry geom
                                                    from
                                                    (SELECT ST_Collect(f.the_geom) as all_pts
                                                        FROM (
                                                        SELECT (ST_Dump(geom)).geom as the_geom
                                                        FROM pluto_centroids)
                                                        as f)
                                                    as f1;

                                                delete from %(latt_tbl)s l
                                                using %(tmp_tbl)s t
                                                where not st_within(l.geom,t.geom);

                                                drop table %(tmp_tbl)s;


                                                -- 2. match lattice points with closest tax lots and collect addresses
                                                DROP TABLE IF EXISTS %(tmp_tbl_2)s;
                                                CREATE TABLE %(tmp_tbl_2)s as
                                                        SELECT st_collect(pc.geom) all_pts
                                                        FROM pluto_centroids pc
                                                        INNER JOIN pluto p on p.gid=pc.p_gid
                                                        WHERE NOT p.zipcode=0
                                                        AND p.real_address is true;


                                                DROP TABLE IF EXISTS %(tmp_tbl_3)s;
                                                CREATE TABLE %(tmp_tbl_3)s as
                                                WITH res AS (
                                                    SELECT
                                                        l.gid l_gid,
                                                        st_closestpoint(t.all_pts,l.geom) t_geom,
                                                        ROW_NUMBER() OVER(PARTITION BY l.geom
                                                                          ORDER BY st_closestpoint(t.all_pts,l.geom) DESC) AS rk
                                                    FROM %(latt_tbl)s l,%(tmp_tbl_2)s t
                                                    )
                                                SELECT s.l_gid l_gid,p.gid p_gid
                                                    FROM res s
                                                    INNER JOIN pluto_centroids pc on pc.geom=s.t_geom
                                                    INNER JOIN pluto p on p.gid=pc.p_gid
                                                    WHERE s.rk = 1;


                                                update %(latt_tbl)s l
                                                set
                                                    address = p.address,
                                                    bbl = p.bbl,
                                                    zipcode = p.zipcode
                                                from %(tmp_tbl_3)s t
                                                INNER JOIN pluto p on p.gid=t.p_gid
                                                WHERE t.l_gid = l.gid;

                                                drop table %(tmp_tbl_3)s;
                                                drop table %(tmp_tbl_2)s;


                                                -- 3. remove lattice points furthest from tax lot having duplicate BBL values
                                                DROP TABLE IF EXISTS %(tmp_tbl_2)s;
                                                CREATE TABLE %(tmp_tbl_2)s as
                                                    WITH res AS (
                                                        SELECT
                                                            l.gid l_gid,
                                                            ROW_NUMBER() OVER(PARTITION BY l.bbl
                                                                              ORDER BY st_distance(pc.geom,l.geom) ASC) AS rk

                                                        FROM %(latt_tbl)s l
                                                        INNER JOIN pluto p on p.bbl=l.bbl
                                                        INNER JOIN pluto_centroids pc on pc.p_gid = p.gid
                                                        )
                                                    SELECT s.l_gid l_gid
                                                        FROM res s
                                                        WHERE s.rk = 1;

                                                DROP TABLE IF EXISTS %(tmp_tbl_3)s;
                                                CREATE TABLE %(tmp_tbl_3)s as
                                                    select *
                                                    from %(latt_tbl)s l
                                                    WHERE EXISTS (select 1 from %(tmp_tbl_2)s t where t.l_gid=l.gid);
                                                drop table %(latt_tbl)s;
                                                ALTER TABLE %(tmp_tbl_3)s rename to %(latt_tbl)s;

                                                DROP TABLE IF EXISTS %(tmp_tbl_2)s;
                                                DROP TABLE IF EXISTS %(tmp_tbl_3)s;



                                                """ % self.T )


            self.T.update(                      { 'latt_tbl'            :   lattice_table_name,})

            # PROVE THAT ALL ADDRESSES ARE UNIQUE ( assuming no two addresses have the same BBL )
            assert True                    ==   self.T.pd.read_sql("""  select all_bbl=uniq_bbl _bool
                                                                        from
                                                                            (select count(distinct y1.bbl) uniq_bbl
                                                                                from %(latt_tbl)s y1) as f1,
                                                                            (select count(y2.bbl) all_bbl
                                                                                from %(latt_tbl)s y2) as f2
                                                                    """ % self.T,self.T.eng)._bool[0]

        def usps_table(self):
            self.T.py_path.append(                        self.T.os_environ['BD'] + '/geolocation/USPS')
            from USPS_syntax_pdf_scrape         import load_from_file
            self.T.py_path.append(                        self.T.os_environ['BD'] + '/html')
            from scrape_vendors                 import Scrape_Vendors
            SV = Scrape_Vendors()

            # Files
            dir_path                                =   self.T.os_environ['BD'] + '/geolocation/USPS'
            fpath_pdf                               =   dir_path + '/usps_business_abbr.pdf'
            fpath_xml                               =   dir_path + '/usps_business_abbr.xml'

            street_abbr_csv                         =   dir_path + '/usps_street_abbr.csv'
            biz_abbr_csv                            =   dir_path + '/usps_business_abbr.csv'
            regex_biz_abbr_csv                      =   dir_path + '/usps_business_abbr_regex.csv'
            regex_street_abbr_csv                   =   dir_path + '/usps_street_abbr_regex.csv'
            # t                                       =   extract_pdf_contents_from_stdout(fpath_pdf)

            A = load_from_file(street_abbr_csv).ix[:,['common_use','usps_abbr']]
            B = load_from_file(biz_abbr_csv).ix[:,['common_use','usps_abbr']]

            # Asserts Based on Previous Run
            assert len(A)==482
            assert len(B)==5969
            assert len(A)+len(B)==6451

            B.to_sql('usps',eng,index=False)
            A.to_sql('tmp_usps',eng,index=False)

            SV.SF.PGFS.Run.make_column_primary_serial_key('usps','gid',True)
            SV.SF.PGFS.Run.make_column_primary_serial_key('tmp_usps','gid',True)

            self.T.conn.set_isolation_level(0)
            self.T.cur.execute("""

            alter table usps add column abbr_type text;
            update usps set abbr_type='business';

            alter table tmp_usps add column abbr_type text;
            update tmp set abbr_type='street';

            insert into usps (common_use,usps_abbr,abbr_type)
            select t.common_use,t.usps_abbr,t.abbr_type
            from tmp_usps t;

            drop table tmp_usps;
            """)

        def mn_zipcodes(self):
            """
            incomplete and untested
            """
            src = 'http://www.unitedstateszipcodes.org/zip_code_database.csv'
            z = self.T.pd.read_csv(src)
            x = z[(z.state=='NY')&(z.county=='New York County')&(z.type!='PO BOX')].sort('zip')

            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    "DROP TABLE IF EXISTS mn_zipcodes")
            df = x.reset_index(drop=True).ix[:,['zip']]
            df.to_sql('mn_zipcodes',self.T.eng,index=False)
            # print len(x)
            # print x.type.unique().tolist()
            # print str(x.zip.tolist())
            #
            # # result
            # mn_zipcodes = [10001, 10002, 10003, 10004, 10005, 10006,
            #                10007, 10009, 10010, 10011, 10012, 10013,
            #                10014, 10015, 10016, 10017, 10018, 10019,
            #                10020, 10021, 10022, 10023, 10024, 10025,
            #                10026, 10027, 10028, 10029, 10030, 10031,
            #                10032, 10033, 10034, 10035, 10036, 10037,
            #                10038, 10039, 10040, 10041, 10043, 10044,
            #                10045, 10046, 10047, 10048, 10055, 10060,
            #                10065, 10069, 10072, 10075, 10079, 10080,
            #                10081, 10082, 10087, 10090, 10094, 10095,
            #                10096, 10098, 10099, 10102, 10103, 10104,
            #                10105, 10106, 10107, 10109, 10110, 10111,
            #                10112, 10114, 10115, 10117, 10118, 10119,
            #                10120, 10121, 10122, 10123, 10124, 10125,
            #                10126, 10128, 10130, 10131, 10132, 10133,
            #                10138, 10149, 10151, 10152, 10153, 10154,
            #                10155, 10157, 10158, 10160, 10161, 10162,
            #                10164, 10165, 10166, 10167, 10168, 10169,
            #                10170, 10171, 10172, 10173, 10174, 10175,
            #                10176, 10177, 10178, 10179, 10184, 10196,
            #                10197, 10199, 10203, 10211, 10212, 10213,
            #                10256, 10257, 10258, 10259, 10260, 10261,
            #                10265, 10269, 10270, 10271, 10273, 10275,
            #                10277, 10278, 10279, 10280, 10281, 10282,
            #                10285, 10286, 10292]

        def regex_repl(self):
            """

            NOTE:
                Because this table is used within a lua function (z_custom_addr_pre_filter),
                some syntax differences exist between regex_replace in pgSQL and the below regex expressions.

                SO, USE LUA CONSOLE TO TEST!

                    addr = "5|LITTLE WEST 12 STREET"
                    p = "([0-9]+)%|(LITTLE W[\.]?[E]?[S]?[T]?[%s]12)[T]?[H]?[%s]?(.*)$"
                    r = "%1|LITTLEQQQQWESTQQQQ12 %3"
                    print(addr:gsub(p,r))

                Some of the differences:

                    1. escape character is percentage symbol '%' instead of backslash '\'
                    2. no numerical quantifiers, i.e., {3} or {3,} or {3,7}
                    3. no alternate patterns, i.e., (one|two)

            ALSO NOTE:

                Below expressions cannot reference replacements for captures above 9.

                For example, the attempted replacement of `capture #37` would result in `capture #3` + `7`

                Lua does not yet provide mechanism to name captures. From lua-users.org:

                    Limitations of Lua patterns

                    Especially if you're used to other languages with regular expressions,
                    you might expect to be able to do stuff like this:

                        '(foo)+' -- match the string "foo" repeated one or more times
                        '(foo|bar)' -- match either the string "foo" or the string "bar"

                    Unfortunately Lua patterns do not support this, only single characters
                    can be repeated or chosen between, not sub-patterns or strings. The
                    solution is to either use multiple patterns and write some custom logic,
                    use a regular expression library like lrexlib or Lua PCRE, or use LPeg.
                    LPeg is a powerful text parsing library for Lua based on
                    Parsing Expression Grammar. It offers functions to create and combine
                    patterns in Lua code, and also a language somewhat like Lua patterns or
                    regular expressions to conveniently create small parsers.


            """
            a="""

                drop table if exists regex_repl;

                create table regex_repl (
                    tag text,
                    repl_from text,
                    repl_to text,
                    repl_flag text,
                    run_order integer,
                    comment text,
                    is_active boolean default true
                );

                insert into regex_repl (tag,
                                        repl_from,
                                        repl_to,
                                        repl_flag,
                                        run_order)
                values

                    -- AVENUE B --> B AVENUE
                    ('custom_addr_pre_filter',
                        '([^|]*)%|(AVE?N?U?E?)[%s]+([A-F])[%s]?(.*)',
                        '%1|%3 %2 %4','','0'),

                    -- AVENUE OF THE AMERICAS
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(AVE?N?U?E?[%s]+O?F?)[%s]+[THE]*[%s]*(AMERI?C?A?S?[%s]*)(.*)',
                        '%1|6 AVENUE %4','','0'),

                    -- Qx5 = repl. w/ no space, Qx4 = repl. w/ space

                    -- AVENUE OF THE FINEST
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(AVE[NUE]*[%s]+OF[%s]+)[THE]*[%s]*(FINEST?)[%s]*(.*)',
                        '%1|AVENUEQQQQOFQQQQTHEQQQQFINEST %4','','0'),


                    -- 3 AVENUE --> 0 3 AVENUE  (b/c street num required for parsing)
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(AVE?N?U?E?)[%s]?([a-zA-Z]*)$',
                        '0|%1 %2 %3','','1'),

                    -- special streets where 'EAST' and 'WEST' don't refer to an end of a street
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(WE?S?T?)%s(END)%s(AV)(.*)$',
                        '%1|%2QQQQ%3 %4%5','','1'),
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(EA?S?T?)%s(RIVER)%s(DR)(.*)$',
                        '%1|%2QQQQ%3 %4%5','','1'),

                    -- "ADAM CLAYTON POWELL JR BOULEVARD"
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(AD?A?M?[%s]*C?L?A?Y?T?O?N?[%s]+PO?W?E?L?L?[%s]*J?R?)[%s]+(BO?U?L?E?V?A?R?D?)(.*)$',
                        '%1|7 AVENUE %4','','1'),

                    -- "DR M L KING JR BOULEVARD"
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(DR[%s]+MA?R?T?I?N?[%s]+LU?T?H?E?R?[%s]+KI?N?G?[%s]*J?R?)[%s]+(BO?U?L?E?V?A?R?D?)(.*)$',
                        '%1|DRQQQQMQQQQLQQQQKQQQQJR BOULEVARD %4','','1'),


                    -- b/c PIKE in 'PIKE SLIP' does not refer to a highway
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(PI)(KE)[%s]+(SLIP)(.*)$',
                        '%1|%2QQQQQ%3 %4%5','','1'),

                    -- b/c WALL in 'WALL STREET' no longer refers to a wall
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(WALL)[%s]+(STR?E?E?T?)[%s]?(.*)$',
                        '%1|WAQQQQQLL %3 %4','','1'),


                    -- b/c WEST in 'LITTLE WEST 12 STREET' does not refer to the west end of Little West 12th Street
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(LITTLE W[.]?[E]?[S]?[T]?[%s]12)[T]?[H]?[%s]?(.*)$',
                        '%1|LITTLEQQQQWESTQQQQ12 %3','','1'),


                    -- STREETS WITH STREET NUMBERS HAVING '1/2'
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(1)%s(/2)[%s]+(.*)',
                        '%1 %2%3|%4','','1'),

                    -- PARSER HANDLED STREETS WITH TERRACE SHORTENED BETTER
                    ('custom_addr_pre_filter',
                        '([^|]*)|(.*)(%s)(TERRACE)([%s]*)([a-zA-Z]*)$',
                        '%1|%2 TERR %5','','2'),

                    -- STREETS WITH SPANISH 'LA' EXCEPT IT DOESN'T STAND FOR 'LANE'
                    ('custom_addr_pre_filter',
                        '([^|]*)|(LA)[%s]+(.*)',
                        '%1|%2QQQQ%3','','3'),

                    -- STREETS WITH MISSING/MISPELLED TAIL LETTERS, e.g., 'STREE', 'AV', 'P'
                    ('custom_addr_pre_filter',
                        '([^|]*)|(.*)[%s]+(STR?E?E?T?)$',
                        '%1|%2 STREET','','3'),
                    ('custom_addr_pre_filter',
                        '([^|]*)|(.*)[%s]+(AVE?N?U?E?)$',
                        '%1|%2 AVENUE','','3'),
                    ('custom_addr_pre_filter',
                        '([^|]*)|(.*)[%s]+(PL?A?C?E?)$',
                        '%1|%2 PLACE','','3'),


                    ('custom_addr_pre_filter',
                        '([0-9]+)[%-]([a-zA-Z0-9]+)%|(.*)',
                        '%1|%3, Bldg. %2','','4'),
                    ('custom_addr_pre_filter',
                        '([0-9]+)([a-zA-Z]+)%|(.*)',
                        '%1|%3, Bldg. %2','','5'),
                    ('custom_addr_pre_filter',
                        '([0-9]+)%|(.*)(,%s)(Bldg%.)%s([a-zA-Z0-9]+)$',
                        'Bldg. %5, %1|%2','g','6'),
                    ('custom_addr_pre_filter',
                        '([^%|]*)%|[%s]*(.*)[%s]*',
                        '%1 %2','g','7')

                ;
            """
            self.T.conn.set_isolation_level(               0)
            self.T.cur.execute(                            a)

        def tmp_addr_idx_pluto(self):
            cmd                         =   """
                drop table if exists tmp_addr_idx_pluto;
                create table tmp_addr_idx_pluto as
                    select * from z_parse_NY_addrs(
                        'select gid,address,zipcode from pluto
                        where address is not null order by gid');

                select z_make_column_primary_serial_key( 'tmp_addr_idx_pluto', 'gid', true);
                                            """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
            tmp_tbl_matches_pluto       =   """
                select pluto_cnt=tmp_cnt is_true from
                    (select count(distinct src_gid) tmp_cnt
                        from tmp_addr_idx_pluto) as f1,
                    (select count(distinct gid) pluto_cnt
                        from pluto where address is not null) as f2;
                                            """
            assert self.T.pd.read_sql(tmp_tbl_matches_pluto,self.T.eng).is_true[0]==True
            tmp_tbl_appears_valid       =   """
                select count(*)=0 is_true from tmp_addr_idx_pluto where
                    box is not null
                    or unit is not null
                    or pretype is not null
                    or qual is not null
                    or (sufdir is not null and sufdir != 'N'
                         and sufdir != 'E' and sufdir != 'W' and sufdir != 'S' )
                    or name is null
                    or city is null
                    or state is null
                    or zip is null or zip = '0'
                    or num is null
                                            """
            assert self.T.pd.read_sql(tmp_tbl_appears_valid,self.T.eng).is_true[0]==True

        def tmp_addr_idx_snd(self):
            # TMP_SND
            a="""
            drop table if exists tmp_snd;

            create table tmp_snd as
            select distinct on (primary_name) concat('0 ',primary_name) address,'11111'::integer zipcode
            from snd;

            select z_make_column_primary_serial_key( 'tmp_snd', 'gid', true);

            """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    a)
            # Remove from TMP_SND
            full_entries_with_words = [' APPR','APPROACH',' EXIT','ENTRANCE','PEDESTRIAN',
                                       'DRIVE NB','DRIVE SB','NORTHBOUND','SOUTHBOUND',' HOUSES',
                                       'FERRY ROUTE','TERMINAL','RAMP',' HOSPITAL',' UNDERPASS',
                                       ' PATH',' PK-NEAR',' HSPTL',' EXPWY','COMPLEX',
                                       ' PLAYGROUND',' STATION',' WALK',' FIELDS','(SITE 7 )',' GREENWAY','PS ',
                                       'CMPX',' YARD',' MEMORIAL',
                                       ' COLLEGE',' TUNNEL',' TOWERS',' NB',' SB',' ET '
                                      ' SLIP ',' INSTITUTE ',' PROMENADE',' BUILDING','CITY LIMIT',' EXPRESSWAY',
                                       ' CITY',' HOUSE',' EN','IND-A','(GROUP 5 )','METRO NORTH','MEDICAL CENTER',
                                       'TAFT REHABS','THE MALL','UNNAMED STREET','AUX PO',
                                      'GR HILL','CHELSEA PIERS','UNIVERSITY','HIGH LINE','MANHATTAN MARINA',
                                       'WASHINGTON HTS','SOUTH ST VIADUCT','SOUTH STREET SEAPORT',
                                       'RANDALLS ISLAND','MANHATTANVILLES']
            for it in full_entries_with_words:
                a = "delete from tmp_snd where address ilike '%s'" % ('%%'+it+'%%')
                self.T.conn.set_isolation_level(       0)
                self.T.cur.execute(                    a)
            # CLEAN ADDRESS IN TMP_SND
            remove_suffix = [' LOOP',' TRANSVERSE',' CROSSING', 'EXTENSION',' REHAB']
            for it in remove_suffix:
                a = "update tmp_snd set address = regexp_replace(address,'%s','')" % it
                self.T.conn.set_isolation_level(       0)
                self.T.cur.execute(                    a)
            fix_extra_end_spaces = "update tmp_snd set address = regexp_replace(address,'[\s]+$','')"
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    fix_extra_end_spaces)
            # de-dupe
            T = {'tbl':'tmp_snd',
                 'uid_col':'gid',
                 'partition_col':'address',
                 'sort_col':'gid'}
            a="""
                drop table if exists tmp1;
                create table tmp1 as
                    WITH res AS (
                        SELECT
                            t.%(uid_col)s t_%(uid_col)s,
                            ROW_NUMBER() OVER(PARTITION BY t.%(partition_col)s
                                              ORDER BY %(sort_col)s ASC) AS rk
                        FROM %(tbl)s t
                        )
                    SELECT s.t_%(uid_col)s t_%(uid_col)s
                        FROM res s
                        WHERE s.rk = 1;

                drop table if exists tmp2;

                create table tmp2 as
                    select *
                    from %(tbl)s t1
                    WHERE EXISTS (select 1 from tmp1 t2 where t2.t_%(uid_col)s = t1.%(uid_col)s);
                drop table %(tbl)s;

                create table %(tbl)s as select * from tmp2;
                alter table %(tbl)s add primary key (%(uid_col)s);

                drop table if exists tmp1;
                drop table if exists tmp2;
                """%T
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    a)
            fix_wall_streets = """
            update tmp_snd set address = regexp_replace(address,'WALL STREET','WALLQQQQSTREET')
            where address ilike '%wall street %'
            """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    fix_wall_streets)

            # Reload tmp_addr_idx_snd
            cmd                         =   """
                drop table if exists tmp_addr_idx_snd;
                create table tmp_addr_idx_snd as
                    select * from z_parse_NY_addrs(
                        'select gid,address,zipcode from tmp_snd order by gid');

                select z_make_column_primary_serial_key( 'tmp_addr_idx_snd', 'gid', true);


            delete from tmp_addr_idx_snd where bldg is not null or unit is not null or city is null;
            alter table tmp_addr_idx_snd
                drop column bldg, drop column box,drop column unit,
                drop column pretype,drop column qual,drop column predir
                drop column num,drop column qual,drop column predir,
                drop column zip,drop column city,drop column state;

            delete from tmp_addr_idx_snd s
            using (
                select array_agg(concat(t.predir,' ',t.name,' ',t.suftype,' ',t.sufdir)) all_idx
                from tmp_addr_idx_pluto t) as f1
            where concat(s.predir,' ',s.name,' ',s.suftype,' ',s.sufdir) = any (all_idx);

                                            """
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    cmd)
            return

        def addr_idx(self):
            self.T.tmp_addr_idx_pluto()
            self.T.tmp_addr_idx_snd()

            # Take all uniq from tmp_addr_idx pluto & snd
            T = {'new_tbl':'tmp_addr_idx',
                 'old_tbl':'tmp_addr_idx_pluto',
                 'uid_col':'gid',
                 'partition_col':'name',
                 'sort_col':'num',
                 'sort_method_1':'ASC',
                 'sort_method_2':'DESC',
                 'wc':'%'}
            a="""

            alter table %(old_tbl)s add column num_fl double precision,add column zip_int integer;
            update %(old_tbl)s set num = regexp_replace(num,' 1/2','.5')
            where num ilike '%(wc)s1/2%(wc)s';
            update %(old_tbl)s set num_fl = num::double precision,zip_int = zip::integer;
            alter table %(old_tbl)s drop column num,drop column zip,drop column num_int;
            alter table %(old_tbl)s rename column num_fl to num;
            alter table %(old_tbl)s rename column zip_int to zip;
            alter table %(old_tbl)s drop column if exists num_fl,drop column if exists zip_int;

            drop table if exists %(new_tbl)s;
            create table %(new_tbl)s as
                WITH res AS (
                    SELECT
                        t.%(uid_col)s t_%(uid_col)s,
                        ROW_NUMBER() OVER(PARTITION BY concat(t.predir,' ',t.name,' ',t.suftype,' ',t.sufdir)
                                          ORDER BY %(sort_col)s %(sort_method_1)s) AS rk
                    FROM %(old_tbl)s t
                    )
                SELECT t.*
                FROM
                    res s,
                    %(old_tbl)s t
                WHERE s.rk = 1
                and t.%(uid_col)s = s.t_%(uid_col)s;

            alter table %(new_tbl)s add column num_max double precision,add column num_min double precision;
            update %(new_tbl)s set num_min = num;

            WITH res AS (
                SELECT
                    t.%(uid_col)s t_%(uid_col)s,
                    ROW_NUMBER() OVER(PARTITION BY concat(t.predir,' ',t.name,' ',t.suftype,' ',t.sufdir)
                                      ORDER BY %(sort_col)s %(sort_method_2)s) AS rk
                FROM %(old_tbl)s t
                )
            UPDATE %(new_tbl)s t1
            SET num_max = t2.num
            FROM
                res s,
                %(old_tbl)s t2
            WHERE s.rk = 1
            and t2.%(uid_col)s = s.t_%(uid_col)s
            and concat(t1.predir,' ',t1.name,' ',t1.suftype,' ',t1.sufdir) =
            concat(t2.predir,' ',t2.name,' ',t2.suftype,' ',t2.sufdir);

            alter table tmp_addr_idx
                drop column orig_addr,drop column box,drop column unit,
                drop column pretype,drop column qual,drop column num;

            """ % T
            self.T.conn.set_isolation_level(       0)
            self.T.cur.execute(                    a)
            return

        def pluto_changes(self):
            """
                CHANGES MADE INITIALLY AND RECORDED HERE BUT THIS FUNCTION IS NOT YET TESTED
            """
            a                       =   """
                update pluto set gid = regexp_replace(gid,'([0-9]+)([O])(.*)','\\10\\2','g')
                    where gid = 20453
                        or gid = 15296
                        or gid = 40214
                        or gid = 31800
                        or gid = 30225
                        or gid = 26608
                        or gid = 36230;
            """

        class NYC:

            def __init__(self,_parent):
                self                        =   _parent.T.To_Sub_Classes(self,_parent)

            def snd(self,table_name='snd',drop_prev=True):
                from f_nyc_data import load_parsed_snd_datafile_into_db
                load_parsed_snd_datafile_into_db(table_name,drop_prev)
                a="""

                delete from snd where primary_name = variation;

                alter table snd
                    add column from_num integer,
                    add column from_predir text,
                    add column from_street_name text,
                    add column from_suftype text,
                    add column from_sufdir text,
                    add column to_num integer,
                    add column to_predir text,
                    add column to_street_name text,
                    add column to_suftype text,
                    add column to_sufdir text;



                delete from snd
                where variation ilike any (array[ '%%NORTHBOUND%%','%%NB%%','%%SOUTHBOUND%%','%%SB%%',
                                                '%%ENTRANCE%%','%% BRDG%%','%% EXIT%%','%% TRAIN%%','RT %%','%% RT%%',
                                                'RTE %%','%% RTE%%','%% US %%','US %%','%% RTE%%','%%INTERSTATE %%',
                                                '%%I-95%%','%%I %% 95%%','%% COMPLEX%%','%% PROJECTS%%','ROUTE %%',
                                                '%%PATH %%','%%PATH-%%','%% HOUSES%%','%% EXTENSION%%']);

                WITH upd AS (
                    SELECT  src_gid,predir,name street_name,suftype,sufdir
                    FROM    z_parse_NY_addrs('
                                            select
                                                uid::bigint gid,
                                                variation::text address,
                                                ''11111''::bigint zipcode
                                            FROM snd
                                            ')
                      )
                UPDATE snd t set
                    from_predir = u.predir,
                    from_street_name = u.street_name,
                    from_suftype = u.suftype,
                    from_sufdir = u.sufdir
                FROM  upd u
                WHERE u.src_gid = t.uid::bigint;


                delete from snd
                where primary_name ilike any (array[ '%%NORTHBOUND%%','%%NB%%','%%SOUTHBOUND%%','%%SB%%',
                                                '%%ENTRANCE%%','%% BRDG%%','%% EXIT%%','%% TRAIN%%','RT %%','%% RT%%',
                                                'RTE %%','%% RTE%%','%% US %%','US %%','%% RTE%%','%%INTERSTATE %%',
                                                '%%I-95%%','%%I %% 95%%','%% COMPLEX%%','%% PROJECTS%%','ROUTE %%',
                                                '%%PATH %%','%%PATH-%%','%% HOUSES%%','%% RECREATION CTR%%',
                                                '%% EXTENSION%%']);

                WITH upd AS (
                    SELECT  src_gid,predir,name street_name,suftype,sufdir
                    FROM    z_parse_NY_addrs('
                                            select
                                                uid::bigint gid,
                                                primary_name::text address,
                                                ''11111''::bigint zipcode
                                            FROM snd
                                            ')
                      )
                UPDATE snd t set
                    to_predir = u.predir,
                    to_street_name = u.street_name,
                    to_suftype = u.suftype,
                    to_sufdir = u.sufdir
                FROM  upd u
                WHERE u.src_gid = t.uid::bigint


                INSERT INTO snd (from_num,from_predir,from_street_name,from_suftype,from_sufdir,
                                to_num,to_predir,to_street_name,to_suftype,to_sufdir,custom)
                VALUES (0,null,'ROCKEFELLER','CTR',null,
                        45,null,'ROCKEFELLER','PLZ',null,true);

                INSERT INTO snd (from_predir,from_street_name,from_suftype,from_sufdir,
                                to_predir,to_street_name,to_suftype,to_sufdir,custom)
                VALUES (null,'ROCKEFELLER','CTR',null,
                        null,'ROCKEFELLER','PLZ',null,true);

                INSERT INTO snd (from_predir,from_street_name,from_suftype,from_sufdir,
                                to_predir,to_street_name,to_suftype,to_sufdir,custom)
                VALUES ('W','59','ST',null,
                        null,'CENTRAL','PARK','S',true);

                INSERT INTO snd (from_predir,from_street_name,from_suftype,from_sufdir,
                                to_predir,to_street_name,to_suftype,to_sufdir,custom)
                VALUES (null,'BROADWAY','AVE',null,
                        null,'BROADWAY',null,null,true);

                INSERT INTO snd (from_predir,from_street_name,from_suftype,from_sufdir,
                                to_predir,to_street_name,to_suftype,to_sufdir,custom)
                VALUES (null,'BROADWAY','ST',null,
                        null,'BROADWAY',null,null,true);

                INSERT INTO snd (variation,primary_name,custom)
                VALUES ('BROADWAY AVENUE','BROADWAY',true);

                INSERT INTO snd (variation,primary_name,custom)
                VALUES ('BROADWAY AVE','BROADWAY',true);

                INSERT INTO snd (variation,primary_name,custom)
                VALUES ('BROADWAY STREET','BROADWAY',true);

                INSERT INTO snd (variation,primary_name,custom)
                VALUES ('BROADWAY ST','BROADWAY',true);

                INSERT INTO snd (from_predir,from_street_name,from_suftype,from_sufdir,
                                to_predir,to_street_name,to_suftype,to_sufdir,custom)
                VALUES (null,'PENNSYLVANIA','PLZ',null,
                        null,'PENN','PLZ',null,true);

                """

            def pad(self):
                # CREATE/ADJUST PAD TABLES

                # CLEAN UP TABLE SPACE
                a="""
                drop table if exists pad_adr;
                drop table if exists pad_bbl;
                drop table if exists tmp_addr_idx_pad;
                drop table if exists addr_idx_wl;
                """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     a)

                # CREATE PAD_BBL
                df2 = SV.T.pd.read_csv(os_path.join(dl_dir,'bobabbl.txt'))
                t=df2.columns.tolist()
                new_cols = [it.replace('"','') for it in t]
                df2.columns = new_cols
                df2[df2.boro==1].to_sql('pad_bbl',SV.T.eng,index=False)
                a="""

                alter table pad_bbl add column lobbl numeric,add column hibbl numeric,add column billbbl numeric;

                update pad_bbl set
                billboro=trim(both ' ' from billboro),
                billblock=trim(both ' ' from billblock),
                billlot=trim(both ' ' from billlot);

                update pad_bbl set billboro=null where billboro='';
                update pad_bbl set billblock=null where billblock='';
                update pad_bbl set billlot=null where billlot='';

                update pad_bbl set
                lobbl =
                regexp_replace( to_char(loboro::integer,'0')||
                                to_char(loblock::integer,'00000')||
                                to_char(lolot::integer,'0000'),'[[:space:]]','','g')::numeric,
                hibbl =
                regexp_replace( to_char(hiboro::integer,'0')||
                                to_char(hiblock::integer,'00000')||
                                to_char(hilot::integer,'0000'),'[[:space:]]','','g')::numeric;


                update pad_bbl set
                billbbl =
                regexp_replace(to_char(billboro::integer,'0')||to_char(billblock::integer,'00000')||to_char(billlot::integer,'0000'),'[[:space:]]','','g')::numeric
                where billboro is not null and billblock is not null and billlot is not null;

                """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     a)

                # CREATE PAD_ADR AND START CLEAN (I.E., ENSURE ALL ADDRESSES HAVE ZIPCODE)
                df = SV.T.pd.read_csv(os_path.join(dl_dir,'bobaadr.txt'))
                df[df.boro==1].to_sql('pad_adr',SV.T.eng,index=False)
                a="""
                    alter table pad_adr
                        add column bbl numeric,
                        add column billbbl numeric,
                        add column stnum_w_letter boolean default false,
                        add column street_name text;

                    update pad_adr set bbl =
                    regexp_replace(to_char(boro,'0')||to_char(block,'00000')||to_char(lot,'0000'),'[[:space:]]','','g')::numeric,
                    lhnd=trim(both ' ' from lhnd),
                    hhnd=trim(both ' ' from hhnd),
                    stname=trim(both ' ' from stname);

                    delete from pad_adr where lhnd = '' and hhnd = '';
                    delete from pad_adr where lhnd ilike '% AIR%';
                    update pad_adr set stname = regexp_replace(stname,'[[:space:]]{2,}',' ','g');
                    update pad_adr set zipcode = regexp_replace(zipcode,'[[:space:]]{2,}',' ','g');
                    update pad_adr set zipcode = null where zipcode = ' ';

                    update pad_adr p1 set zipcode = f2.zipcode
                    from
                        (select distinct on (p2.bbl) p2.bbl,p2.zipcode from
                            pad_adr p2,
                            (select distinct on (bbl) bbl from pad_adr where zipcode is null) f1
                        where p2.bbl = f1.bbl
                        and p2.zipcode is not null ) f2
                    where p1.zipcode is null
                    and p1.bbl = f2.bbl;

                    update pad_adr p1 set zipcode = f2.zipcode from
                    --select * from pad_adr p1,
                        (select distinct on (p2.bbl) p2.bbl,p2.zipcode from
                            pluto p2,
                            (select distinct on (bbl) bbl from pad_adr where zipcode is null) f1
                        where p2.bbl = f1.bbl
                        and p2.zipcode is not null ) f2
                    where p1.zipcode is null
                    and p1.bbl = f2.bbl;
                    """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     a)
                assert SV.T.pd.read_sql(""" select count(*)=0 only_known_addr_remaining from pad_adr
                                            where zipcode is null
                                            and not lhnd ilike '## AIR##'
                                            and not bbl = '1002230997' -- 2400 READE STREET
                                         """.replace('##','%%'),SV.T.eng).only_known_addr_remaining[0] == True

                # SEPARATE ADDRESSES WHERE STREET NUMBER HAS LETTERS
                a="""

                    delete from pad_adr where zipcode is null
                    and ( lhnd ilike '## AIR##' );

                    update pad_adr p set stnum_w_letter = true
                    from
                        (select distinct on (lhnd) lhnd,regexp_replace(lhnd,'[0-9]*','','g') repls
                        from pad_adr) f
                    where f.repls != '' and f.lhnd = p.lhnd;

                    update pad_adr p set stnum_w_letter = true
                    from
                        (select distinct on (hhnd) hhnd,regexp_replace(hhnd,'[0-9]*','','g') repls
                        from pad_adr) f
                    where f.repls != '' and f.hhnd = p.hhnd;

                    """.replace('##','%%')
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     a)

                ## PROVE THAT ONLY ADDR REMAINING IN pad_adr ARE VALID TYPES
                assert SV.T.pd.read_sql(""" select count(*)=0 has_only_valid_addr from pad_adr
                                            where addrtype = any (array['B','G','N','X','U'])
                                            or lhnd = ''
                                            or hhnd = ''
                                            or stname is null or stname = ' ' or stname = ''
                                            """,SV.T.eng).has_only_valid_addr[0] == True

                ## PROVE THAT ALL pluto_bbl EXIST IN pad_bbl_billbbl
                assert SV.T.pd.read_sql(""" select count(*)=0 no_pluto_bbl_not_in_pad_bbl_billbbl
                                            from pluto p,
                                            (select array_agg(billbbl) all_pad_billbbl from pad_bbl) f
                                            where not p.bbl = any(all_pad_billbbl)
                                 """,SV.T.eng).no_pluto_bbl_not_in_pad_bbl_billbbl[0] == True

                ## PROVE THAT ALL pad_bbl_billbbl EXIST IN pluto_bbl
                assert SV.T.pd.read_sql(""" select count(*)=0 no_pad_bbl_billbbl_not_in_pluto_bbl
                                            from pad_bbl p,
                                            (select array_agg(bbl) all_pluto_bbl from pluto) f
                                            where not p.billbbl = any(all_pluto_bbl)
                                 """,SV.T.eng).no_pad_bbl_billbbl_not_in_pluto_bbl[0] == True

                ## CLEAN UP PLUTO BBL JUST TO BE SAFE
                reset_pluto_bbl="""
                    update pluto set bbl =
                    regexp_replace( '1'||
                                    to_char(block::integer,'00000')||
                                    to_char(lot::integer,'0000'),'[[:space:]]','','g')::numeric(10)
                    """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     reset_pluto_bbl)

                # cross_copy_bbl_if_existing_in_pluto

                prepare_to_cross_copy="""

                    update pad_adr set billbbl = null;

                    update pad_adr a set billbbl = a.bbl
                    from (select array_agg(distinct bbl) all_pluto_bbl from pluto where bbl is not null) f
                    where a.billbbl is null and array[a.bbl] && all_pluto_bbl; --59187 rows affected

                    delete from pad_adr p
                    using (select array_agg(block) pluto_blocks from pluto) f1
                    where p.billbbl is null
                    and not p.block = any(pluto_blocks); --577 rows affected, 571 ms execution time.


                    alter table  pad_adr add column tmp boolean default false;
                    update pad_adr set tmp = true where billbbl is null;
                """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     prepare_to_cross_copy)

                # CROSS MATCH BBL (I.E., WHERE BBL ON ONE ADDRESS HOSTS ADDRESS FOR OTHER SURROUNDING STREETS)
                cross_match_bbls="""
                    update pad_adr p set billbbl = (select billbbl from pad_bbl where lobbl=_bbl or hibbl=_bbl)
                    from
                        (select uid,bbl from pad_adr where billbbl is null) a,
                        (select array_agg(lobbl) all_lo,array_agg(hibbl) all_hi,array_agg(billbbl) all_bill from pad_bbl) b,
                        (
                        select distinct _bbl
                        from
                            (
                            select unnest(array_cat(array_agg(distinct lobbl),
                                                    array_cat(array_agg(distinct hibbl),
                                                    array_agg(distinct billbbl)))) _bbl
                            from
                                (
                                select lobbl,hibbl,billbbl
                                from pad_bbl,
                                (select array_agg(fa1.bbl) null_bbls
                                    from (select bbl from pad_adr
                                            where billbbl is null
                                            order by uid limit 10) fa1) f1
                                where
                                (array[lobbl] && null_bbls
                                or array[hibbl] && null_bbls
                                or array[billbbl] && null_bbls)
                                ) f2
                            ) f3
                        where _bbl is not null
                        ) f5
                    where a.bbl = _bbl
                    and (array[_bbl] && all_lo
                    or array[_bbl] && all_hi
                    or array[_bbl] && all_bill )
                    and p.uid = a.uid;
                    """
                cnt="select count(*) cnt from pad_adr where billbbl is null;"
                pts_left = SV.T.pd.read_sql(cnt,SV.T.eng).cnt[0]
                pt = 0
                while pts_left>0:
                    pts_left_a = SV.T.pd.read_sql(cnt,SV.T.eng).cnt[0]
                    SV.T.conn.set_isolation_level(        0)
                    SV.T.cur.execute(                     cross_match_bbls)
                    pt+=10
                    print pt
                    pts_left_b = SV.T.pd.read_sql(cnt,SV.T.eng).cnt[0]
                    if pts_left_b==0 or pts_left_a==pts_left_b:
                        break

                cross_match_bbls_2="""
                update pad_adr p set billbbl = _bbl
                from
                    (select array_agg(bbl) pluto_bbls from pluto where bbl is not null) f0,
                    (
                    select f1.uid uid,f1.tmp tmp,unnest(  array_cat(array_agg(distinct lobbl),
                                        array_cat(array_agg(distinct hibbl),
                                        array_agg(distinct billbbl)))) _bbl
                    from
                        pad_bbl,
                            (
                            select uid,bbl tmp,array_agg(bbl) null_bbls
                            from (select distinct bbl,uid from pad_adr where billbbl is null order by uid offset %d limit 10) af1
                            group by uid,tmp
                            ) f1
                        where
                        (array[lobbl] && null_bbls
                        or array[hibbl] && null_bbls
                        or array[billbbl] && null_bbls)
                    group by f1.uid,f1.tmp
                    ) f2
                where _bbl!=f2.tmp and _bbl is not null
                and array[_bbl] && pluto_bbls
                and p.uid = f2.uid
                returning f2.uid uid
                """

                cnt="select count(distinct bbl) cnt from pad_adr where billbbl is null;"
                pts_left = SV.T.pd.read_sql(cnt,SV.T.eng).cnt[0]
                pt,_offset = 0,0
                while pts_left>0:
                    pts_left_a = SV.T.pd.read_sql(cnt,SV.T.eng).cnt[0]
                    res = SV.T.pd.read_sql(  cross_match_bbls_2 % _offset  ,SV.T.eng)
                    updates = len(res)
                    if updates==0:
                        _offset += 10
                        if _offset>=pts_left_a:
                            break
                    elif updates<10:
                        _offset += 10-updates
                    print updates
                    pts_left_b = SV.T.pd.read_sql(cnt,SV.T.eng).cnt[0]


                a="""   select * from pad_adr p, (select array_agg(distinct block) bbl_blocks
                        from pad_adr where billbbl is null) f1
                        where array[p.block] && bbl_blocks"""
                df = SV.T.pd.read_sql(a,SV.T.eng)
                df['billbbl'] = df.billbbl.fillna(value=0).astype(int)
                g = df.groupby('block')

                def update_resp_dict(locs,src):
                    D={}
                    _billbbl = 0
                    for it in locs:
                        _uid = src.ix[it,'uid']
                        if it==0:
                            n = range(1,len(src))
                        elif it==src.index.tolist()[-1:]:
                            n = range(len(src)-1)
                            n.reverse()
                        else:
                            n=range(it,len(src)-1)
                        for i in n:
                            if src.ix[i,'billbbl']:
                                _billbbl = src.ix[i,'billbbl']
                                break
                        D.update({ _uid : _billbbl })
                    return D

                D={}
                t=list(g.groups.iteritems())
                for k,v in g.groups.iteritems():
                    grp = g.get_group(k).sort('min_num')
                    odds = grp[grp.parity=='1'].copy().reset_index(drop=True)
                    evens = grp[grp.parity=='2'].copy().reset_index(drop=True)
                    assert len(odds)+len(evens)==len(grp)

                    odd_blanks,even_blanks = list(odds.billbbl==0),list(evens.billbbl==0)

                    pt,i_odd,odd_idx = 0,[],odds.index.tolist()
                    for j in range(odd_blanks.count(True)):
                        pt = odd_blanks.index(True,pt)
                        i_odd.append( pt )
                        pt += 1

                    pt,i_even,even_idx = 0,[],evens.index.tolist()
                    for j in range(even_blanks.count(True)):
                        pt = even_blanks.index(True,pt)
                        i_even.append( pt )
                        pt += 1

                    odd_locs = [odd_idx[it] for it in i_odd]
                    if odd_locs:
                        D.update(update_resp_dict(odd_locs,odds))

                    even_locs = [even_idx[it] for it in i_even]
                    if even_locs:
                        D.update(update_resp_dict(even_locs,evens))

                assert len(D.keys())==len(df[df.billbbl==0])

                ndf = SV.T.pd.DataFrame(columns=['_uid','_billbbl'],data={'_uid':D.keys(),'_billbbl':D.values()})
                ndf.head()
                ndf = ndf[ndf._billbbl!=0].copy().reset_index(drop=True)
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute('drop table if exists tmp')
                ndf.to_sql('tmp',SV.T.eng,index=False)
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute("""update pad_adr p set billbbl = t._billbbl::numeric
                                    from tmp t where p.uid = t._uid;
                                    drop table if exists tmp;""")

                print "Had to look up blocks by hand for remaining 20 BBLs"

                # CREATE TMP_IDX
                a="""

                drop table if exists tmp_addr_idx_pad;

                create table tmp_addr_idx_pad as
                    select
                        predir,
                        name street_name,
                        suftype,
                        sufdir
                    from z_parse_ny_addrs(
                        'select distinct on (stname) uid::bigint gid,''0 ''||stname address, zipcode::bigint from pad_adr
                        where
                        stnum_w_letter is false
                        ');

                select z_make_column_primary_serial_key('tmp_addr_idx_pad','gid',true);

                """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     a)

                # INSERT PARSED INFO ONTO PAD_ADR
                a="""
                    update pad_adr t set street_name = f1.parsed_addr
                    from
                    (select src_gid,concat_ws(' ',predir,name,suftype,sufdir) parsed_addr
                    from z_parse_ny_addrs('select uid::bigint gid,''0 ''||stname address, zipcode::bigint from pad_adr
                                            where stnum_w_letter is false and street_name is null
                                            ')) f1
                    where f1.src_gid::bigint = t.uid::bigint;
                    """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(            a)
                cnt_str = "select count(*) cnt from pad_adr where stnum_w_letter is false and street_name is null"
                cnt = cnt_a = cnt_b = SV.T.pd.read_sql(cnt_str,SV.T.eng).cnt[0]
                while cnt>0 and cnt_a!=cnt_b:
                    cnt_a=SV.T.pd.read_sql(cnt_str,SV.T.eng).cnt[0]
                    SV.T.conn.set_isolation_level(        0)
                    SV.T.cur.execute(            a)
                    cnt = cnt_b = SV.T.pd.read_sql(cnt_str,SV.T.eng).cnt[0]

                assert SV.T.pd.read_sql(""" select count(*)=0 all_addr_in_idx_found_in_pad_adr from
                                            tmp_addr_idx_pad t,
                                            (select array_agg(distinct street_name) all_parsed from pad_adr) f1
                                            where not concat_ws(' ',t.predir,t.street_name,t.suftype,t.sufdir) = any (all_parsed)
                                            """,SV.T.eng).all_addr_in_idx_found_in_pad_adr[0] == True

                # ADD MIN/MAX ADDRESS NUMBERS
                a="""

                update pad_adr set min_num = lhnd::integer where stnum_w_letter is false;
                update pad_adr set max_num = hhnd::integer where stnum_w_letter is false;

                -- UPDATE ALONG lhnd
                update pad_adr set min_num = regexp_replace(lhnd,' 1/3','.33')::double precision
                where stnum_w_letter = true and lhnd ilike '% 1/3%';
                update pad_adr set min_num = regexp_replace(lhnd,' 3/4','.75')::double precision
                where stnum_w_letter = true and lhnd ilike '% 3/4%';
                update pad_adr set min_num = regexp_replace(lhnd,' 1/4','.25')::double precision
                where stnum_w_letter = true and lhnd ilike '% 1/4%';
                update pad_adr set min_num = regexp_replace(lhnd,' 1/2','.5')::double precision
                where stnum_w_letter = true and lhnd ilike '% 1/2%';

                -- UPDATE ALONG hhnd
                update pad_adr set max_num = regexp_replace(hhnd,' 3/4','.75')::double precision
                where stnum_w_letter = true and hhnd ilike '% 3/4%';
                update pad_adr set max_num = regexp_replace(hhnd,' 1/4','.25')::double precision
                where stnum_w_letter = true and hhnd ilike '% 1/4%';
                update pad_adr set max_num = regexp_replace(hhnd,' 1/3','.33')::double precision
                where stnum_w_letter = true and hhnd ilike '% 1/3%';
                update pad_adr set max_num = regexp_replace(hhnd,' 1/2','.5')::double precision
                where stnum_w_letter = true and hhnd ilike '% 1/2%';

                --WHERE '-' in street_num
                update pad_adr set min_num = regexp_replace(lhnd,'-','.','g')::double precision
                where stnum_w_letter = true and lhnd ilike '%%-%%';
                update pad_adr set max_num = regexp_replace(hhnd,'-','.','g')::double precision
                where stnum_w_letter = true and hhnd ilike '%%-%%';

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       a)

                # USE tmp TABLE TO FORMAT ROWS (where stnum_w_letter=True)
                # 1. add rows where lhnd = hhnd
                #
                # 2. ADD ROWS FOR EACH ADDRESS WHERE lhnd and hhnd have street number and one letter, e.g., (304A,304F)
                # 	    make rows for each letter up to and including last letter, e.g., 304A,304B,...304F
                #
                # 3. ADD ROWS WHERE lhnd = hhnd + 'A' , e.g., (304,304A)
                # 	    make row with just number
                # 	    make row with number+A
                #
                # 4. ADD ROWS WHERE lhnd != hhnd and hhnd = # + A, e.g., (300,304A)
                # 	    make row with just number range, e.g., 300...304
                # 	    make row with last number + A, e.g., 304A
                #
                # 5. ADD ROWS WHERE lhnd = hhnd + letter other than 'A' , e.g., (304,304F)
                # 	    make row with just number, e.g., 304
                # 	    make rows for each letter up to and including last letter, e.g., 304A,304B,...304F
                #
                #
                # 6. ADD ROWS WHERE lhnd != hhnd and hhnd = # + letter other than 'A' , e.g., (300,304F)
                # 	    make row with just number range, e.g., 300...304
                # 	    make rows for each letter up to and including last letter, e.g., 304A,304B,...304F
                #
                #
                # 7. ADD ROWS WHERE lhnd+'A' and hhnd, e.g., (4A,6)
                # 	    make row with just letter, e.g., 4A
                # 	    make row with range, e.g., (4,6)
                #
                #
                # 8. ADD ROWS WHERE lhnd!=hhnd,lhnd + word_l,hhnd + word_r,word_l=word_r, e.g., (501 REAR,503 REAR)
                # 	    make row for each number (ALONG PARITY) between lhnd and hhnd
                #
                #
                # 9. REMOVE ROWS ALREADY (and recently done) MANAGED IN pad_adr
                #
                # ### PROVE THAT ALL EXPECTED UID EXIST IN TMP TABLE
                # ### PROVE THAT ALL ROWS IN TMP WHERE min/max_num ARE NULL THEN BOTH min/max_num ARE NULL
                # ### PROVE THAT ONLY ROWS IN tmp WITHOUT min/max_num ARE THOSE ROWS WITH LETTERS
                # # FINISH WITH TMP (update,delete related rows from pad_adr,re-insert rows from tmp,delete tmp)

                one="""
                -- ADD ROWS WHERE lhnd = hhnd
                create table tmp as
                    select * from pad_adr
                    where stnum_w_letter is true
                    and lhnd = hhnd;

                select z_make_column_primary_serial_key('tmp','uid',false);

                alter table tmp add column src_gid integer;
                update tmp set src_gid = uid;
                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       one)
                # 2.
                two="""
                -- ADD ROWS FOR EACH ADDRESS WHERE lhnd and hhnd have street number and one letter, e.g., (304A,304F)
                --      make rows for each letter up to and including last letter, e.g., 304A,304B,...304F

                insert into tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)

                select  1 boro,
                        f2.src_gid,
                        (z).alpha_range lhnd,
                        (z).alpha_range hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,regexp_replace(lhnd,no_num_l,'','g') base_str,
                            lhnd,hhnd,no_num_l,no_num_r,z_make_rows_with_alpha_range(uid,only_num_l,no_num_l,no_num_r,false) z
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where not f1.no_num_l = any (array['','-'])
                    and length(no_num_r)=1

                    ) f2

                where p.uid = f2.uid
                order by p.uid,lhnd;

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       two)
                # 3.
                three="""

                -- ADD ROWS WHERE lhnd =  hhnd + 'A' , e.g., (304,304A)
                --      make row with just number
                --      make row with number+A

                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl,parity,min_num,max_num)

                select  1,
                        f2.src_gid,
                        f2.lhnd,  --PART 1/2
                        f2.lhnd hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl,
                        p.parity,
                        f2.only_num_l::integer min_num,
                        f2.only_num_l::integer max_num
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,lhnd,hhnd,only_num_l

                    from
                        (
                        select uid src_gid,uid,lhnd,hhnd,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where f1.no_num_l = any (array['','-'])
                    and no_num_r='A'
                    and lhnd=trim(trailing 'A' from hhnd)


                    ) f2
                where p.uid = f2.uid
                order by p.uid;


                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)
                select  1,
                        f2.src_gid,
                        f2.hhnd lhnd,  --PART 2/2
                        f2.hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,lhnd,hhnd

                    from
                        (
                        select uid src_gid,uid,lhnd,hhnd,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where f1.no_num_l = any (array['','-'])
                    and no_num_r='A'
                    and lhnd=trim(trailing 'A' from hhnd)


                    ) f2
                where p.uid = f2.uid
                order by p.uid;

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       three)
                # 4.
                four="""

                -- ADD ROWS WHERE lhnd != hhnd and hhnd = # + A, e.g., (300,304A)
                --    make row with just number range, e.g., 300...304
                --    make row with last number + A, e.g., 304A


                insert into tmp (boro,src_gid,stname,zipcode,bbl,billbbl,parity,min_num,max_num)

                --PART 1/2
                select  1,
                        f2.src_gid,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl,
                        p.parity,
                        only_num_l::integer min_num,
                        only_num_r::integer max_num
                from
                    pad_adr p,
                    (
                    select uid src_gid,uid,
                        lhnd,hhnd,
                        only_num_l,only_num_r,
                        no_num_l,no_num_r
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where f1.no_num_l = any (array['','-'])
                    and no_num_r='A'
                    and not lhnd=trim(trailing 'A' from hhnd)
                    ) f2

                where p.uid = f2.uid
                order by p.uid;


                select  1,
                        f2.src_gid,
                        --f2.lhnd real_lhnd,
                        f2.hhnd lhnd,
                        f2.hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,
                        lhnd,hhnd,
                        only_num_l,only_num_r,
                        no_num_l,no_num_r
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where f1.no_num_l = any (array['','-'])
                    and no_num_r='A'
                    and not lhnd=trim(trailing 'A' from hhnd)


                    ) f2
                where p.uid = f2.uid
                order by p.uid;


                --PART 2/2
                insert into tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)
                select  1 boro,
                        f2.src_gid,
                        f2.hhnd lhnd,
                        f2.hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,
                        lhnd,hhnd,
                        only_num_l,only_num_r,
                        no_num_l,no_num_r
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where f1.no_num_l = any (array['','-'])
                    and no_num_r='A'
                    and not lhnd=trim(trailing 'A' from hhnd)


                    ) f2
                where p.uid = f2.uid
                order by p.uid;

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       four)
                # 5.
                five="""

                -- ADD ROWS WHERE lhnd = hhnd and hhnd = # + letter other than A, e.g., (304,304C)
                --    make rows with just number, e.g., 304
                --    make row with last number + A, e.g., 304A,304B,304C


                -- PART 1/2
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)
                select  1,
                        f2.src_gid,
                        --f2.lhnd,
                        --f2.hhnd,
                        (z).alpha_range lhnd,
                        (z).alpha_range hhnd,
                        --f2.only_num_l,
                        --f2.no_num_l,
                        --f2.no_num_r,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,regexp_replace(lhnd,no_num_l,'','g') base_str,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                        ,z_make_rows_with_alpha_range(uid,only_num_l,'A',no_num_r,false) z
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l = ''
                    and f1.no_num_r = any (array['B','C','D','E','F','G','H','I','J','K','L','M'])
                    and only_num_l=only_num_r


                    ) f2
                where p.uid = f2.uid
                order by p.uid;


                -- PART 2/2
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl,min_num,max_num)
                select  1 boro,
                        f2.src_gid,
                        f2.lhnd lhnd,
                        f2.lhnd hhnd ,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl,
                        only_num_l::integer min_num,
                        only_num_r::integer max_num
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,regexp_replace(lhnd,no_num_l,'','g') base_str,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                        --,z_make_rows_with_alpha_range(uid,only_num_l,'A',no_num_r,false) z
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l = ''
                    and f1.no_num_r = any (array['B','C','D','E','F','G','H','I','J','K','L','M'])
                    and only_num_l=only_num_r


                    ) f2
                where p.uid = f2.uid
                order by p.uid--,lhnd;

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       five)
                # 6.
                six="""

                -- ADD ROWS WHERE lhnd != hhnd and hhnd = # + letter other than A, e.g., (300,304B)
                --    make row with just number range, e.g., 300...304
                --    make row with last number + A, e.g., 304A,304B



                --PART 1/2: row with range
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl,parity,min_num,max_num)
                select  1 boro,
                        f2.src_gid,
                        only_num_l lhnd,
                        only_num_r hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl,
                        p.parity,
                        only_num_l::integer min_num,
                        only_num_r::integer max_num
                from
                    pad_adr p,
                    (
                    select uid src_gid,uid,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l = ''
                    and f1.no_num_r = any (array['B','C','D','E','F','G','H','I','J','K','L','M'])
                    and only_num_l!=only_num_r
                    ) f2
                where p.uid = f2.uid
                order by p.uid;



                --PART 2/2: rows with alpha_range
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)
                select  1 boro,
                        f2.src_gid,
                        --f2.lhnd,
                        --f2.hhnd,
                        (z).alpha_range lhnd,
                        (z).alpha_range hhnd,
                        --f2.only_num_l,
                        --f2.no_num_l,
                        --f2.no_num_r,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (

                    select uid src_gid,uid,regexp_replace(lhnd,no_num_l,'','g') base_str,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                        ,z_make_rows_with_alpha_range(uid,only_num_l,'A',no_num_r,false) z
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l = ''
                    and f1.no_num_r = any (array['B','C','D','E','F','G','H','I','J','K','L','M'])
                    and only_num_l!=only_num_r


                    ) f2
                where p.uid = f2.uid
                order by p.uid,lhnd;

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       six)
                # 7.
                seven="""
                --ADD ROWS WHERE lhnd+'A' and hhnd, e.g., (4A,6)
                --    make row with just letter, e.g., 4A
                --    make row with range, e.g., (4,6)

                --PART 1/2
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)
                select  1 boro,
                        f2.src_gid,
                        f2.lhnd lhnd,
                        f2.lhnd hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (
                    select uid src_gid,uid,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l = 'A'
                    and f1.no_num_r = ''
                    and only_num_l!=only_num_r
                    ) f2
                where p.uid = f2.uid
                and not
                    (
                        f2.lhnd ilike any (array['%% 1/2%%','%% 3/4%%','%%-%%'])
                        or f2.hhnd ilike any (array['%% 1/2%%','%% 3/4%%','%%-%%'])
                    )
                order by p.uid;


                --PART 2/2
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl,parity,min_num,max_num)
                select  1 boro,
                        f2.src_gid,
                        only_num_l lhnd,
                        only_num_r hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl,
                        p.parity,
                        only_num_l::integer min_num,
                        only_num_r::integer max_num
                from
                    pad_adr p,
                    (
                    select uid src_gid,uid,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l = 'A'
                    and f1.no_num_r = ''
                    and only_num_l!=only_num_r
                    ) f2
                where p.uid = f2.uid
                order by p.uid;
                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       seven)
                # 8.
                eight="""
                --ADD ROWS WHERE lhnd!=hhnd,lhnd + word_l,hhnd + word_r,word_l=word_r, e.g., (501 REAR,503 REAR)
                --    make row for each number (ALONG PARITY) between lhnd and hhnd
                INSERT INTO tmp (boro,src_gid,lhnd,hhnd,stname,zipcode,bbl,billbbl)
                select  1 boro,
                        f2.src_gid,
                        concat_ws(' ',(z).res_i,no_num_l) lhnd,
                        concat_ws(' ',(z).res_i,no_num_l) hhnd,
                        p.stname,
                        p.zipcode,
                        p.bbl,
                        p.billbbl
                from
                    pad_adr p,
                    (
                    select uid src_gid,uid,
                        lhnd,hhnd,only_num_l,only_num_r,no_num_l,no_num_r
                        ,z_make_rows_with_numeric_range(uid,only_num_l::integer,only_num_r::integer,true) z
                    from
                        (
                        select uid,lhnd,hhnd,
                            regexp_replace(lhnd,'[^0-9]*','','g') only_num_l,
                            regexp_replace(hhnd,'[^0-9]*','','g') only_num_r,
                            regexp_replace(lhnd,'[0-9]*','','g') no_num_l,
                            regexp_replace(hhnd,'[0-9]*','','g') no_num_r
                        from pad_adr
                        where stnum_w_letter is true
                        and lhnd != hhnd
                        order by stname
                        ) f1
                    where
                    f1.no_num_l != ''
                    and f1.no_num_r != ''
                    and ( length(no_num_l)>0 or length(no_num_r)>0 )
                    and only_num_l!=only_num_r
                    and no_num_l=no_num_r
                    ) f2
                where p.uid = f2.uid
                and not
                    (
                        f2.lhnd ilike any (array['%% 1/2%%','%% 3/4%%','%%-%%'])
                        or f2.hhnd ilike any (array['%% 1/2%%','%% 3/4%%','%%-%%'])
                    )
                order by p.uid,(z).res_i;
                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       eight)
                # 9.
                nine="""
                -- REMOVE ROWS ALREADY (and recently done) MANAGED IN pad_adr
                delete from tmp
                where   stnum_w_letter = true
                        and (
                        (lhnd ilike '% 3/4%'
                        or lhnd ilike '% 1/4%'
                        or lhnd ilike '% 1/3%'
                        or lhnd ilike '% 1/2%'
                        or lhnd ilike '%-%')
                        or
                        (hhnd ilike '% 3/4%'
                        or hhnd ilike '% 1/4%'
                        or hhnd ilike '% 1/3%'
                        or hhnd ilike '% 1/2%'
                        or hhnd ilike '%-%')
                        );
                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       nine)

                ### PROVE THAT ALL EXPECTED UID EXIST IN TMP TABLE
                assert PG.T.pd.read_sql(    """ select count(*)=0 all_expected_uid_from_pad_adr_exist_in_tmp
                                                from
                                                (select distinct on (uid) * from pad_adr
                                                    where stnum_w_letter = true and not
                                                    (
                                                        lhnd ilike any (array['%% 1/2%%','%% 3/4%%','%% 1/4%%','%% 1/3%%','%%-%%'])
                                                        or hhnd ilike any (array['%% 1/2%%','%% 3/4%%','%% 1/4%%','%% 1/3%%','%%-%%'])
                                                    )) p,
                                                (select array_agg(distinct src_gid) all_t_uid from tmp) f2

                                                where not array[p.uid] && all_t_uid
                                            """,PG.T.eng).all_expected_uid_from_pad_adr_exist_in_tmp[0]==True

                ### PROVE THAT ALL ROWS IN TMP WHERE min/max_num ARE NULL THEN BOTH min/max_num ARE NULL
                assert PG.T.pd.read_sql("""
                                            select count(*)=0 min_max_null_or_not_null_equally
                                            from tmp where (min_num is null or max_num is null)
                                            and not (min_num is null and max_num is null)
                                        """,PG.T.eng).min_max_null_or_not_null_equally[0]==True

                ### PROVE THAT ONLY ROWS IN tmp WITHOUT min/max_num ARE THOSE ROWS WITH LETTERS
                assert PG.T.pd.read_sql("""
                                            select count(*)=0 rows_without_letters_and_without_min_max
                                            from tmp where
                                            (min_num is null and not lhnd ~ '[a-zA-z]')
                                            or (max_num is null and not hhnd ~ '[a-zA-z]')
                                        """,PG.T.eng).rows_without_letters_and_without_min_max[0]==True

                # FINISH WITH TMP (update,delete related rows from pad_adr,re-insert rows from tmp,delete tmp)
                finish_up="""

                -- ADD parser_info TO pad_adr WHERE NO STREET_NAME (should be those rows with lhnd or hhnd having '1/2', etc...)
                WITH upd AS (
                    SELECT  src_gid,predir,name street_name,suftype,sufdir
                    FROM    z_parse_NY_addrs('
                                            select
                                                uid::bigint gid,
                                                concat_ws('' '',''0'',stname)::text address,
                                                zipcode::bigint zipcode
                                            FROM pad_adr
                                            WHERE street_name is null
                                            ')
                    )
                UPDATE pad_adr t set
                    predir = u.predir,
                    street_name = u.street_name,
                    suftype = u.suftype,
                    sufdir = u.sufdir
                FROM  upd u
                WHERE u.src_gid = t.uid::bigint;

                -- ADD parser_info TO tmp
                WITH upd AS (
                    SELECT  src_gid,predir,name street_name,suftype,sufdir
                    FROM    z_parse_NY_addrs('
                                            select
                                                uid::bigint gid,
                                                concat_ws('' '',''0'',stname)::text address,
                                                zipcode::bigint zipcode
                                            FROM tmp
                                            ')

                    )
                UPDATE tmp t set
                    predir = u.predir,
                    street_name = u.street_name,
                    suftype = u.suftype,
                    sufdir = u.sufdir
                FROM  upd u
                WHERE u.src_gid = t.uid::bigint;


                -- COPY OVER REMAINING DATA TO tmp
                update tmp t set
                    block = p.block,
                    lot = p.lot,
                    bin = p.bin,
                    lhns = p.lhns,
                    lcontpar = p.lcontpar,
                    lsos = p.lsos,
                    hhns = p.hhns,
                    hcontpar = p.hcontpar,
                    hsos = p.hsos,
                    scboro = p.scboro,
                    sc5 = p.sc5,
                    sclgc = p.sclgc,
                    stname = p.stname,
                    addrtype = p.addrtype,
                    realb7sc = p.realb7sc,
                    validlgcs = p.validlgcs,
                    b10sc = p.b10sc,
                    segid = p.segid
                from pad_adr p
                where t.src_gid = p.uid;

                -- DELETE FROM pad_adr THOSE ROWS ABOUT TO BE RE-INSERTED BACK INTO pad_adr FROM tmp
                delete from pad_adr p
                using (select array_agg(distinct src_gid) all_t_uid from tmp) t
                where array[p.uid] && all_t_uid;


                -- RE-INSERT FORMATTED ROWS FROM TMP
                insert into pad_adr (boro,block,lot,bin,
                    lhnd,lhns,lcontpar,lsos,
                    hhnd,hhns,hcontpar,hsos,
                    scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,segid,
                    uid,bbl,stnum_w_letter,
                    predir,street_name,suftype,sufdir,min_num,max_num,billbbl,tmp)
                select
                    boro,block,lot,bin,
                    lhnd,lhns,lcontpar,lsos,
                    hhnd,hhns,hcontpar,hsos,
                    scboro,sc5,sclgc,stname,addrtype,realb7sc,validlgcs,parity,b10sc,segid,
                    uid,bbl,stnum_w_letter,
                    predir,street_name,suftype,sufdir,min_num,max_num,billbbl,tmp
                from tmp;

                -- DROP tmp TABLE
                DROP TABLE tmp;

                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       finish_up)

                # TIGHTEN THINGS UP

                # 1. Satisfy/Prove condition:  no_rows_where_only_min_or_max_num_is_null
                t="""   update pad_adr set min_num = lhnd::integer
                        where (min_num is null and max_num is not null);

                        update pad_adr set max_num = hhnd::integer
                        where (max_num is null and min_num is not null);
                """
                SV.T.conn.set_isolation_level(          0)
                SV.T.cur.execute(                       t)
                assert PG.T.pd.read_sql(""" select count(*)=0 no_rows_where_only_min_or_max_num_is_null
                                            from pad_adr where
                                            (min_num is null and max_num is not null)
                                            or
                                            (max_num is null and min_num is not null)
                                        """,PG.T.eng).no_rows_where_only_min_or_max_num_is_null[0]==True

                # 2. Prove that all_rows_where_min_or_max_is_null_have_lhnd_equal_hhnd
                assert PG.T.pd.read_sql(""" select count(*)=0 all_rows_where_min_or_max_is_null_have_lhnd_equal_hhnd
                                            from pad_adr
                                            where (min_num is null or max_num is null) and lhnd != hhnd
                                        """,PG.T.eng).all_rows_where_min_or_max_is_null_have_lhnd_equal_hhnd[0]==True


                # ** BELOW NEEDS TO BE INTEGRATED WITH ABOVE
                a="""
                    -- UPDATE MIN/MAX BLDG NUMBERS
                    update pad_adr set min_num = lhnd::double precision,max_num = hhnd::double precision
                    where stnum_w_letter is false;

                    -- ADD PARSED INFO
                    alter table pad_adr add column tmp_addr text;
                    update pad_adr set tmp_addr = concat_ws(' ',min_num::integer,stname) where stnum_w_letter is false;

                    select z_update_with_parsed_info(array_agg(p.uid),'pad_adr','uid','tmp_addr','zipcode',
                                                        array['street_name','predir','suftype','sufdir'])
                    from pad_adr p where stnum_w_letter is false;

                    alter table pad_adr drop column if exists tmp_addr;

                    -- MAKE LINKAGE B/T pad_adr AND pluto
                    --    (having already asserted all pad_bbl.billbbl exist in pluto)
                    update pad_adr a set billbbl = b.billbbl
                        from pad_bbl b where
                        b.lobbl = a.bbl
                        or b.hibbl = a.bbl
                        or b.billbbl = a.bbl;

                    -- ADD GEOM/BBL BASE ON MATCHES WITH PLUTO
                    select z_update_with_geom_from_parsed(array_agg(p.uid),'pad_adr','uid')
                    from pad_adr p where stnum_w_letter is false;

                """
                SV.T.conn.set_isolation_level(        0)
                SV.T.cur.execute(                     a)

                # ** THIS SHOULD BE AN ASSERTION
                a="""
                    select count(*) from pad_adr where
                    stnum_w_letter is false
                    and
                        (street_name is null
                        or (suftype is null and
                            not (
                                street_name ilike '%broadway%'
                                or street_name ilike '%bowery%'
                                or street_name ilike '%slip%'
                                )
                            )
                        or min_num is null
                        or max_num is null)
                """

            def pluto(self):
                from f_nyc_data                 import NYC_Data
                ND                          =   NYC_Data()
                df                          =   ND.get_latest_links_from_nyc_bytes()

                import re
                shape_files                 =   df[(df.Data_Set.str.contains('mappluto',flags=re.I)) & (df.file_type=='zip')]
                link                        =   shape_files[shape_files.link.str.contains('mn_')].link.iloc[0]

                save_dir                    =   'NYC_data/'
                save_path                   =   save_dir + '2015.06.01_mn_shape_file.zip'
                assert self.T.download_file(link, save_path)==True

                import zipfile
                z                           =   zipfile.ZipFile(save_path)
                z.extractall(                   )
                zip_dir                     =   z.filelist[0].filename[:z.filelist[0].filename.find('/')]

                import getpass
                sql_file                    =   save_dir + 'mn_pluto.sql'
                log_f                       =   save_dir + 'shp_import.log'
                pw                          =   getpass.getpass('root password?\t')
                tbl_name                    =   self.T.PLUTO_TBL
                print 'Using title "%s" for pluto table.' % tbl_name

                from subprocess                 import Popen            as sub_popen
                from subprocess                 import PIPE             as sub_PIPE

                cmds                        =   ['echo "ALTER EVENT TRIGGER missing_primary_key_trigger DISABLE;" > %s' % sql_file,
                                                 'shp2pgsql -s 2263:4326 Manhattan/MNMapPLUTO.shp %s >> %s 2> %s' % (tbl_name,sql_file,log_f),
                                                 'echo "ALTER EVENT TRIGGER missing_primary_key_trigger ENABLE;" >> %s' % sql_file,
                                                 ' '.join(['echo "%s" |' % pw,
                                                           'sudo -S --prompt=\'\' su postgres -c',
                                                           '"psql -h %(DB_HOST)s -p %(DB_PORT)s %(DB_NAME)s' % self.T,
                                                           '< %s" 2>&1 > %s 2>&1' % (sql_file,log_f)])
                                                ]
                (_out,_err)                 =   sub_popen('; '.join(cmds),stdout=sub_PIPE,shell=True).communicate()
                assert not _out
                assert _err is None
                clean_up=raw_input('remove all files (INCLUDING LOG: %s) that were created for this function? (y/n)\t' % log_f)
                if clean_up=='y':
                    cmds                    =   ['rm -fr %s' % save_dir,
                                                 'rm -fr %s' % zip_dir]
                    (_out,_err)             =   sub_popen('; '.join(cmds),stdout=sub_PIPE,shell=True).communicate()
                    assert not _out
                    assert _err is None
                return

            def mnv(self):
                qry = """
                    CREATE TABLE IF NOT EXISTS mnv (
                        building text,
                        camis bigint,
                        vend_name text,
                        clean_street text,
                        cuisinecode bigint,
                        dba text,
                        inspdate timestamp without time zone,
                        street text,
                        id integer,
                        recog_street boolean,
                        recog_addr boolean DEFAULT false,
                        bbl integer,
                        phone bigint,
                        norm_addr text,
                        seamless_id bigint,
                        yelp_id text,
                        address text,
                        geom geometry(Point,4326),
                        lot_cnt integer DEFAULT 1,
                        zipcode integer,
                        cuisine_description text,
                        grade text,
                        grade_date timestamp with time zone,
                        inspection_type text,
                        record_date timestamp with time zone,
                        violation_code text,
                        violation_description text
                    );
                """
                self.T.to_sql(                  qry)
                if not self.T.check_primary_key('mnv'):
                    self.T.make_column_primary_serial_key('mnv','uid')

            def turnstile_data(self):
                """
                NOTE:   NYC Turn Stile data format changed starting 10/18/14.
                        This function was developed before 10/18/14 and only applies to data in the older format.

                General Source for NYC Turn Stiles Info:   http://web.mta.info/developers/turnstile.html

                     - field description (current, as of 2015.09.01):
                        http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt

                     - data key:
                        http://web.mta.info/developers/resources/nyct/turnstile/Remote-Booth-Station.xls
                        --> this provides the Remote Unit/Control & Area/Station Name Key


                Remote and Station Name:
                    - see source for 'data kay' above


                Station Names and Coords here:
                     'http://web.mta.info/developers/data/nyct/subway/StationEntrances.csv'
                     - Relevant stations were added to pgsql as 'sub_stat_entr'

                """

                def add_station_geoms(remove_non_mn=False):
                    url                         =   'http://web.mta.info/developers/data/nyct/subway/StationEntrances.csv'
                    s                           =   self.T.pd.read_csv(url)
                    s.columns                   =   [it.lower().strip() for it in s.columns.tolist()]
                    self.T.to_sql(                  'drop table if exists sub_stations')
                    s.to_sql(                       'sub_stations',self.T.eng,index=False)

                    if not self.T.check_evt_trigger_enabled('missing_primary_key_trigger'):
                        self.T.to_sql(              """
                                                    alter table sub_stations add column uid serial primary key;
                                                    update sub_stations set uid = nextval(pg_get_serial_sequence('sub_stations','uid'));
                                                    """)

                    self.T.to_sql(                  'alter table sub_stations add column geom geometry(Point,4326)')
                    self.T.to_sql(                  "UPDATE sub_stations set geom = ST_SetSRID(ST_MakePoint(station_longitude,station_latitude),4326)")

                    if not self.T.check_table_exists(self.T.PLUTO_TBL):
                        print 'This function depends on NYC tax lot geometries (MapPLUTO) being available in pgSQL.'
                        print 'Currently this is looking for table "%s"' % self.T.PLUTO_TBL
                        print 'If no pluto table exists, run "import f_postgres as PG; pg=PG.pgSQL(); pg.Create.NYC.pluto();"'
                        print 'Else, consider changing the target table variable at the top of this function'
                        make_pluto = raw_input('\nRun the script to make pluto using table name: "%s" ? (y/n)\t' % self.T.PLUTO_TBL)
                        if make_pluto=='y':
                            self.T.pluto()
                        else:
                            raise SystemExit

                    if remove_non_mn:
                        # Remove subway stations that exist outside Manhattan
                        #   (where Manhattan, here, is all convex hull of all geometries in %(PLUTO_TBL)s)
                        self.T.to_sql(      """ DELETE FROM sub_stations
                                                WHERE NOT st_within(geom, (
                                                    SELECT ST_ConcaveHull(ST_Collect(f.the_geom),100,true) as geom
                                                    FROM (
                                                        SELECT *, (ST_Dump(geom)).geom as the_geom
                                                        FROM %s
                                                    ) As f
                                                ))
                                            """ % self.T.PLUTO_TBL)
                        print 'removed non-Manhattan subway stations'
                    return
                def add_subway_stops(remove_non_mn=False):
                    import zipfile, StringIO
                    r                           =   self.T.requests.get('http://web.mta.info/developers/data/nyct/subway/google_transit.zip')
                    z                           =   zipfile.ZipFile(StringIO.StringIO(r.content))
                    sub_stops                   =   self.T.pd.read_csv(z.open('stops.txt'))
                    self.T.to_sql(                  'drop table if exists sub_stops;')
                    sub_stops.to_sql(               'sub_stops',self.T.eng,index=False)

                    if not self.T.check_evt_trigger_enabled('missing_primary_key_trigger'):
                        self.T.to_sql(              """
                                                    alter table sub_stops add column uid serial primary key;
                                                    update sub_stops set uid = nextval(pg_get_serial_sequence('sub_stops','uid'));
                                                    """)

                    self.T.to_sql(                  """
                                                    ALTER table sub_stops add column geom geometry(Point,4326);
                                                    UPDATE sub_stops set geom = ST_SetSRID(ST_MakePoint(stop_lon,stop_lat),4326);
                                                    """)
                    if remove_non_mn:
                        self.T.to_sql(      """
                                            DELETE FROM sub_stops
                                            WHERE NOT st_within(geom, (
                                                SELECT ST_ConcaveHull(ST_Collect(f.the_geom),100,true) as geom
                                                FROM (
                                                    SELECT *, (ST_Dump(geom)).geom as the_geom
                                                    FROM %s
                                                ) As f
                                            ));
                                            """ % self.T.PLUTO_TBL)
                        print 'removed non-Manhattan subway stops'
                    return
                def add_turnstile_key_info():
                    ts_key                      =   self.T.pd.read_excel('http://web.mta.info/developers/resources/nyct/turnstile/Remote-Booth-Station.xls')
                    ts_key.columns              =   [str(it).lower().replace(' ','_') for it in ts_key.columns.tolist()]
                    ts_key['line_name']         =   ts_key['line_name'].map(lambda s: ''.join(sorted([str(it) for it in s])) if type(s)!=int else str(s))
                    # ts_key['clean']             =   ts_key.station.map(lambda s: s.lower())
                    self.T.to_sql(                  "drop table if exists ts_key")
                    ts_key.to_sql(                  'ts_key',self.T.eng,index=False)
                def get_data_fields_pre_change():
                    fields_url                  =   'http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description_pre-10-18-2014.txt'
                    fields_info                 =   self.T.requests.get(fields_url).content.split('\n')
                    for line in fields_info[1:]:
                        if line:
                            cols                =   line.split(',')
                            break
                    # cols                      =   ['C/A','UNIT','SCP','DATE1','TIME1','DESC1','ENTRIES1','EXITS1',
                    #                                'DATE2','TIME2','DESC2','ENTRIES2','EXITS2','DATE3','TIME3','DESC3','ENTRIES3',
                    #                                'EXITS3','DATE4','TIME4','DESC4','ENTRIES4','EXITS4','DATE5','TIME5','DESC5',
                    #                                'ENTRIES5','EXITS5','DATE6','TIME6','DESC6','ENTRIES6',
                    #                                'EXITS6','DATE7','TIME7','DESC7','ENTRIES7','EXITS7','DATE8',
                    #                                'TIME8','DESC8','ENTRIES8','EXITS8']
                    cols                        =   [str(it).lower().replace('/','_') for it in cols]
                    return cols
                def get_data_fields_post_change():
                    fields_url                  =   'http://web.mta.info/developers/resources/nyct/turnstile/ts_Field_Description.txt'
                    fields_info                 =   self.T.requests.get(fields_url).content.split('\n')
                    for line in fields_info[1:]:
                        if line:
                            cols                =   line.split(',')
                            break
                    # cols                      =   ['c_a', 'unit', 'scp', 'station', 'linename', 'division',
                    #                                'date', 'time', 'desc', 'entries', 'exits']
                    cols                        =   [str(it).lower().replace('/','_') for it in cols]
                    return cols
                def get_data_links(link_type='latest'):
                    url                     =   'http://web.mta.info/developers/turnstile.html'
                    from bs4                    import BeautifulSoup
                    req                     =   self.T.requests.get(url)
                    html                    =   BeautifulSoup(req.content)
                    base_url                =   req.url[:req.url.rfind('/')+1]

                    file_links              =   html.find_all('h2',text='Data Files')[0].parent.find_all('a')
                    if link_type=='latest':
                        return                  base_url + file_links[0].get('href')
                    elif link_type=='all':
                        _dates              =   map(lambda d: self.T.DU.parse(d.contents[0]),file_links)
                        _links              =   map(lambda d: base_url + d.get('href'),file_links)
                        df                  =   self.T.pd.DataFrame(data={'links':_links,'dates':_dates})
                        return                  df

                # tkm = ts_key_min
                def create_ts_key_min():
                    q="""
                        DROP TABLE IF EXISTS ts_key_min;
                        CREATE TABLE ts_key_min AS ( SELECT * from ts_key );
                        ALTER TABLE ts_key_min ADD COLUMN rk integer;
                        WITH res AS (
                            SELECT
                                uid,station,line_name,division,
                                ROW_NUMBER() OVER(PARTITION BY concat(station,line_name,division)
                                                  ORDER BY uid ASC) AS rk
                            FROM ts_key_min
                            )
                        UPDATE ts_key_min ts
                        SET rk = s.rk
                        FROM res s
                        WHERE s.uid = ts.uid;

                        -- minimalize ts_key_min
                        DELETE FROM ts_key_min WHERE not rk=1;
                        ALTER TABLE ts_key_min
                            DROP COLUMN rk;

                        -- redo uids (and reduce possible confusion)
                        WITH upd as (
                            SELECT uid,row_number() over() new_uid
                            FROM ts_key_min
                            )
                        UPDATE ts_key_min ts
                        SET uid = upd.new_uid
                        FROM upd
                        WHERE upd.uid = ts.uid;

                        -- update ts_key
                        ALTER TABLE ts_key ADD COLUMN ts_key_min_idx integer;
                        UPDATE ts_key ts SET ts_key_min_idx=tsm.uid
                        FROM ts_key_min tsm
                        WHERE concat(ts.station,ts.line_name,ts.division) = concat(tsm.station,tsm.line_name,tsm.division);

                    """

                    self.T.to_sql(              q)
                    q="""
                        select a=b as _check
                        from
                        ( select count(*) a from ts_key_min ) f1,
                        ( select count(distinct ts_key_min_idx) b from ts_key ) f2
                    """
                    assert self.T.pd.read_sql(q,self.T.eng)['_check'][0]==True
                def update_stops_via_overlapping_station_geoms(add_col=False):
                    t = '' if not add_col else 'ALTER TABLE sub_stops ADD COLUMN sub_station_idx integer;'
                    q = """
                        %s
                        UPDATE sub_stops _stops SET sub_station_idx = _stations.uid
                        FROM sub_stations _stations
                        WHERE _stations.geom && _stops.geom;

                        """ % t
                    self.T.to_sql(q)
                def update_stations_via_stops_idx_of_stations(add_col=False):
                    t='' if not add_col else 'ALTER TABLE sub_stations ADD COLUMN sub_stop_idx integer;'
                    q="""
                        %s
                        UPDATE sub_stations _stations SET sub_stop_idx = _stops.uid
                        FROM
                            (select uid,sub_station_idx from sub_stops
                            where location_type=1 and sub_station_idx is not null) _stops
                        WHERE  _stations.uid = _stops.sub_station_idx;
                    """ % t
                    self.T.to_sql(q)
                def update_stations_via_stations_with_stops_idx():
                    '''
                    RE: sub_stations, stations sharing {division,line,station_name} are assumed to be the same station,
                    and will be consolidated as so, and share the same sub_stop_idx (if any).
                    '''
                    q="""
                        WITH upd AS (
                            SELECT
                                uid,division,line,station_name,sub_stop_idx,
                                ROW_NUMBER() OVER(PARTITION BY concat(division,line,station_name)
                                                  ORDER BY uid ASC) AS rk
                            FROM sub_stations
                            WHERE sub_stop_idx is not null
                            )
                        UPDATE sub_stations _st
                        SET sub_stop_idx = s.sub_stop_idx
                        FROM upd s
                        WHERE concat(_st.division,_st.line,_st.station_name) = concat(s.division,s.line,s.station_name)
                        AND _st.sub_stop_idx is null;
                        """
                    self.T.to_sql(q)
                def update_stations_via_stations_overlapping_stops(and_within_meters=10):
                    q="""
                        UPDATE sub_stations _stats
                        SET sub_stop_idx = _stops.uid
                        FROM sub_stops _stops
                        WHERE _stops.geom = _stats.geom
                        AND _stats.sub_stop_idx IS NULL;
                        """
                    if and_within_meters:
                        q+="""
                            UPDATE sub_stations _stats
                            SET sub_stop_idx = _stop.uid
                            FROM
                            (SELECT uid,geom FROM sub_stops WHERE location_type=1) _stop
                            WHERE st_dwithin(_stats.geom::geography,_stop.geom::geography,10) --defaults to use_spheroid=true when ::geography; units are meters for WGS 84
                            AND sub_stop_idx IS NULL
                        """
                    self.T.to_sql(q)
                def update_stations_via_station_names_matching_stop_names():
                    q="""
                        UPDATE sub_stations _stats
                        SET sub_stop_idx = _stops.uid
                        FROM sub_stops _stops
                        WHERE _stats.sub_stop_idx IS NULL
                        AND _stops.location_type=1
                        AND _stops.stop_name = _stats.station_name
                    """
                    self.T.to_sql(q)

                def add_route_n_to_stations():
                    q = """
                        ALTER TABLE sub_stations ADD COLUMN route_n TEXT;
                        UPDATE sub_stations _stations SET route_n = f.concat
                        FROM (
                                SELECT
                                    concat(route_1::text,route_2::text,route_3::text,
                                    route_4::text,route_5::text,route_6::text,
                                    route_7::text,route_8::text,route_9::text,
                                    route_10::text,route_11::text)::text,
                                    uid
                                FROM sub_stations
                        ) f
                        where f.uid=_stations.uid;


                        --don't know how to do the below in pgSQL, and not worth figuring out for such a small use

                        DO LANGUAGE plpythonu
                        $BODY$

                            qry = 'select uid,route_n from sub_stations'
                            res = plpy.execute(qry)
                            assert len(res)>0
                            patch = ''
                            for it in res:
                                _sorted_rt = ''.join(sorted(it['route_n']))
                                if not it['route_n'] == _sorted_rt:
                                    patch+="UPDATE sub_stations SET route_n='%s' WHERE uid=%s;" % (_sorted_rt,it['uid'])
                                    print patch
                                    plpy.log(patch)

                            if patch:
                                plpy.execute(patch)

                        $BODY$;
                        """
                    self.T.to_sql(q)
                def update_tkm_with_stops_via_stations(add_col=False,with_regexp_matching=True):
                    t='' if not add_col else 'ALTER TABLE ts_key_min ADD COLUMN sub_stop_idx integer;'
                    q="""
                        %s
                        UPDATE ts_key_min tsm SET sub_stop_idx = _st.sub_stop_idx
                        FROM
                            sub_stations _st
                        WHERE concat(_st.division,upper(_st.station_name),_st.route_n) =
                        concat(tsm.division,tsm.station,tsm.line_name)
                        and _st.sub_stop_idx is not null;
                    """ % t
                    if with_regexp_matching:
                        q+= """
                            -- regex for ts_key station name only
                            UPDATE ts_key_min tsm
                            SET sub_stop_idx = _st.sub_stop_idx
                            FROM sub_stations _st
                            WHERE concat(_st.division,_st.route_n,upper(_st.station_name)) =
                            concat(tsm.division,tsm.line_name,regexp_replace(tsm.station,'^([^-]*)-(.*)$','\\1','g'))
                            AND tsm.sub_stop_idx IS NULL;

                            -- regex for both ts_key_min.station and sub_stations.station_name only
                            UPDATE ts_key_min tsm
                            SET sub_stop_idx = _st.sub_stop_idx
                            FROM sub_stations _st
                            WHERE concat(_st.division,_st.route_n,upper(regexp_replace(_st.station_name,'^([^-]*)-(.*)$','\\1','g'))) =
                            concat(tsm.division,tsm.line_name,regexp_replace(tsm.station,'^([^-]*)-(.*)$','\\1','g'))
                            AND tsm.sub_stop_idx IS NULL;
                            """
                    self.T.to_sql(q)
                def update_stations_via_tkm_matching(add_col=False):
                    t = '' if not add_col else 'ALTER TABLE sub_stations ADD COLUMN ts_key_min_idx integer;'
                    q="""
                        %s
                        UPDATE sub_stations _st SET ts_key_min_idx = tsm.uid
                        FROM ts_key_min tsm
                        WHERE concat(_st.division,upper(_st.station_name),_st.route_n) =
                        concat(tsm.division,tsm.station,tsm.line_name);

                        """ % t
                    self.T.to_sql(q)

                def do_string_replacements(tbl,col):
                    t = {'tbl':tbl,'col':col}
                    patch = ''
                    repl_dict                   =   {   r'(1st|first)'                          :   r'1',
                                                        r'(2nd|second)'                         :   r'2',
                                                        r'(3rd|third)'                          :   r'3',
                                                        r'(4th|fourth)'                         :   r'4',
                                                        r'(5th|fifth)'                          :   r'5',
                                                        r'(6th|sixth)'                          :   r'6',
                                                        r'(7th|seventh)'                        :   r'7',
                                                        r'(8th|eigth)'                          :   r'8',
                                                        r'(9th|nineth|ninth)'                   :   r'9',
                                                        r'(0th)'                                :   r'0',
                                                        r'(1th)'                                :   r'1',
                                                        r'(2th)'                                :   r'2',
                                                        r'(3th)'                                :   r'3',
                                                        r'\s(street)'                           :   r' st',
                                                        r'\s(square)'                           :   r' sq',
                                                        r'\s(center)'                           :   r' ctr',
                                                        r'\s(av)'                               :   r' ave',
                                                        r'\Aunion sq'                           :   r'14 st-union sq',
                                                        r'cathedral parkway-110 st'             :   r'110 st-cathedrl',
                                                        r'163 st - amsterdam ave'               :   r'163 st-amsterdm',
                                                        r'81 st - museum of natural history'    :   r'81 st-museum',
                                                        r'47-50 sts rockefeller ctr'            :   r'47-50 st-rock',
                                                        r'137 st-city college'                  :   r'137 st-city col',
                                                        r'broadway-lafayette st'                :   r'broadway/lafay',
                                                        r'west 4 st'                            :   r'w 4 st-wash sq' ,
                                                        r'110 st-central park north'            :   r'110 st-cpn',
                                                        r'116 st-columbia university'           :   r'116 st-columbia',
                                                        r'168 st - washington heights'          :   r'168 st-broadway',
                                                        r'49 st'                                :   r'49 st-7 ave',
                                                        r'168 st\Z'                             :   r'168 st-broadway',
                                                        r'59 st-columbus circle'                :   r'59 st-columbus',
                                                        r'66 st-lincoln ctr'                    :   r'66 st-lincoln',
                                                        r'68 st-hunter college'                 :   r'68st-hunter col',
                                                        # r'astor pl'                             :   'astor place',
                                                        r'brooklyn bridge-city hall'            :   r'brooklyn bridge',
                                                        r'dyckman st-200 st'                    :   r'dyckman-200 st',
                                                        r'grand central-42 st'                  :   r'42 st-grd cntrl',
                                                        r'inwood - 207 st'                      :   r'inwood-207 st',
                                                        r'lexington av-53 st'                   :   r'lexington-53 st',
                                                        r'prince st'                            :   r"prince st-b''way",
                                                        r'\Atimes sq\Z'                         :   r'42 st-times sq',
                                                        r'times sq-42 st'                       :   r'42 st-times sq',
                                                        r'van cortlandt park-242 st'            :   r'242 st',
                                                        r'marble hill-225 st'                   :   r'225 st',
                                                        r'lexington ave-53 st'                  :   r'lexington-53 st',
                                                        r'harlem-148 st'                        :   r'148 st-lenox',
                                                        r'\Agrand central\Z'                    :   r'42 st-grd cntrl',
                                                        r'\Acanal st (ul)\Z'                    :   r'canal st',}
                    for k,v in repl_dict.iteritems():
                        t.update({'k':k,'v':v})
                        patch+="UPDATE %(tbl)s SET %(col)s = regexp_replace(%(col)s,E'%(k)s',E'%(v)s','g'); " % t
                    self.T.to_sql(patch)
                def do_string_matching(idx_to_string,list_of_strings,tbl,list_of_cols):
                    """
                        select z_get_string_dist(
                            idx             integer[],
                            string_set      text[],
                            compare_tbl     text,
                            compare_col     text[],
                            jaro            boolean default true,
                            leven           boolean default true,
                            nysiis          boolean default true,
                            rating_codex    boolean default true,
                            usps_repl_first boolean default true
                            )
                    """
                    if not self.F.functions_exists('z_get_string_dist'):
                        self.F.functions_create_z_get_string_dist()
                    q="""
                        SELECT f2.* --jaro_b matched_stations
                        FROM
                                (
                                select (z).* from
                                    (
                                    select z_get_string_dist(       array%s,
                                                                    array%s,
                                                                    '%s'::text,
                                                                    array%s ) z
                                ) f1
                            ) f2
                        WHERE   f2.jaro > 0.66
                        """ % (idx_to_string,
                               [str(it) for it in list_of_strings],
                               tbl,
                               list_of_cols)

                    return self.T.pd.read_sql(q,self.T.eng) #['matched_stations']


                # i_trace()

                # I. ADD GEOMETRIES TO DB
                print 'adding turn stile geoms to DB'
                # Add subway entrances/exits to map
                add_station_geoms()

                # Add subway stops to map
                add_subway_stops()

                # Add turn stile key/legend to DB
                add_turnstile_key_info()

                # II. ADD DATA TO DB

                # Add turn stile data to DB
                # --- Get Data Fields:
                # cols                        =   get_data_fields_pre_change()
                cols                        =   get_data_fields_post_change()

                # --- Get Latest Data:
                data_link                   =   get_data_links(link_type='latest')
                datetime_cols               =   [ i for i in range(0,len(cols)) if cols[i].find('date')==0 or cols[i].find('time')==0 ]
                # print 'about to start downloading and loading turn stile data.  This might take some time.'
                # turn_stile_data             =   self.T.pd.read_csv(data_link,names=cols,skiprows=1,parse_dates=datetime_cols)
                # print 'turn stile data downloaded.  %s rows about to be loaded into pgSQL.' % len(turn_stile_data)
                # turn_stile_data.to_sql(         'turn_stiles',self.T.eng,index=False)
                # print 'turn stile data loaded.  now harmonizing data inconsistencies.'

                # REVIEW LATER
                # Next 2 lines were used in pre_format_change analysis, not sure of the purpose
                # idx                         =   p[p.unit.str.contains('R')==False].index
                # p                           =   p.drop(idx,axis=0).reset_index(drop=True)

                # Next group of lines also used in pre_format_change analysis, not sure of the purpose
                # dropCols                    =   []
                # for it in cols:
                #     if it.find('exit')==0 or it.find('entries')==0:
                #         p[it]               =   p[it].map(float)
                #     if it.find('date')==0:
                #         date_pt,time_pt,datetime_pt = it,'time'+it[4:],'datetime'+it[4:]
                #         p[datetime_pt]      =   self.T.pd.to_datetime(p[date_pt] + ' ' + p[time_pt],format='%m-%d-%y %H:%M:%S',coerce=False)
                #         p[datetime_pt]      =   p[datetime_pt].map(lambda s: None if str(s)=='NaT' else str(s))
                #         dropCols.extend(        [date_pt,time_pt])
                # p                           =   p.drop(dropCols,axis=1)
                # cols                        =   p.columns.tolist()



                # III. HARMONIZE DATA

                # 1. Reduce `ts_key` to minimum --> `ts_key_min`;  Index `ts_key_min` on `ts_key`
                create_ts_key_min()

                '''
                Because number(stations) >> number(stops),
                stations are first matched up on "sub_stops".
                STEP ONE:       fill sub_stops.sub_station_idx
                STEP TWO:       fill sub_station.sub_stop_idx
                STEP THREE:     fill ts_key_min

                '''
                do_string_replacements('ts_key_min','station')
                do_string_replacements('sub_stations','station_name')
                do_string_replacements('sub_stations','line')
                do_string_replacements('sub_stops','stop_name')

                #
                update_stops_via_overlapping_station_geoms(add_col=True)
                update_stations_via_stops_idx_of_stations(add_col=True)
                update_stations_via_stations_with_stops_idx()
                update_stations_via_stations_overlapping_stops()
                update_stations_via_station_names_matching_stop_names()
                update_stops_via_overlapping_station_geoms()
                update_stations_via_stops_idx_of_stations()
                update_stations_via_stations_with_stops_idx()
                add_route_n_to_stations()
                update_tkm_with_stops_via_stations(add_col=True)
                update_stations_via_tkm_matching(add_col=True)



                '''
                RE: the two known errors below,
                    ts_key_min value will replace sub_stations value,
                        which will replace sub_stops value.
                Then step #3 will be run again.

                '''

                def FIX_FOR_KNOWN_ERRORS():
                    q="""

                    UPDATE sub_stations _st SET station_name = '145 St'
                    WHERE _st.station_name ~* '145';


                    UPDATE sub_stations _st SET station_name = f1.station
                    FROM
                        (select station from ts_key_min tsm where tsm.station ~* 'w 4') f1
                    WHERE _st.station_name ~* 'West 4';

                    UPDATE sub_stops _st SET stop_name = f1.station
                    FROM
                        (select station from ts_key_min tsm where tsm.station ~* 'w 4') f1
                    WHERE _st.stop_name ~* 'w 4';

                    """
                    self.T.to_sql(q)

                FIX_FOR_KNOWN_ERRORS()



                def RULE_1(station_name):
                    '''
                    RULE #1 -- (and RE: South Ferry)

                    If single value exists for concat(division,line,station_name) in sub_stations,
                        (which implies single value in sub_stops):
                            -add sub_station_idx to, and copy sub_station coordinates onto, sub_stops
                            -add sub_stop_idx to ts_key_min
                            -add sub_stop_idx,ts_key_min_idx to sub_stations

                    '''

                    q="""
                        select count(
                            distinct concat(division,line,station_name)
                            )=1 _check
                        from sub_stations
                        where station_name = '%s'
                        """ % station_name
                    assert self.T.pd.read_sql(q,self.T.eng)['_check'][0]==True
                    q="""
                        WITH upd as (
                            UPDATE sub_stops _stops
                            SET stop_lat = _stations.latitude,
                                stop_lon = _stations.longitude,
                                sub_station_idx = _stations.uid
                            FROM sub_stations _stations
                            WHERE _stops.stop_name ~* '%(X)s'
                            and _stations.station_name ~* '%(X)s'
                            RETURNING _stops.uid
                        ),
                        upd2 as (
                            UPDATE ts_key_min tsm
                            SET sub_stop_idx = u.uid
                            FROM upd u
                            WHERE station ~* '%(X)s'
                            RETURNING tsm.uid
                        )
                        UPDATE sub_stations
                        SET sub_stop_idx = u1.uid,
                            ts_key_min_idx = u2.uid
                        FROM upd u1,upd2 u2
                        WHERE station_name ~* '%(X)s';
                        """ % {'X':station_name}
                    self.T.to_sql(q)

                station_name = 'South Ferry'
                RULE_1(station_name)



                i_trace()

                do_string_replacements('sub_stations','line')


                # create table str_matching as (
                # 0 (select uid ts_uid from ts_key_min where sub_stop_idx is null)
                # 1 (select concat(division,line_name,station) from ts_key_min,(select ts_uid from str_matching)
                # 1 (select concat(division,route_n,station_name) from sub_stations)
                # 2 (select concat(division,route_n,line,station_name) from sub_stations)
                # 3 (select concat(division,route_n,station_name,line) from sub_stations)
                # 4 (select concat(division,route_n,_stops.stop_name) from sub_stations,
                #   (   select uid,_stop_name
                #       from sub_stops,(select array_agg(sub_stop_idx) all_sub_stop_idxs from sub_stations)
                #       where array[uid] && all_sub_stop_idxs

                # compare 0:1,0:2,0:3,0:4 with do_string_matching


                do_string_matching()

                # END GOAL conform 'station_names' to 'mn_stations'
                mn_stations                 =   self.T.pd.read_sql("select * from sub_stat_entr", self.T.eng)

                # Limit NYC key to MN
                mn_div_list                 =   mn_stations.division.unique().tolist()
                ts_key                      =   ts_key.drop(ts_key[ts_key.division.isin(mn_div_list)==False].index,axis=0)

                # Create and Sort Route List for each station
                mn_cols                     =   mn_stations.columns.tolist()
                s_pt,e_pt                   =   mn_cols.index('route_1'),mn_cols.index('route_11')+1
                mn_stations['all_lines']    =   mn_stations.ix[:,s_pt:e_pt].apply(lambda s: ''.join([str(it) for it in s if str(it)!='NaN']).replace('nan','').replace('.0',''),axis=1)
                mn_stations['all_lines']    =   mn_stations.all_lines.map(lambda s: ''.join(sorted(s)))
                # ts_key['line_name']       =   ts_key['line_name'].map(lambda s: ''.join(sorted([str(it) for it in s])) if type(s)!=int else str(s))

                # clean up many but small differences between station names
                mn_stations['clean']        =   mn_stations.station_name.map(lambda s: s.lower())
                # ts_key['clean']           =   ts_key.station.map(lambda s: s.lower())
                do_string_replacements()

                station_names               =   ts_key.clean.tolist()
                #print len(mn_stations[mn_stations.clean.isin(station_names)==False]), 'stations not mapped'
                # mn_stations[mn_stations.clean.isin(station_names)==False].ix[:,['division','line','station_name','all_lines','clean']].sort('clean')
                # mn_stations.head()

                # push to [ sub_stat_entr,ts_key,p(turn stiles) ] to pgsql for comparison and unificiation
                self.T.to_sql(                  ';'.join(['drop table if exists sub_stat_entr cascade',
                                                          'drop table if exists ts_key',
                                                          'drop table if exists sub_turn_stiles']) )
                self.T.delay(                   1)

                mn_stations.to_sql(             'sub_stat_entr',self.T.eng)
                self.T.to_sql(                  "UPDATE sub_stat_entr set geom = ST_SetSRID(ST_MakePoint(station_longitude,station_latitude),4326)")

                ts_key.to_sql(                  'ts_key',self.T.eng)
                self.T.to_sql(                  """
                                                alter table ts_key add column lat double precision;
                                                alter table ts_key add column lon double precision;
                                                """)

                p.to_sql(                       'sub_turn_stiles',self.T.eng)
                self.T.to_sql(                  """
                                                alter table sub_turn_stiles add column station text;
                                                alter table sub_turn_stiles add column lat double precision;
                                                alter table sub_turn_stiles add column lon double precision;
                                                """)


                # 1. Copy coords from station_entrances to turnstile_key
                self.T.to_sql(                  """
                                                UPDATE ts_key t
                                                SET lat = s.station_latitude,lon = s.station_longitude
                                                FROM sub_stat_entr s
                                                WHERE s.clean=t.clean
                                                AND s.all_lines=t.line_name;
                                                """)
                #print pd.read_sql('select count(*) c from ts_key where lon is null',engine).c[0],'null'
                #print pd.read_sql('select count(*) c from ts_key where lon is not null',engine).c[0],'not null'

                # 2. Copy matching ts_key table data to turn_stiles table
                self.T.to_sql(                  """
                                                UPDATE sub_turn_stiles
                                                SET lat = t.lat,lon=t.lon,station=t.station
                                                FROM ts_key t
                                                WHERE t.remote = unit
                                                AND t.booth = c_a;
                                                """)
                self.T.to_sql(                  'alter table sub_turn_stiles add column geom geometry(Point,4326)')
                self.T.to_sql(                  "UPDATE sub_turn_stiles set geom = ST_SetSRID(ST_MakePoint(lon,lat),4326)")

                # 3. Attempt to match leftovers (non-matching) b/t ts_key/sub_stat_entr using wildcards
                leftovers                   =   self.T.pd.read_sql("select * from ts_key where lon is null and division = any(array['IRT','IND','BMT'])",self.T.eng)
                for i in range(0,len(leftovers)):
                    row                     =   leftovers.ix[i,:]
                    chk                     =   row['clean'].find('-')
                    if chk != -1:
                        a,b                 =   '%%'+str(row['clean'].split('-')[0])+'%%',row['line_name']
                        tmp                 =   self.T.pd.read_sql(  """
                                                    SELECT station_latitude lat,station_longitude lon
                                                    FROM sub_stat_entr s
                                                    WHERE s.clean ilike '%s'
                                                    AND s.all_lines='%s';
                                                    """%(a,b),self.T.eng)
                        if (len(tmp.lat.unique())==len(tmp.lat.unique())==1):
                            a,b,c           =   tmp.lat[0],tmp.lon[0],row['id']
                            self.T.to_sql(      """
                                                UPDATE ts_key set lat=%f,lon=%f
                                                WHERE uid = %d
                                                """%(a,b,c),self.T.eng)

                # 4. Attempt to match leftover by:
                        # starting with lines,division,
                            # if one result, make match;
                            # else if only one result and it's a partial match, match it?
                leftovers                   =   self.T.pd.read_sql("select * from ts_key where lon is null and division = any(array['IRT','IND','BMT'])",self.T.eng)
                for i in range(0,len(leftovers)):
                    row                     =   leftovers.ix[i,:]
                    a,b,c                   =   '%%'+str(row['clean'].split('-')[0])+'%%',row['line_name'],row['division']
                    tmp                     =   pd.read_sql(  """
                                                    SELECT station_latitude lat,station_longitude lon,clean
                                                    FROM sub_stat_entr s
                                                    WHERE s.division='%s'
                                                    AND s.all_lines='%s';
                                                    """%(c,b),self.T.eng)
                    if (len(tmp.lat.unique())==len(tmp.lat.unique())==1):
                        a,b,c               =   tmp.lat[0],tmp.lon[0],row['uid']
                        self.T.to_sql(          """
                                                    UPDATE ts_key
                                                    SET lat=%f,lon=%f
                                                    WHERE uid = %d
                                                """%(a,b,c))
                    else:
                        z                   =   tmp[tmp.clean.str.contains(a)]
                        if (len(z.lat.unique())==len(z.lat.unique())==1):
                            a,b,c           =   tmp.lat[0],tmp.lon[0],row['uid']
                            self.T.to_sql(      """
                                                    UPDATE ts_key
                                                    SET lat=%f,lon=%f
                                                    WHERE uid = %d
                                                """%(a,b,c))

                # TURN STILE CONT'D convert text to timestamp

                self.T.to_sql("""

                    DROP TABLE if exists tmp;

                    CREATE TABLE tmp (
                        gid serial primary key,
                        datetime1 timestamp with time zone,
                        datetime2 timestamp with time zone,
                        datetime3 timestamp with time zone,
                        datetime4 timestamp with time zone,
                        datetime5 timestamp with time zone,
                        datetime6 timestamp with time zone,
                        datetime7 timestamp with time zone,
                        datetime8 timestamp with time zone
                        );

                    UPDATE tmp SET gid = nextval(pg_get_serial_sequence('tmp','gid'));

                    INSERT INTO tmp (
                        datetime1,
                        datetime2,
                        datetime3,
                        datetime4,
                        datetime5,
                        datetime6,
                        datetime7,
                        datetime8)
                    SELECT
                        to_timestamp(s.datetime1,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime2,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime3,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime4,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime5,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime6,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime7,'YYYY-MM-DD HH24:MI:SS'),
                        to_timestamp(s.datetime8,'YYYY-MM-DD HH24:MI:SS')
                    FROM sub_turn_stiles s;

                    ALTER TABLE sub_turn_stiles
                    DROP COLUMN datetime1,
                    DROP COLUMN datetime2,
                    DROP COLUMN datetime3,
                    DROP COLUMN datetime4,
                    DROP COLUMN datetime5,
                    DROP COLUMN datetime6,
                    DROP COLUMN datetime7,
                    DROP COLUMN datetime8,
                    ADD COLUMN datetime1 timestamp with time zone,
                    ADD COLUMN datetime2 timestamp with time zone,
                    ADD COLUMN datetime3 timestamp with time zone,
                    ADD COLUMN datetime4 timestamp with time zone,
                    ADD COLUMN datetime5 timestamp with time zone,
                    ADD COLUMN datetime6 timestamp with time zone,
                    ADD COLUMN datetime7 timestamp with time zone,
                    ADD COLUMN datetime8 timestamp with time zone;

                    UPDATE sub_turn_stiles s
                    SET
                        datetime1 = t.datetime1,
                        datetime2 = t.datetime2,
                        datetime3 = t.datetime3,
                        datetime4 = t.datetime4,
                        datetime5 = t.datetime5,
                        datetime6 = t.datetime6,
                        datetime7 = t.datetime7,
                        datetime8 = t.datetime8
                    FROM tmp t
                    where t.gid = s.uid;

                               """)

                # TURN STILE CONT'D calc. register differences
                self.T.to_sql("""
                    alter table sub_turn_stiles
                    add column out1 double precision,
                    add column out2 double precision,
                    add column out3 double precision,
                    add column out4 double precision,
                    add column out5 double precision,
                    add column out6 double precision,
                    add column out7 double precision,
                    add column in1 double precision,
                    add column in2 double precision,
                    add column in3 double precision,
                    add column in4 double precision,
                    add column in5 double precision,
                    add column in6 double precision,
                    add column in7 double precision;


                    update sub_turn_stiles
                    set out1 = exits2-exits1
                    where exits1 != 'NaN'::float
                    and exits2 != 'NaN'::float;

                    update sub_turn_stiles
                    set out2 = exits3-exits2
                    where exits2 != 'NaN'::float
                    and exits3 != 'NaN'::float;

                    update sub_turn_stiles
                    set out3 = exits4-exits3
                    where exits3 != 'NaN'::float
                    and exits4 != 'NaN'::float;

                    update sub_turn_stiles
                    set out4 = exits5-exits4
                    where exits4 != 'NaN'::float
                    and exits5 != 'NaN'::float;

                    update sub_turn_stiles
                    set out5 = exits6-exits5
                    where exits5 != 'NaN'::float
                    and exits6 != 'NaN'::float;

                    update sub_turn_stiles
                    set out6 = exits7-exits6
                    where exits6 != 'NaN'::float
                    and exits7 != 'NaN'::float;

                    update sub_turn_stiles
                    set out7 = exits8-exits7
                    where exits7 != 'NaN'::float
                    and exits8 != 'NaN'::float;


                    update sub_turn_stiles
                    set in1 = entries2-entries1
                    where entries1 != 'NaN'::float
                    and entries2 != 'NaN'::float;

                    update sub_turn_stiles
                    set in2 = entries3-entries2
                    where entries2 != 'NaN'::float
                    and entries3 != 'NaN'::float;

                    update sub_turn_stiles
                    set in3 = entries4-entries3
                    where entries3 != 'NaN'::float
                    and entries4 != 'NaN'::float;

                    update sub_turn_stiles
                    set in4 = entries5-entries4
                    where entries4 != 'NaN'::float
                    and entries5 != 'NaN'::float;

                    update sub_turn_stiles
                    set in5 = entries6-entries5
                    where entries5 != 'NaN'::float
                    and entries6 != 'NaN'::float;

                    update sub_turn_stiles
                    set in6 = entries7-entries6
                    where entries6 != 'NaN'::float
                    and entries7 != 'NaN'::float;

                    update sub_turn_stiles
                    set in7 = entries8-entries7
                    where entries7 != 'NaN'::float
                    and entries8 != 'NaN'::float;


                    alter table sub_turn_stiles
                        add column in_all double precision,
                        add column out_all double precision;
                    update sub_turn_stiles
                    set in_all = (select sum(s) from unnest(array[in1,in2,in3,in4,in5,in6,in7]) s);
                    update sub_turn_stiles
                    set out_all = (select sum(s) from unnest(array[out1,out2,out3,out4,out5,out6,out7]) s);
                """)

                # NEED to consolidate turn_stile data

                # turn_stile data with missing station name -- SMALL ADJUSTMENTS NEEDED
                cmd                         =   """ select distinct unit,c_a from sub_turn_stiles
                                                    where station is null order by unit;"""
                self.T.pd.read_sql(             cmd,self.T.eng)

                # turn_stile data analysis:

                # general data used herein
                datetime_cols               =   ['datetime'+str(i) for i in range(1,9)]
                desc_cols                   =   ['desc'+str(i) for i in range(1,9)]
                entry_cols                  =   ['entries'+str(i) for i in range(1,9)]
                exit_cols                   =   ['exits'+str(i) for i in range(1,9)]
                in_cols                     =   ['in'+str(i) for i in range(1,9)]
                out_cols                    =   ['out'+str(i) for i in range(1,9)]
                agg_cols                    =   ['in_all','out_all']
                other_cols                  =   ['index','lat','lon','uid','geom']

                # 1. this shows there are multiple entries per station, i.e., multiple turn stiles
                cmd                         =   """
                                                select * from sub_turn_stiles
                                                where out_all is not null
                                                and station = '34 ST-HERALD SQ'
                                                AND extract(hour from datetime1) = 4
                                                AND extract(day from datetime1) = 29
                                                order by out_all desc;
                                                """

                # 2. this shows there are multiple rows even when {c_a,unit,scp,datetime1} are same
                cmd                         =   """
                                                select * from sub_turn_stiles
                                                where out_all is not null
                                                and station = '34 ST-HERALD SQ'
                                                AND scp = '00-00-00'
                                                order by c_a,unit,datetime1 asc;
                                                """

                # 3. this shows the door is open for people leaving during rush hour... (see datetime6-7)
                #        and possibly explains why multiple rows
                cmd                         =   """
                                                select * from sub_turn_stiles
                                                where out_all is not null
                                                and station = '34 ST-HERALD SQ'
                                                AND scp = '00-00-00'
                                                AND datetime1 = '2014-09-28 09:00:00-04:00';
                                                """

                # dropCols                  =   desc_cols + entry_cols + exit_cols + in_cols + out_cols + other_cols
                self.T.pd.read_sql(             cmd,self.T.eng)#.drop(dropCols,axis=1)

class pgSQL_Types:

    def __init__(self,_parent):
        self                                =   _parent.T.To_Sub_Classes(self,_parent)

    def exists(self,type_name):
        qry                                 =   """ SELECT EXISTS (SELECT 1
                                                    FROM pg_type
                                                    WHERE typname = '%s')
                                                """ % type_name
        return                                  self.T.pd.read_sql(qry,self.T.eng).exists[0]

    class Create:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)

        def string_dist_results(self):
            qry="""
                DROP TYPE IF EXISTS string_dist_results cascade;
                CREATE TYPE string_dist_results as (
                    idx integer,
                    orig_str text,
                    jaro double precision,
                    jaro_b text,
                    leven integer,
                    leven_b text,
                    nysiis text,
                    rating_codex text
                );
            """
            self.T.to_sql(                  qry)

    class Destroy:

        def __init__(self,_parent):
            self                            =   _parent.T.To_Sub_Classes(self,_parent)
        def string_dist_results(self):
            qry="""DROP TYPE IF EXISTS string_dist_results cascade;
            """
            self.T.to_sql(                  qry)

class pgSQL:

    def __init__(self):
        def download_file(url,save_path):
            import os
            _dir = save_path[:save_path.rfind('/')]
            if not os.path.exists(_dir):
                os.makedirs(_dir)

            with open(save_path, 'wb') as handle:
                response = self.T.requests.get( url, stream=True)

                if not response.ok:
                    # Something went wrong
                    print 'error'

                for block in response.iter_content(1024):
                    if not block:
                        break

                    handle.write(block)
                    handle.flush()
            return True

        def read_json_from_url_response(url):
            r = self.T.requests.get(url)
            assert r.status_code=='200'
            # print r.text
            g = r.text
            g = g.replace('true',"'true'")
            a = eval(g)
            return a

        def to_sql(cmd):
            self.T.conn.set_isolation_level(    0)
            self.T.cur.execute(                 cmd)

        def redirect_logs_to_file(file_desc='/dev/pts/0',msg_form="%(asctime)s - %(levelname)s - %(message)s"):
            # print T.logger.__dict__
            # print T.logger.manager.__dict__

            # for it in dir(logger):
            #     print it,getattr(logger,it)

            for it in self.T.logger.handlers:
                self.T.logger.removeHandler(it)

            for it in self.T.logger.parent.handlers:
                self.T.logger.parent.removeHandler(it)

            for it in self.T.logger.root.handlers:
                self.T.logger.root.removeHandler(it)

            # print logger.manager.__dict__
            del_these                       =   ['IPKernelApp','basic_logger']
            for it in del_these:
                if self.T.logger.manager.__dict__['loggerDict'].has_key(it):
                    del self.T.logger.manager.__dict__['loggerDict'][it]

            for k in self.T.logger.manager.__dict__['loggerDict'].keys():
                if k.count('sqlalchemy') or k.count('pandas'):
                    del self.T.logger.manager.__dict__['loggerDict'][k]

            self.T.logging.basicConfig(filename=file_desc, level=self.T.logging.DEBUG, format=msg_form)
            return

        import                                  datetime                as dt
        from dateutil                           import parser           as DU               # e.g., DU.parse('some date as str') --> obj(datetime.datetime)
        from time                               import sleep
        from urllib                             import quote_plus,unquote
        from re                                 import findall          as re_findall
        from re                                 import sub              as re_sub           # re_sub('patt','repl','str','cnt')
        from re                                 import search           as re_search        # re_search('patt','str')
        from subprocess                         import Popen            as sub_popen
        from subprocess                         import PIPE             as sub_PIPE
        from traceback                          import format_exc       as tb_format_exc
        from sys                                import exc_info         as sys_exc_info
        from types                              import NoneType
        from time                               import sleep            as delay
        from uuid                               import uuid4            as get_guid
        import                                  requests
        from f_geolocation                      import Addr_Parsing,Geocoding
        from db_settings                        import DB_NAME,DB_HOST,DB_PORT,DB_USER,DB_PW
        from db_settings                        import PLUTO_TBL
        from py_classes_link                    import To_Sub_Classes,To_Class,To_Class_Dict
        import                                  pandas                  as pd
        pd.set_option(                          'expand_frame_repr', False)
        pd.set_option(                          'display.max_columns', None)
        pd.set_option(                          'display.max_colwidth', 250)
        pd.set_option(                          'display.max_rows', 1000)
        pd.set_option(                          'display.width', 1500)
        pd.set_option(                          'display.colheader_justify','left')
        np                                  =   pd.np
        np.set_printoptions(                    linewidth=1500,threshold=np.nan)
        import                                  geopandas               as gd
        from sqlalchemy                         import create_engine
        import logging
        logger = logging.getLogger(                      'sqlalchemy.dialects.postgresql')
        logger.setLevel(logging.INFO)

        eng                                 =   create_engine(r'postgresql://%s:%s@%s:%s/%s'
                                                          %(DB_USER,DB_PW,DB_HOST,DB_PORT,DB_NAME),
                                                          encoding='utf-8',
                                                          echo=False)
        from psycopg2                           import connect          as pg_connect
        conn                                =   pg_connect("dbname='%s' " % DB_NAME +
                                                           "user='%s' " % DB_USER +
                                                           "host='%s' password='%s' port=%s"
                                                           % (DB_HOST,DB_PW,DB_PORT));
        cur                                 =   conn.cursor()

        D                                   =   {'guid'                 :   str(get_guid().hex)[:7]}
        D.update(                               {'tmp_tbl'              :   'tmp_'+D['guid']})

        self.T                              =   To_Class_Dict(  self,
                                                                dict_list=[D,locals()],
                                                                update_globals=True)

        self.Functions                      =   pgSQL_Functions(self)
        self.Triggers                       =   pgSQL_Triggers(self)
        self.Tables                         =   pgSQL_Tables(self)
        self.Types                          =   pgSQL_Types(self)
        self.__initial_check__(                 )
        self.__temp_options__(                  )

    def __initial_check__(self):
        # at minimum, confirm that geometry is enabled
        try:
            self.T.to_sql("""   CREATE EXTENSION IF NOT EXISTS plpythonu;
                                CREATE EXTENSION IF NOT EXISTS pllua;
                                --CREATE EXTENSION IF NOT EXISTS plpgsql;
                                CREATE EXTENSION IF NOT EXISTS postgis;""")
        except:
            self.F.functions_run_confirm_extensions()

    def __temp_options__(self):

        self.T.redirect_logs_to_file(                  '/tmp/tmplog')



##### --------------------------------------
###
#   The functions below are old and likely surplusage, but,
#   they could be used as parts of other functions of other functions.
#   Will review again at later date.  (2015.08.27)
###
##### --------------------------------------


def get_tables_headers(filePath=''):
    df_t = self.T.pd.read_sql_query("select * from information_schema.tables",engine)
    all_t = df_t[(df_t.table_schema=='public') & (df_t.table_catalog=='routing')].table_name.tolist()
    w,max_l = {},0
    print("All Routing Tables")
    for i in range(0,len(all_t)):
        t=all_t[i]
        print(str(i)+'\t\t'+t+'\n')
        df = self.T.pd.read_sql_query("SELECT * FROM "+t+" LIMIT 1", engine)
        x = df.columns
        w.update({t : x.values.tolist()})
        if len(x)>max_l: max_l=len(x)
        for it in x: print(it)
        print('\n\n')
        print('\n',df.head())
        print('\n\n')
    j=self.T.pd.DataFrame(dict([ (k,self.T.pd.Series(v)) for k,v in w.iteritems() ]))
    j.to_csv(filePath)
    return j


def add_points_to_remaining_lots(show_some_detail=True,show_steps=False):
    ### add_points_to_remaining_lots
#    show_some_detail=False
#    show_steps=False

    from IPython.display import FileLink, FileLinks
    save_fig_path = '/Users/admin/Projects/GIS/matplotlib_data/nyc_block_and_lot.png'
    matplot_files = FileLinks("/Users/admin/Projects/GIS/matplotlib_data/")
    # display(matplot_files)
    from math import degrees as rad_to_deg
    from math import radians as deg_to_rad
    from itertools import chain
    from matplotlib import pyplot as plt

    def geoms_to_collection(geoms):
        s='GEOMETRYCOLLECTION('
        for it in geoms:
            s+=it.to_wkt()+','
        return s.rstrip(',')+')'
    def geoms_to_text(geoms):
        if type(geoms_to_text) != list: geoms=list(geoms)
        s=''
        for it in geoms:
            try:
                s+="ST_GeomFromText('"+it.to_wkt()+"'),"
            except:
                s+="ST_GeomFromText('"+it+"'),"
        return s.rstrip(',')
    def geom_txts_to_collection(geom_txts):
        return "ST_Collect(ARRAY["+geoms_to_text(geom_txts)+'])'

    all_pts = self.T.pd.read_sql_query("select * from lot_pts where geom is null and ignore is false and place is false",engine)
    all_pts['block'] = all_pts.bbl.map(lambda s: str(s)[1:6])
    uniq_blocks = all_pts.block.unique().tolist()
    if show_some_detail==True: print len(uniq_blocks),'unique blocks'
    if show_some_detail==True: print 'pluto has 1961 unique blocks'
    # a=self.T.pd.read_sql_query("select distinct block from pluto",engine).block.tolist()
    # for it in a:
    #     if uniq_blocks.count(str('%05d'%it))==0:
    #         print it
    if show_some_detail==True: print 'lot 656 is a pier and was removed from lot_pts'

    #

    ### iter block for add_points_to_remaining_lots
    for block in chain(uniq_blocks):
        if show_some_detail==True: print 'block\t',block

        lot_pts,A,skip = [],[],False
        plt.clf()

        uniq_street = all_pts[all_pts.block==block].bldg_street.unique().tolist()
        if uniq_street.count('')!=0:    dev = uniq_street.pop(uniq_street.index(''))
        if uniq_street.count(None)!=0:  dev = uniq_street.pop(uniq_street.index(None))

        for street_name in chain(uniq_street):
            if show_some_detail==True: print 'street_name\t',street_name

            ### Get All Lots on Block and On This Street
            pts = all_pts[(all_pts.block==block)&(all_pts.bldg_street==street_name)].sort('bldg_num').reset_index(drop=True)
            cmd=""" SELECT _bbl bbl,geom
                    FROM pluto p,unnest(array%s) _bbl
                    WHERE p.bbl = _bbl"""%str(pts.bbl.astype(int).tolist()).replace("'",'')
            lots = self.T.gd.read_postgis(cmd,engine)
            #if show_steps==True: A.extend(lots.geom)

            ### Create Buffer Around Lots
            s = str([it.to_wkt() for it in lots.geom])
            T = {'buffer': '0.0005',
                 'geoms' : s}
            cmd =   """ SELECT ST_Buffer(ST_ConvexHull((ST_Collect(the_geom))), %(buffer)s) as geom
                        FROM ( SELECT (ST_Dump( unnest(array[%(geoms)s]) )).geom the_geom) as t""".replace('\n','')%T
            block_buffer = self.T.gd.read_postgis(cmd,engine)
            #if show_steps==True: A.extend(block_buffer.geom)

            ### Get street as Line from lion_ways
            cmd =   """ SELECT st_makeline(_geom) geom
                        FROM
                            st_geomfromtext(
                            ' %(block_buffer)s '
                            , 4326) block_buffer,
                            (select ( st_dump( geom )).geom _geom
                                FROM lion_ways l
                                WHERE l.clean_street = '%(1)s'
                                AND l.geom is not null  ) as t2
                        WHERE st_intersects(_geom,block_buffer) is True
                    """.replace('\n','')%{'1':str(street_name),
                                          'block_buffer':str(block_buffer.geom[0].to_wkt())}
            try:
                line_geom = self.T.gd.read_postgis(cmd,engine)
                skip_street=False
            except AttributeError:
                print 'skipping street:',street_name
                skip_street=True

            if skip_street==False:

                # A.extend(line_geom.geom)
                LINE = line_geom.geom[0].to_wkt()

                ### Keep only part of line intersecting with buffer
                s1 = str([it.to_wkt() for it in block_buffer.geom][0])
                s2 = str([it.to_wkt() for it in line_geom.geom][0])
                T = {'block_buffer': s1,
                     'street_line' : s2}
                cmd =   """ SELECT res_geom geom
                            FROM
                                st_geomfromtext('%(block_buffer)s') s1,
                                st_geomfromtext('%(street_line)s') s2,
                                st_intersection(s1,s2) res_geom
                        """.replace('\n','')%T
                tmp_part_line=self.T.gd.read_postgis(cmd,engine)
        #
                if show_steps==True:  A.extend(tmp_part_line.geom)


                for i in range(0,len(lots.geom)):
                    bbl,lot = lots.ix[i,['bbl','geom']].values

                    cmd =   """ SELECT ( st_dump( _geom )).geom,ST_NPoints((( st_dump( _geom )).geom))
                            FROM (
                                SELECT ( st_dump( st_geomfromtext('%(1)s') )).geom _geom
                                ) as t
                        """.replace('\n','')%{'1':str(lot.to_wkt())}

                    ### Get Single Lot Polygon
                    lot=self.T.gd.read_postgis(cmd,engine)
                    if show_some_detail==True: A.append(lot.geom[0])

                    ### Get Line From Lot Polygon
                    perim_line = lot.boundary[0].to_wkt()
                    # if show_steps==True: A.append(lot.boundary[0])

                    ### Get Points of Segment of Lot Polygon Closest to Street
                    cmd =   """ SELECT ( st_dumppoints( st_geomfromtext('%(1)s') )).geom""".replace('\n','')%{'1':str(perim_line)}
                    points = self.T.gd.read_postgis(cmd,engine).geom
                    t1,t2={},{}
                    for j in range(1,len(points)):
                        poly_seg_pts=points[j-1],points[j]
                        dist_from_street = self.T.pd.read_sql("""  select
                                                            st_distance(
                                                               st_geomfromtext(' %(start_pt)s '),
                                                               st_geomfromtext(' %(street)s '))
                                                               +
                                                            st_distance(
                                                               st_geomfromtext(' %(end_pt)s '),
                                                               st_geomfromtext(' %(street)s '))
                                                            dist
                                                        """.replace('\n','')%{'start_pt':str(poly_seg_pts[0]),
                                                                              'end_pt' : str(poly_seg_pts[1]),
                                                                              'street' : LINE},engine)
                        t1.update({j:dist_from_street.dist[0]})
                        t2.update({j:poly_seg_pts})
                    closest_seg_pts = t2[t1.values().index(min(t1.values()))+1]

                    ### Lot Segment MidPoint
                    lot_seg_mid_pt = self.T.gd.read_postgis("""   SELECT ST_Line_Interpolate_Point(st_makeline(ptA,ptB),0.5) geom
                                                            FROM
                                                                st_geomfromtext(' %(start_pt)s ') ptA,
                                                                st_geomfromtext(' %(end_pt)s ') ptB
                                                    """.replace('\n','')%{'start_pt': str(closest_seg_pts[0]),
                                                                          'end_pt'  : str(closest_seg_pts[1]),
                                                                          'street'  : LINE},engine)

                    if show_steps==True: A.append(lot_seg_mid_pt.geom[0])

                    ### Closest Point in Street
                    street_seg_mid_pt = self.T.gd.read_postgis("""
                        SELECT ST_ClosestPoint(
                            st_geomfromtext(' %(street)s '),
                            st_geomfromtext(' %(mid_pt)s ')) geom
                        """.replace('\n','')%{'mid_pt'  : str(lot_seg_mid_pt.geom[0].to_wkt()),
                                              'street'  : LINE},engine)
                    #A.append(street_seg_mid_pt.geom[0])

                    ### Absolute Angle of Segment (12=0 deg.,6=180 deg.)
                    seg_angle = self.T.pd.read_sql("""     SELECT ST_Azimuth(ptA,ptB) ang
                                                    FROM
                                                                st_geomfromtext(' %(start_pt)s ') ptA,
                                                                st_geomfromtext(' %(end_pt)s ') ptB
                                                    """.replace('\n','')%{'start_pt': str(closest_seg_pts[0]),
                                                                          'end_pt'  : str(closest_seg_pts[1])},engine).ang[0]

                    ### Point on Street that Intersects with perp. line extending from Poly Segment Midpoint
                    this_lot_pt = self.T.gd.read_postgis("""
                        SELECT ( st_dumppoints( st_intersection( street, st_makeline(
                            st_makeline(mid_pt::geometry(Point,4326),ptA::geometry(Point,4326)),
                            st_makeline(mid_pt::geometry(Point,4326),ptB::geometry(Point,4326))))  )).geom
                        FROM
                            st_geomfromtext(' %(mid_pt)s ', 4326) mid_pt,
                            st_geomfromtext(' %(street)s ', 4326) street,
                            st_geomfromtext(' %(line_seg_pt)s ', 4326) line_seg_pt,
                            ST_Distance_Spheroid(mid_pt,line_seg_pt,
                                'SPHEROID["WGS 84",6378137,298.257223563]') dist,
                            ST_Project(mid_pt,dist+(dist*1.1),%(ang1)s) ptA,
                            ST_Project(mid_pt,dist+(dist*1.1),%(ang2)s) ptB
                                                    """.replace('\n','')%{'mid_pt' : str(lot_seg_mid_pt.geom[0].to_wkt()),
                                                                          'line_seg_pt' : str(street_seg_mid_pt.geom[0].to_wkt()),
                                                                          'street'  : LINE,
                                                                          'ang1' : str(deg_to_rad(90-(360-rad_to_deg(seg_angle)))),
                                                                          'ang2' : str(deg_to_rad(270-(360-rad_to_deg(seg_angle))))},
                                                  engine).geom
        #
                    if len(this_lot_pt)==0 or len(lot.geom)>1:
                        print 'skipping lot:',lots.bbl.tolist()[i]
                        if len(lot.geom)>1:
                            print '\t\tabove too much for single lot'
                    else:

                        if show_some_detail==True: A.append(this_lot_pt[0]) # taking first value...
                        lot_pts.append({'bbl':bbl,
                                        'geom':this_lot_pt[0]})

                        ### Line Connecting Lot to Street
                        line_lot_to_street = self.T.gd.read_postgis("""
                            SELECT ST_ShortestLine(lot_pt,mid_pt) geom
                            FROM
                                st_geomfromtext(' %(lot_pt)s ',4326) lot_pt,
                                st_geomfromtext(' %(mid_pt)s ',4326) mid_pt
                            """.replace('\n','')%{'mid_pt'  : str(lot_seg_mid_pt.geom[0].to_wkt()),
                                                  'lot_pt'  : this_lot_pt[0]},engine)
            #
                        if len(line_lot_to_street.geom)>1:
                            self.T.gd.GeoSeries(line_lot_to_street.geom).plot()
                            print 'too much for Line Connecting Lot to Street'
                            raise SystemError
                        if show_some_detail==True: A.append(line_lot_to_street.geom[0])


        if show_some_detail==True:
            d=self.T.gd.GeoSeries(A).plot(fig_size=(26,22),
                                   save_fig_path=save_fig_path,
                                   save_and_show=False)

        if show_some_detail==True: me = raw_input('y?')
        else: me='y'

        if me=='y' and lot_pts!=[]:
            c = self.T.gd.GeoDataFrame(lot_pts)
            d = self.T.to_sql("""
                        UPDATE lot_pts
                        SET geom = the_geom
                        FROM
                            (SELECT these_bbl the_bbl
                                FROM unnest(array%(these_bbl)s) these_bbl) as t1,
                            (SELECT st_geomfromtext(these_geoms,4326) the_geom
                                FROM unnest(array%(these_geoms)s) these_geoms) as t2
                        WHERE bbl = the_bbl
                    """.replace('\n','')%{'these_bbl'  : str(c.bbl.tolist()),
                                          'these_geoms' : str([it.to_wkt() for it in c.geom])
                                          }
                           ,engine)
        else:
            if me!='y': break
    print 'DONE!'

def convert_street_names_in_lot_pts():
    cmd = "select bbl,bldg_street from lot_pts where geom is not null and ignore is false and bldg_street is not null"
    l = lots = self.T.pd.read_sql_query(cmd,engine)

    l = geoparse(l,'bldg_street','bldg_street')

    engine.execute('drop table if exists temp')
    l.to_sql('temp',engine,if_exists='append',index=False)
    # engine.execute('update lot_pts l set bldg_num_start = t.bldg_num_start from temp t where t.bbl = l.bbl')
    engine.execute('update lot_pts l set bldg_street = t.bldg_street from temp t where t.bbl = l.bbl')
    engine.execute('drop table if exists temp')

    ## Update lot_pts with lot_idx = {bldg_street.bldg_num}

    cmd = """
        SELECT bbl, a.bbl %(3)s
        FROM lot_pts a
        INNER JOIN unnest(array[%(1)s]) %(2)s
        ON a.start_idx <= %(2)s and %(2)s <= a.end_idx
        """.replace('\n',' ') % T
    res = self.T.pd.read_sql_query(cmd,engine)

def make_index(describe=True,commit=False):
    # from sys import path as sys_path
    # sys_path.append('/Users/admin/SERVER2/BD_Scripts/geolocation')
    # from f_postgres import geoparse,ST_PREFIX_DICT,ST_SUFFIX_DICT

    from re import search as re_search # re_search('pattern','string')
    from re import sub as re_sub  # re_sub('pattern','repl','string','count')

    cmd = "select bbl,bldg_num,bldg_street from lot_pts where geom is not null and ignore is false and bldg_street is not null"
    l = lots = self.T.pd.read_sql_query(cmd,engine)

    ## geoparse everything
    l = geoparse(l,'bldg_street','clean_addr')
    ## reduce to unique way (street,road,avenue,lane, etc...)
    addr_bbl_dict = dict(zip(l.clean_addr.tolist(),l.bbl.tolist()))
    ul = self.T.pd.DataFrame({'addr':addr_bbl_dict.keys(),'bbl':addr_bbl_dict.values()})

    # ul['has_number_in_name'] = ul.addr.map(lambda s: bool(re_search(r'[0-9]',s)))
    #
    # nn = non_numbered_streets = ul[ul.has_number_in_name==False].sort('addr')
    # n  = numbered_street      = ul.ix[ul.index-nn.index,:]
    #
    #n = a[a[street_column].str.contains('^(w)\s[0-9]+')==True]
    #from f_nyc_data import create_numbered_streets
    #n  = create_numbered_streets(n.copy(),street_column='addr')
    #print len(n.index),'length of generated number streets'
    #
    # ul_num_street_idx = nn.index.copy()
    # ul.drop(['has_number_in_name','bbl'],axis=1,inplace=True)
    # nn.drop(['has_number_in_name','bbl'],axis=1,inplace=True)
    # nn = nn.reset_index(drop=True)
    #
    # a = n.append(nn,ignore_index=True)
    # a = self.T.pd.DataFrame({'addr':dict(zip(a.addr.tolist(),range(0,len(a.index)))).keys()})

    a = ul.copy()
    a['nid'] = a.index
    a['bldg_street_idx'] = a.nid.map(lambda s: str('%05d' % s))
    a['addr'] = a.addr.map(lambda s: s.lower().strip())

    # add entries with or without different combinations of info (e.g., e/w, st/ave, etc...)
    a_idx = a.bldg_street_idx.tolist()

    # pre_re_s = re_search_string = r'^('+"|".join(ST_PREFIX_DICT.values())+r')\s'
    # alp  = a_less_prefix = self.T.pd.DataFrame({'addr':a.addr.map(lambda s: re_sub(pre_re_s,r'',s).strip()
    #                                        if ST_SUFFIX_DICT.values().count(  # this extra is to prevent a "st" result
    #                                         re_sub(pre_re_s,r'',s).strip()
    #                                         ) == 0 else ''),
    #                                      'bldg_street_idx': a_idx})

    suf_re_s = re_search_string = r'\s('+r'|'.join(ST_SUFFIX_DICT.values())+r')$'
    als  = a_less_suffix = self.T.pd.DataFrame({'addr':a.addr.map(lambda s: re_sub(suf_re_s,r'',s).strip()
                                           if ST_SUFFIX_DICT.values().count(  # this extra is to prevent a "w" result
                                            re_sub(suf_re_s,r'',s).strip()
                                            ) == 0 else s),
                                         'bldg_street_idx':a_idx})

    nw = num_ways = als[als.addr.str.contains('\d+')]
    nw['below13'] = nw.addr.map(lambda s: True if eval(re_search('\d+',s).group().strip())<=12 else False)
    nw['one_word'] = nw.addr.map(lambda s: True if len(s.split(' '))==1 else False)
    als = als[als.index.isin(nw[(nw.below13==True)&(nw.one_word==True)].index.tolist())==False]

    # alps = a_less_prefix_less_suffix = self.T.pd.DataFrame({'addr':alp.addr.map(lambda s: re_sub(suf_re_s,'',s).strip()),
    #                                      'bldg_street_idx':a_idx})

    alr = avenues_lettered_reversed = a[a.addr.map(lambda s: (s.find('avenue')==0)&(len(s.split(' '))==2))].copy()
    alr['addr'] = alr.addr.map(lambda s: s.split(' ')[1]+' ave')

    sns = street_not_st = a[a.addr.map(lambda s: bool(re_search('(st)$',s)))].copy()
    sns['addr'] = sns.addr.map(lambda s: re_sub(r'(st)$',r'street',s).strip())

    ana = avenue_not_ave = a[a.addr.map(lambda s: bool(re_search('(ave)$',s)))].copy()
    ana['addr'] = ana.addr.map(lambda s: re_sub(r'(ave)$',r'avenue',s).strip())

    if describe==True:
        print '\n',len(ana),'ana',ana.head()
        print '\n',len(sns),'sns',sns.head()
        print '\n',len(alr),'alr',alr.head()
        # print '\n',len(alp),'alp',alp.head()
        print '\n',len(als),'als',als.head()
        # print '\n',len(alps),'alps',alps.head()

    # combine all frames
    all_f = ana.append(sns,ignore_index=True)\
                .append(alr,ignore_index=True)\
                .append(als,ignore_index=True)\
                .reset_index(drop=True)



    # reduce to unique items in frames
    all_f_list = all_f.addr.tolist()
    all_f['cnt'] = all_f.addr.map(lambda s: all_f_list.count(s))
    all_f = all_f[all_f.cnt==1]

    # remove combine original street names with street name permutations
    uniq_orig_addr = a.addr.unique().tolist()
    all_f = all_f[all_f.addr.isin(uniq_orig_addr)==False].reset_index(drop=True)

    # add index 'nid' to new addr.s
    st_pt=int(a.nid.max())+1
    end_pt=st_pt+len(all_f)
    all_f['nid'] = range(st_pt,end_pt)

    A = a.append(all_f,ignore_index=True)
    A = A[A.addr!=''].reset_index(drop=True)
    # A.head()
    J = A.addr.tolist()
    all_unique_addr_list = dict(zip(J,range(0,len(J)))).keys()

    if describe==True:
        print '\n',all_unique_addr_list.count('s'),'"s" count'
        print all_unique_addr_list.count('w'),'"w" count\n'

    bldg_idx_addr_map = dict(zip(A.bldg_street_idx.tolist(),A.addr.tolist()))
    addr_bldg_idx_map = dict(zip(A.addr.tolist(),A.bldg_street_idx.tolist()))
    B = self.T.pd.DataFrame({'addr':all_unique_addr_list})
    B['bldg_street_idx'] = B.addr.map(addr_bldg_idx_map)
    st_pt=int(a.nid.max())+1
    end_pt=st_pt+len(B.index)
    B['nid'] = range(st_pt,end_pt)
    B['one_word'] = B.addr.map(lambda s: True if len(s.split(' '))==1 else False)

    if describe==True: print '\n',len(B.index),'total row count in addr_idx'

    if commit==False: return B
    engine.execute('drop table if exists addr_idx')
    B.to_sql('addr_idx',engine,if_exists='append',index=False)
    engine.execute('ALTER TABLE addr_idx ADD PRIMARY KEY (nid)')
    return B
def create_lot_idx_start_pts():
    cmd = """
    update lot_pts
    set lot_idx_start = to_number(
        concat(
            a.bldg_street_idx,
            '.',
            to_char(bldg_num,'00000')
        )
    ,'00000D00000')
    from addr_idx a
    where a.addr = bldg_street
    """.replace('\n',' ')
    engine.execute(cmd)
def create_lot_idx_end_pts():
    cmd = """
    update lot_pts
    set lot_idx_end = to_number(
        concat(
            a.bldg_street_idx,
            '.',
            to_char(bldg_num_end,'00000')
        )
    ,'00000D00000')
    from addr_idx a
    where a.addr = bldg_street
    """.replace('\n',' ')
    engine.execute(cmd)
def convert_zip_street_num_to_street_num():
    cmd="""
    update lot_pts l
    set lot_idx_end = to_number(
        concat(
            substring(to_char(end_idx,'999999999999999') from 7 for 5),
            '.',
            substring(to_char(end_idx,'999999999999999') from 12 for 5)
        )
    ,'99999D99999')
    where end_idx is not null
    """.replace('\n',' ')
    engine.execute(cmd)
def add_bldg_range_pts():
    ###Add "bldg_num_end" values

    cmd = "select bbl,bldg_num,bldg_street from lot_pts where geom is not null and ignore is false and bldg_street is not null"
    l = self.T.pd.read_sql_query(cmd,engine)
    uniq_streets = l.bldg_street.unique().tolist()

    i=0
    for u_st in uniq_streets:
        #print i,u_st
        i+=1
        d = l[(l.bldg_street==u_st)&(l.bldg_num!=0)].sort('bldg_num').reset_index(drop=True)
        d_len = len(d.index)
        if d_len > 0:
            d['d_idx'] = range(0,d_len)

            d['bldg_num_next'] = d.ix[:,['d_idx','bldg_num']
                                      ].apply(lambda s: s[1] if s[0]==d_len-1
                                              else (d.ix[s[0]+1,'bldg_num']-s[1])-1,axis=1)

            # d['bldg_num_start'] = d.ix[:,['d_idx','bldg_num'
            #                             ,'bldg_num_next']
            #                          ].apply(lambda s: s[1] if s[0]>0 else 0,axis=1)

            d['bldg_num_end'] = d.ix[:,['d_idx','bldg_num'
                                        ,'bldg_num_next']
                                     ].apply(lambda s: s[1] if s[0]==d_len-1
                                             else s[1]+s[2],axis=1)

            d=d.drop(['bldg_num_next','bldg_num','bldg_street','d_idx'],axis=1)

        #     if d.ix[0,'bldg_num_end']==-1:
        #         n = d[d.bldg_num_end>-1]
        #         n_idx = n.index[0]
        #         first_real = d.ix[n_idx,'bldg_num_end']
        #         d.ix[0,'bldg_num_end'] = first_real

            engine.execute('drop table if exists temp')
            d.to_sql('temp',engine,if_exists='append',index=False)
            # engine.execute('update lot_pts l set bldg_num_start = t.bldg_num_start from temp t where t.bbl = l.bbl')
            engine.execute('update lot_pts l set bldg_num_end = t.bldg_num_end from temp t where t.bbl = l.bbl')
            engine.execute('drop table if exists temp')

    #print self.T.pd.read_sql_query('select count(*) cnt from lot_pts',engine).cnt[0],'\tTOTAL LOTS'
    #print self.T.pd.read_sql_query('select count(*) cnt from lot_pts where bldg_num_end is null',engine).cnt[0],'\tremaining lots without bldg_num_end'
def add_geoms_to_index():

    def geoms_to_collection(geoms):
        s='GEOMETRYCOLLECTION('
        for it in geoms:
            s+=it.to_wkt()+','
        return s.rstrip(',')+')'

    def geoms_to_text(geoms):
        if type(geoms_to_text) != list: geoms=list(geoms)
        s=''
        for it in geoms:
            try:
                s+="ST_GeomFromText('"+it.to_wkt()+"'),"
            except:
                s+="ST_GeomFromText('"+it+"'),"
        return s.rstrip(',')

    def geom_txts_to_collection(geom_txts):
        return "ST_Collect(ARRAY["+geoms_to_text(geom_txts)+'])'

    lion_ways = self.T.gd.read_postgis("select lw.gid,lw.clean_street,lw.streetcode,lw.geom from addr_idx a "+
                                "inner join lion_ways lw on lw.clean_street = a.street",engine)
    print len(lion_ways),'total lion ways'
    uniq_streets = lion_ways.clean_street.unique().tolist()
    print len(uniq_streets),'uniq streets'
    uniq_streetcodes = lion_ways.streetcode.unique().tolist()
    print len(uniq_streetcodes),'uniq streetcodes'
    g = lion_ways.groupby('clean_street')
    t = []
    for name,grp in g:
        a=geom_txts_to_collection(grp.geom)
        T = { '1':a}
        cmd = """
        select st_astext(st_linefrommultipoint(st_boundary(st_unaryunion(
        %(1)s
        )))) this_line""".replace('\n',' ') % T
        this_line = self.T.pd.read_sql_query(cmd,engine).ix[0,'this_line']
        this_nid = self.T.pd.read_sql_query("select nid from addr_idx where street = '%s' order by nid"%name,engine).ix[0,'nid']
        t.append({'street':name,
                  'nid':this_nid,
                  'geom':this_line})
    d = self.T.pd.DataFrame(t)
    engine.execute('drop table if exists temp')
    d.to_sql('temp',engine,if_exists='append',index=False)
    engine.execute('update addr_idx a set geom = ST_GeomFromText(t.geom,4326) from temp t where t.nid = a.nid and t.street = a.street')
    engine.execute('drop table if exists temp')


def save_point_shape(fPath,x,y):

    schema = {
        'geometry': 'Point',
        'properties': {'id': 'int'},
    }

    pt_len = len(x)
    with fiona.open(fPath, 'w', 'ESRI Shapefile', schema) as c:
        for i in range(1,pt_len+1):
            c.write({
                'geometry': mapping(Point(x[i-1],y[i-1])),
                'properties': {'id': i},
            })
def save_polygon_shapefile():
    from osgeo import ogr

    # Here's an example Shapely geometry
    # poly = Polygon(zip(x_outer,y_outer))
    poly = poly_all.convex_hull

    # Now convert it to a shapefile with OGR
    driver = ogr.GetDriverByName('Esri Shapefile')
    ds = driver.CreateDataSource('/Users/admin/Projects/GIS/outer_MN.shp')
    layer = ds.CreateLayer('', None, ogr.wkbPolygon)
    # Add one attribute
    layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
    defn = layer.GetLayerDefn()

    ## If there are multiple geometries, put the "for" loop here

    # Create a new feature (attribute and geometry)
    feat = ogr.Feature(defn)
    feat.SetField('id', 123)

    # Make a geometry, from Shapely object
    geom = ogr.CreateGeometryFromWkb(poly.wkb)
    feat.SetGeometry(geom)

    layer.CreateFeature(feat)
    feat = geom = None  # destroy these

    # Save and close everything
    ds = layer = feat = geom = None
def polygon_from_points():
    #Create polygon from lists of points
    from_nodes = self.T.pd.read_sql_query("SELECT lat,lon FROM lion_nodes",engine)
    x = from_nodes.lon.tolist()
    y = from_nodes.lat.tolist()
    poly_all = Polygon(zip(x,y))

    # Extract the point values that define the perimeter of the polygon
    # x_outer,y_outer = poly_all.exterior.coords.xy
def plot_pluto_block(block):
    # 121 madison is on block 860
    m = self.T.gd.read_postgis("SELECT * FROM pluto where block = "+block+'"',conn)
    m.plot()
def geoms_to_text(geoms):
    if type(geoms) != list: geoms=[geoms]
    s=''
    for it in geoms:
        try:
            s+="ST_GeomFromText('"+it.to_wkt()+"',4326),"
        except:
            s+="ST_GeomFromText('"+it+"',4326),"
    return s.rstrip(',')
def geoms_as_text(geoms):
    if type(geoms) != list: geoms=[geoms]
    s=''
    for it in geoms:
        s+="ST_AsText("+it+"),"
    return s.rstrip(',')
def geom_inside_street_box(ways,geom_table,geom_label,table_cols=None,conditions=None):
    T = {'1':str(ways).strip('[]'),
         '2':geom_table,
         '3':geom_label}
    if table_cols!=None:
        T.update({'4':', %s'%str(table_cols).strip('[]').replace("'",'')})
    else:
        T.update({'4':''})
    cmd =   """
        select res_geom geom %(4)s
        from
            %(2)s t,
            (select z_get_way_box(%(1)s) box_geom) as d,
            st_intersection(t.%(3)s,box_geom) res_geom
        where t.%(3)s is not null
        and st_astext(res_geom) != 'GEOMETRYCOLLECTION EMPTY'
            """.replace('\n',' ') % T
    return cmd

def lion_node_changes1():
    t = 'lion_nodes'
    ####Get all unique lion_node ids with a Manhattan boolean attribute
    # select_lion = self.T.pd.read_hdf(BASE_SAVE_PATH+t+'_select.h5', 'table')
    # uniq_nodes = np.unique(np.array(select_lion.nodeidfrom.unique().tolist()+
    #                                 select_lion.nodeidto.unique().tolist())).tolist()
    ####Add Manhattan Column/Attribute to lion_nodes
    # ADD COLUMN
    cmd = "ALTER TABLE "+t+" ADD COLUMN manhattan boolean"
    # sql_cmd(cmd,engine)

    # ADD ATTRIBUTE
    cmd = ("UPDATE "+t+" SET manhattan = True "+
           "WHERE "+t+".nodeid IN "+
           str(uniq_nodes).replace("[u'","('").replace("u'","'").replace(']',')'))
    # sql_cmd(cmd,engine)
    ####Reduce Nodes in lion_nodes
    cmd="select gid,nodeid,geom from lion_nodes where lion_nodes.manhattan is true"
    with_MN=self.T.pd.read_sql_query(cmd,engine)
    print 'with_MN',len(with_MN.index)

    cmd="select gid,nodeid,geom from lion_nodes where lion_nodes.manhattan is not true"
    without_MN=self.T.pd.read_sql_query(cmd,engine)
    print 'without_MN',len(without_MN.index)

    cmd="select count(*) from lion_nodes"
    lion_cnt = self.T.pd.read_sql_query(cmd,engine)
    print 'lion_cnt',lion_cnt.ix[0,0]

    if lion_cnt.ix[0,0] - (len(with_MN.index) + len(without_MN.index)) == 0:
        print "with_MN + without_MN = lion_cnt"

        # drop column vintersect
        cmd="ALTER TABLE lion_nodes DROP COLUMN vintersect"
        # sql_cmd(cmd,engine)

        # delete without_MN
        cmd="DELETE FROM lion_nodes WHERE lion_nodes.manhattan is not true"
        # sql_cmd(cmd,engine)


    #     Keep only lion_ways
    #     where either lion_ways.nodeIDFrom or lion_ways.nodeIDTo
    #     are in ARRAY(lion_nodes.nodeid),
    #     i.e., lion_nodes.manhattan is True

    #     Note the format for "lion_nodes.nodeid" was numeric(10),
    #     whereas the format for "lion_ways.nodeidfrom" was char(254)

    ### NOTE:

    # Another way to do this:
    #
    # 1. load layer in QGIS,
    # 2. Click Database --> DB Manager,
    # 3. run sql "select gid,nodeid,vintersect,geom from lion_nodes where lion_nodes.manhattan is true", and
    # 4. create new layer and re-import.
def lion_nodes_changes2():
    ####Remove Nodes from "lion_nodes" based on changed "lion_ways"
    nodes_from_lion_nodes = self.T.pd.read_sql_query("SELECT nodeid FROM lion_nodes",engine)
    nodes_from_lion_ways = self.T.pd.read_sql_query("SELECT nodeidfrom,nodeidto FROM lion_ways",engine)

    check_nodes = nodes_from_lion_nodes.nodeid.map(int)
    good_nodes = nodes_from_lion_ways.nodeidfrom.map(int).append(nodes_from_lion_ways.nodeidto.map(int))

    check_nodes = self.T.pd.Series(check_nodes.unique())
    good_nodes = good_nodes.unique().tolist()

    bool_check = check_nodes.isin(good_nodes)
    df = self.T.pd.DataFrame({'nodes':check_nodes,'good':bool_check})
    nodes_to_remove = df[(df.good==False)].nodes.tolist()
    print len(nodes_from_lion_nodes.nodeid),'\t','total nodes'
    print len(nodes_to_remove),'\t','nodes to remove'

    # ## Finally, did a quick in QGIS, the marked nodes were ripe for removal, and the DB was updated via pgAdmin3 query:
    #
    #     DELETE FROM lion_nodes WHERE lion_nodes.remove IS True;
    #
    # ## Also, added coordinates (lat,long) to "lion_nodes"
    #
    #     ALTER TABLE lion_nodes ADD COLUMN lat float
    #     ALTER TABLE lion_nodes ADD COLUMN lon float
    #
    #     UPDATE lion_nodes SET lat = ST_Y(geom)
    #     UPDATE lion_nodes SET lon = ST_X(geom)
def pluto_changes():
    ##Pluto -- Alter Table and Add Columns

    # Create Index on pluto.address:
    #
    #     CREATE INDEX pluto_addr_idx ON pluto("address");
    #
    # Add Columns for building number and street:
    #
    #     ALTER TABLE pluto ADD COLUMN "bldg_num" integer;
    #     ALTER TABLE pluto ADD COLUMN "bldg_street" character varying (28);

    df = self.T.pd.read_sql_query("select gid,address,bldg_num,bldg_street from pluto;",conn)
    print '\n'
    print len(df.index),'\t','Total Addresses (slash geometries?)'
    df['isdigit']=df.bldg_num.map(lambda s: str(s).isdigit())
    a = df[(df.isdigit==True)&(df.bldg_num!=0)]
    print len(a.index),'\t','Buildings with Street Numbers'
    print len(df.index)-len(a.index),'\t','Difference'

    a['num'] = a.bldg_num.map(int)
    a.drop(['bldg_num','isdigit'],axis=1,inplace=True)
    a = a.rename(columns={'num':'bldg_num'})

    a['street'] = a.address.map(lambda s: s[s.find(' ')+1:].lower())
    a.drop(['bldg_street'],axis=1,inplace=True)
    a = a.rename(columns={'street':'bldg_street'})

    output = a
    print '\n',output.head()
    output.dtypes
    output.to_sql('pluto_info',engine,if_exists='append',index=False,index_label='gid')
def lion_ways_changes1():
    t='lion_ways'
    ####Remove All Non-Manhattan Entries:
    all_ways = sql_cmd("select count(*) from "+t,engine).fetchall()[0][0]
    mn_ways = sql_cmd("select count(*) from lion_ways where left( lion_ways.streetcode, 1 )  = '1'",engine).fetchall()[0][0]
    non_mn_ways = sql_cmd("select count(*) from lion_ways where left( lion_ways.streetcode, 1 )  != '1'",engine).fetchall()[0][0]
    print 'Total "'+t+'" \t=',all_ways
    print '\t'+'in Manhattan \t=',mn_ways
    print '\t'+'non-Manhattan \t=',non_mn_ways
    print '\t'+'TOTAL in & non \t=',mn_ways + non_mn_ways

    if mn_ways + non_mn_ways == all_ways:
        cmd = ("DELETE FROM "+t+" WHERE LEFT( lion_ways.streetcode, 1 )  != '1';")
    #     sql_cmd(cmd,engine)
        print '\n'+'DATA REMOVED from "'+t+'"'
        new_all_ways = sql_cmd("select count(*) from "+t,engine).fetchall()[0][0]
        print '\n'+'Total "'+t+'" \t=',new_all_ways

    all_ways = sql_cmd("select count(*) from "+t,engine).fetchall()[0][0]
    marked_ways = sql_cmd("select count(*) from lion_ways WHERE lion_ways.featuretyp IN ('1','2','3','7')",engine).fetchall()[0][0]
    remaining_ways = sql_cmd("select count(*) from lion_ways WHERE lion_ways.featuretyp NOT IN ('1','2','3','7')",engine).fetchall()[0][0]
    print 'Total "'+t+'" \t=',all_ways
    print '\t'+'marked \t\t=',marked_ways
    print '\t'+'remaining \t=',remaining_ways
    print '\t'+'SUM TOTAL \t=',marked_ways + remaining_ways

    if marked_ways + remaining_ways == all_ways:
        cmd = ("DELETE FROM "+t+" WHERE lion_ways.featuretyp IN ('1','2','3','7')")
    #     sql_cmd(cmd,engine)
        print '\n'+'DATA REMOVED from "'+t+'"'
        new_all_ways = sql_cmd("select count(*) from "+t,engine).fetchall()[0][0]
        print '\n'+'Total "'+t+'" \t=',new_all_ways
def add_clean_street_to_lion_ways():
    lw = self.T.pd.read_sql_query("select lw.gid,lw.street from lion_ways lw",engine)
    lw = geoparse(lw,'street','clean_street')
    engine.execute('drop table if exists temp')
    lw.to_sql('temp',engine,if_exists='append',index=False)
    engine.execute('update lion_ways lw set clean_street = t.clean_street from temp t where t.gid = lw.gid')
    engine.execute('drop table if exists temp')
def reduce_ways():
    ##Reduce Ways_vertices_pgr

    # Here, trying to reduce OSM data by using convex hull of pluto to find points outside of the hull and set remove to true.

    pts = self.T.gd.read_postgis("SELECT id gid,the_geom geom FROM ways_vertices_pgr",conn)
    f = '/Users/admin/Projects/GIS/map_data/MN_pluto_lines_hull.shp'
    hull = self.T.gd.GeoDataFrame.from_file(f)

    #####Executed these SQL queries in QGIS:

        # ALTER TABLE ways_vertices_pgr ADD COLUMN "remove" boolean;
        # UPDATE ways_vertices_pgr SET remove = False;

def postgres_plotting():
    ##Plotting

    # from pylab import *
    import matplotlib.pyplot as plt
    from matplotlib.collections import PatchCollection
    from mpl_toolkits.basemap import Basemap
    from shapely.geometry import Point, MultiPoint, MultiPolygon
    from descartes import PolygonPatch
    b = hull.bounds.ix[0,:]
    ####Make subplot for hull
    minx, maxx, miny, maxy = b['minx'], b['maxx'], b['miny'], b['maxy']
    w, h = maxx - minx, maxy - miny
    fig = plt.figure(figsize=(w*1.2,h*1.2))
    ax = fig.add_axes([minx - 0.2, miny - 0.1, w, h])
    patches = [PolygonPatch(hull.geometry[0],fc='#cc00cc', ec='#555555', alpha=0.5, zorder=4)]
    ax.add_collection(PatchCollection(patches, match_original=True));
    ####Make subplot for way_vertices
    x,y = zip(*pts.geom.map(lambda s: s.coords[0]))
    T = self.T.pd.DataFrame({'lon': x ,'lat' : y })
    slim_T = T[(minx<=T.lon)&(T.lon<=maxx)&(miny<=T.lat)&(T.lat<=maxy)]
    a=len(T.index)
    b=len(slim_T.index)
    print 'Total Way Vertices','\t',a
    print 'Way Vertices in hull','\t',b

    ax2 = fig.add_subplot(222)
    ax2.plot(slim_T.lon, slim_T.lat, 'r')

    # main figure
    ax2.set_xlabel('long.')
    ax2.set_ylabel('lat.')
    ax2.set_title('OSM Pts in Pluto')
    plt.show();
def enable_pg_routing_with_nyc_data():
    ### Enable pgRouting for NYC Data

    ## create new table for routing:
    cmd1 = "CREATE SEQUENCE mn_ways_gid_seq START 1;"
    cmd2 = """create table mn_ways (
                gid integer NOT NULL DEFAULT nextval('mn_ways_gid_seq'::regclass),
                length double precision,
                name text,
                x1 double precision,
                y1 double precision,
                x2 double precision,
                y2 double precision,
                reverse_cost double precision,
                rule text,
                to_cost double precision,
                maxspeed_forward integer,
                maxspeed_backward integer,
                osm_id bigint,
                priority double precision DEFAULT 1,
                the_geom geometry(LineString,4326),
                source integer,
                target integer,
                CONSTRAINT pk_mn_ways PRIMARY KEY (gid)
            );""".replace('\n','')
    # engine.execute(cmd1)
    # engine.execute(cmd2)

    ## add simplified geom data from lion_ways

    # uniq_streets = self.T.pd.read_sql_query("select street streets from lion_ways",engine).streets.unique().tolist()
    # print len(uniq_streets)
    # i=1
    # for u_st in uniq_streets[1:]:
    #     print i,u_st
    #     cmd = ("insert into mn_ways (the_geom) "+
    #            "select (st_dump( st_unaryunion( st_collect(l.geom) ) ) ).geom "+
    #            "from lion_ways l where l.street = '"+u_st+"'")
    #     engine.execute(cmd)
    #     i+=1

    ## add length  ( in meters...)  (divide by 0.3048)
    # engine.execute("UPDATE mn_ways SET length = "+
    #                "ST_Length_Spheroid(the_geom,'SPHEROID["+
    #                '"'+"WGS 84"+'"'+",6378137,298.257223563]')::DOUBLE PRECISION;")


    ## create table for nodes (b/c nodes will be generated)
    # used createtopo but wanted to use nodenetwork
    # engine.execute("SELECT pgr_nodenetwork         ('mn_ways', 0.00001,'the_geom','gid');") # need to fix

    # also, see
        # pgr_nodenetwork(text, double precision, text, text, text)
        # pgr_createverticestable(edge_table, the_geom, source, target, row_where)

    ## create topology
    # engine.execute("SELECT pgr_createTopology('mn_ways', 0.00001, 'the_geom', 'gid');")

    ## add indicies
    # engine.execute('CREATE INDEX mn_source_idx ON mn_ways("source");')
    # engine.execute('CREATE INDEX mn_target_idx ON mn_ways("target");')

    ## add cost column
    # engine.execute('UPDATE mn_ways SET reverse_cost = length;')
def use_USPS_street_abbr_as_nyc_street_names():
    ###Use USPS Street Abbr. as map for NYC Street Names (unsuccessful)

    from regex import sub as re_sub
    from regex import escape as re_escape

    def multisub(subs, subject):
        "Simultaneously perform all substitutions on the subject string."
        pattern = '|'.join('(%s)' % p for p, s in subs)
    #     print pattern
        substs = [s for p, s in subs]
    #     print substs
        replace = lambda m: ' '+substs[m.lastindex - 1]+' '
        return re_sub('\s?'+pattern+'\s?', replace, subject)

        # multisub([('hi', 'bye'), ('bye', 'hi')], 'hi and bye')
        # returns 'bye and hi'
    # multisub(X,'1 AVENUE LOWER NB ROADBED'.lower())

    # fpath_abbr = SND_NON_S_PATH.replace('snd14Bcow_non_s_recs','usps_street_abbr')
    # u = self.T.pd.read_csv(fpath_abbr,index_col=0)
    # fpath_abbr_regex = fpath_abbr.replace('.csv','_regex.csv')
    # df = self.T.pd.read_csv(fpath_abbr_regex,index_col=0)
    # print len(u),len(df)

    # g = u.groupby('prim_suff')
    # df = self.T.pd.DataFrame({'prim_suff':g.groups.keys()}).sort('prim_suff').reset_index(drop=True)

    # suff_abbr_map = dict(zip(u.prim_suff.tolist(),u.usps_abbr.tolist()))
    # df['usps_abbr'] = df.prim_suff.map(suff_abbr_map)

    # f = lambda s: str(g.get_group(s).common_use.map(lambda s: s.lower()).tolist()).strip(' ').replace(', ',' | ').replace("'",'').strip('[]')
    # df['pattern'] = df.prim_suff.map(f)
    # df['combined'] = df.ix[:,['pattern','usps_abbr']].apply(lambda s: (s[0],s[1].lower()),axis=1)
    # usps_repl_list = df.combined.tolist()


    # regex_repl_path = '/Users/admin/Projects/GIS/table_data/usps_street_abbr_regex.txt'
    # f = open(regex_repl_path,'r')
    # usps_repl_list = f.read().split('\n')
    # f.close()

    tmp = d.ix[:50,:]
    tmp['clean2'] = tmp.full_stname.map(lambda s: multisub(usps_repl_list,s.lower()))
    tmp.ix[:,['full_stname','clean_name','clean2']]

def combine_east_west():
    ### Combine East/West Streets
    a = self.T.gd.read_postgis("select street,geom from addr_idx where geom is not null",engine)

    ns = num_streets = a[a.street.str.contains('(^e\s[0-9])|(^w\s[0-9])')==True]
    ns['num'] = ns.street.map(lambda s: eval(re_search(r'([0-9]+)',s).groups()[0]))
    ns['below60'] = ns.num.map(lambda d: True if d<60 else False)
    ns.head()
    east_streets = ns[(ns.street.str.contains('^e\s[0-9]')==True)&(ns.below60==True)].sort('num').reset_index(drop=True)
    west_streets = ns[(ns.street.str.contains('^w\s[0-9]')==True)&(ns.below60==True)].sort('num').reset_index(drop=True)
    east_num_geom_map = dict(zip(east_streets.num.tolist(),east_streets.geom.tolist()))
    west_num_geom_map = dict(zip(west_streets.num.tolist(),west_streets.geom.tolist()))
    east_keys,west_keys = east_num_geom_map.keys(),west_num_geom_map.keys()
    save_for_later = []
    for it in east_keys:
        if west_keys.count(it)==0:
            save_for_later.append({'street_names':str(it)+' st',
                                   'geom':east_num_geom_map.pop(it)})

    for it in west_keys:
        if east_keys.count(it)==0:
            save_for_later.append({'street_names':str(it)+' st',
                                   'geom':east_num_geom_map.pop(it)})
    east_keys,west_keys = east_num_geom_map.keys(),west_num_geom_map.keys()
    east_streets = east_streets[east_streets.num.isin(east_keys)].sort('num').reset_index(drop=True)
    west_streets = west_streets[west_streets.num.isin(west_keys)].sort('num').reset_index(drop=True)
    print len(east_streets),len(west_streets)
    print east_streets.head()
    print west_streets.head()

    cs = combined_streets = self.T.pd.DataFrame({'street':east_streets.street.map(lambda s: s.replace('e ','')).tolist(),
                                          'east':east_streets.geom.tolist(),
                                          'west':west_streets.geom.tolist()})
    cs = cs.ix[:,['street','west','east']]
    cs['west'] = cs.west.map(str)
    cs['east'] = cs.east.map(str)

    g = cs.ix[:,['west','east']].as_matrix()
    T = { '1':'street',
          'street_names':str(east_streets.street.map(lambda s: s.replace('e ','')).tolist()).replace("u'","'").strip('[]'),
          '2':'streets',
          'streets':str(g).replace(")'\n  '",")<.>").replace("']\n [ '","', '").strip('[]')}
    cmd =   """
            SELECT
                %(1)s[i],
                st_makeline(
                    st_geomfromtext((string_to_array(%(2)s[i],'<.>'))[1]),
                    st_geomfromtext((string_to_array(%(2)s[i],'<.>'))[2])
                ) geom
            FROM (
                SELECT generate_series(1, array_upper(%(2)s, 1)) AS i, %(2)s, %(1)s
                FROM
                    (SELECT array[%(street_names)s] %(1)s) as %(1)s,
                    (SELECT array[%(streets)s] %(2)s) as %(2)s
                ) t
            """.replace('\n',' ') % T

    g = self.T.gd.read_postgis(cmd,engine)

    nid_start = 1+self.T.pd.read_sql_query("select nid from addr_idx order by nid desc limit 1",engine).nid.tolist()[0]
    g['nid'] = range(nid_start,nid_start+len(g))
    g['geom'] = g.geom.map(str)

    engine.execute('drop table if exists temp')
    g.to_sql('temp',engine,if_exists='append',index=False)
    engine.execute('INSERT INTO addr_idx (street, nid, geom) select street, nid, st_geomfromtext(geom,4326) from temp')
    engine.execute('drop table if exists temp')

def compare_lion_ways_content():
    ### Read Lion Ways from DB/File
    current_db,new_db = 'lion_ways','lion_ways2'
    conditions = [" where specaddr is null",
                  ' and (lboro = 1 or rboro=1) ',
                  " and segmenttyp != 'R' "]

    # cols = ['gid','street','safstreetn','featuretyp','segmenttyp','rb_layer',
    #    'specaddr','facecode','seqnum','streetcode','safstreetc','geom']

    cmd = "select %(1)s from %(3)s %(2)s" % \
            { '1' : '*',#','.join(cols),
              '2' : ''.join(conditions),
              '3' : current_db}
    a1 = self.T.gd.read_postgis(cmd,engine)

    cmd2 = "select %(1)s from %(3)s %(2)s" % \
            { '1' : '*',#','.join(cols),
              '2' : ''.join(conditions),
              '3' : new_db}
    a2 = self.T.gd.read_postgis(cmd,engine)
    ### Comparing Overlapping Streets
    uniq_geoms = a2.geom.map(lambda s: str(s)).unique().tolist()
    all_str_geoms = a2.geom.map(lambda s: str(s)).tolist()
    print '\nUnique Geoms, All Geoms, (difference)'
    print len(uniq_geoms),len(all_str_geoms),len(all_str_geoms)-len(uniq_geoms),'\n'
    a2['cnt'] = a2.geom.map(lambda s: all_str_geoms.count(str(s)))
    a2['str_geom'] = a2.geom.map(lambda s: str(s))
    A=a2[a2.cnt>1].sort(['str_geom','gid'],ascending=[True,True])
    G = A.groupby('str_geom')
    K = G.groups.keys()
    print len(K),'groups of matching geoms being checked...\n'
    rem_cols = ['gid','llo_hyphen','lhi_hyphen','rlo_hyphen','rhi_hyphen','fromleft','toleft','fromright','toright']
    skip_these_gid = []#[99689,104049,
    #                   99686,104048,]
    skip_these_streets = ['BIKE PATH']
    end_msg,t=True,[]
    for grp in K:
        r = G.get_group(grp).reset_index(drop=True)
        R1 = r.drop(rem_cols,axis=1).ix[0,:].to_dict()
        R2 = r.drop(rem_cols,axis=1).ix[1,:].to_dict()
        if R1!=R2:
            if (len(r[r.gid.isin(skip_these_gid)==False])>0 &
                len(r[r.street.isin(skip_these_streets)==False])>1):
                j=R1.keys()
                for it in j:
                    if str(R1[it])!=str(R2[it]):
                        print K.index(grp)
                        print it
                        print R1[it]
                        print R2[it]
                        end_msg=False
                        break
        t.append(r.ix[1,'gid'])

    l=self.T.pd.DataFrame({'gid':t,'geom':None})
    engine.execute('drop table if exists temp')
    l.to_sql('temp',engine,if_exists='append',index=False)
    engine.execute("update lion_ways l set geom = t.geom from temp t where t.gid = l.gid")
    engine.execute('drop table if exists temp')
    end_msg=True
    if end_msg==True:
        print 'Between',current_db,'and',new_db,'with the conditions:\n'
        for it in conditions:
            print '-',it
        print '\nThe only differences between rows were in columns:\n'
        for it in rem_cols:
            print '-',it
        print '\n',len(t),'rows had geoms stripped\n'

