# Import python packages
import streamlit as st
import time

import snowflake.permissions as permissions
from snowflake.snowpark.context import get_active_session


session = get_active_session()

def objects():
    session.call("private.install")
    session.call("private.recurring_run")
    session.call("private.tests")
    session.call("private.reports")
    session.call("private.assessment")
    session.call("private.grant_select")
    session.call("config.update_installation_status")
    

if not permissions.get_held_account_privileges(["CREATE DATABASE"]) or not permissions.get_held_account_privileges(["CREATE WAREHOUSE"]) or not permissions.get_held_account_privileges(["EXECUTE TASK"]) or not permissions.get_held_account_privileges(["MANAGE WAREHOUSES"]) or not permissions.get_held_account_privileges(["EXECUTE MANAGED TASK"]):
    permissions.request_account_privileges(['CREATE DATABASE', 'CREATE WAREHOUSE', 'EXECUTE TASK', 'MANAGE WAREHOUSES', 'EXECUTE MANAGED TASK'])

if not permissions.get_reference_associations('ext_snoptimizer_wh'):
    permissions.request_reference("ext_snoptimizer_wh")
else:
    st.title('Snoptimizer! :snowflake:')
    with st.spinner('Waiting on Snowflake Streamlit!'):
        time.sleep(2)
        s = session.sql("""
        SELECT 
            status
        FROM config.installation_status
        """)
        if s.collect()[0][0] == 'PRE-INSTALLED':
            objects()

    
    # st.write("Created OBJECTS")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(['Snoptimizer Summary :cyclone:', 'Cost Optimization :credit_card:', 'Security Optimization :safety_vest:', 'Performance Optimization :white_flower:',  'Assessment :bookmark_tabs:'])

    with tab1:

    # st.title('Databases')
        s = session.sql("""
        SELECT 
            COUNT(*) DATABASES_COUNTED,
            MAX(LOGGED_DT) LOGGED_DT
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_DATABASES_ONGOING
        GROUP BY LOGGED_DT
        ORDER BY LOGGED_DT DESC
        """)
        # st.dataframe(s, use_container_width=True)

        # st.metric("Databases", s.collect()[0][0])

        # st.title('External Functions')
        q = session.sql("""
        SELECT 
            COUNT(*) EXTERNAL_FUNCTIONS_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_EXTERNAL_FUNCTIONS_ONGOING
        """)
        # st.dataframe(q, use_container_width=True)

        # st.metric("External Functions", q.collect()[0][0])

        l = session.sql("""
        SELECT 
            COUNT(*) FILE_FORMATS_COUNTED,
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_FILE_FORMATS_ONGOING
        """)

        m = session.sql("""
        SELECT 
            COUNT(*) MASKING_POLICIES_COUNTED, 
            MAX(LOGGED_DT)  
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_MASKING_POLICIES_ONGOING
        """)

        o = session.sql("""
        SELECT 
            COUNT(*) MATERIALIZED_VIEWS_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_MATERIALIZED_VIEWS_ONGOING
        """)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Databases", s.collect()[0][0])
        col2.metric("External Functions", q.collect()[0][0])
        col3.metric("File Formats", l.collect()[0][0])
        col4.metric("Masking Policies", m.collect()[0][0])
        col5.metric("Materialized Views", o.collect()[0][0])

        st.divider()

        s = session.sql("""
        SELECT 
            COUNT(*) PIPES_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_PIPES_ONGOING
        """)

        q = session.sql("""
        SELECT 
            COUNT(*) PROCEDURES_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_PROCEDURES_ONGOING
        GROUP BY LOGGED_DT
        ORDER BY LOGGED_DT DESC
        """)

        l = session.sql("""
        SELECT 
            COUNT(*) REPLICATION_DATABASES_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_REPLICATION_DATABASES_ONGOING
        """)

        m = session.sql("""
        SELECT 
            COUNT(*) RESOURCE_MONITORS_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_RESOURCE_MONITORS_ONGOING
        """)

        o = session.sql("""
        SELECT 
            COUNT(*) SEQUENCES_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_SEQUENCES_ONGOING
        """)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Pipes", s.collect()[0][0])
        col2.metric("Procedures", q.collect()[0][0])
        col3.metric("Replication Databases", l.collect()[0][0])
        col4.metric("Resource Monitors", m.collect()[0][0])
        col5.metric("Sequences", o.collect()[0][0])

        st.divider()

        s = session.sql("""
        SELECT 
            COUNT(*) STAGES_COUNTED,  
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_STAGES_ONGOING
        """)

        q = session.sql("""
        SELECT 
            COUNT(*) STREAMS_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_STREAMS_ONGOING
        """)

        l = session.sql("""
        SELECT 
            COUNT(*) TASKS_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_TASKS_ONGOING
        GROUP BY LOGGED_DT
        ORDER BY LOGGED_DT DESC
        """)

        m = session.sql("""
        SELECT 
            COUNT(*) USER_FUNCTIONS_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_USER_FUNCTIONS_ONGOING
        GROUP BY LOGGED_DT
        ORDER BY LOGGED_DT DESC
        """)

        o = session.sql("""
        SELECT 
            COUNT(*) WAREHOUSES_COUNTED, 
            MAX(LOGGED_DT) 
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_WAREHOUSES_ONGOING
        GROUP BY LOGGED_DT
        ORDER BY LOGGED_DT DESC
        """)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Stages", s.collect()[0][0])
        col2.metric("Streams", q.collect()[0][0])
        col3.metric("Tasks", l.collect()[0][0])
        col4.metric("User Functions", m.collect()[0][0])
        col5.metric("Warehouses", o.collect()[0][0])

    with tab4:
        st.title('Potential Optimizations')
        s = session.sql("""
        SELECT 
            TO_DATE(CREATED_DT), 
            COUNT(*) 
        FROM EXT_SNOPTIMIZER_SERVICES.SNOPTIMIZER S
        GROUP BY 1
        """)
        
        st.dataframe(s, use_container_width=True)
        # st.metric("Potential Optimizations", s.collect()[1][1])

    with tab2:
        st.title('Cost Fixes')
        v = session.sql("""
        SELECT 
            'Cost Fixes' AS "Cost Fixes",
            -- S.RUN_ID,
            -- S.ACTION_ID,
            -- TO_VARCHAR(DECRYPT(S.SQL_TEXT1, ADMIN.PASS_KEY(CURRENT_ACCOUNT()),'***','aes-gcm'), 'utf-8') SQL_TEXT1,
            -- S.EST_RISK_SAVINGS_CREDITS,S.CREATED_DT,
            COUNT(*)
        FROM EXT_SNOPTIMIZER_SERVICES.SNOPTIMIZER S
        WHERE S.ACTION_ID IN (
        101,
        102,
        103,
        104,
        105,
        106,
        107,
        108,
        109,
        111,
        112,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
        121,
        122,
        123,
        124,
        130,
        131,
        132,
        133,
        134,
        135,
        136,
        137,
        138,
        139,
        140,
        142,
        143,
        144,
        151,
        152,
        153,
        154,
        155,
        156,
        157,
        158,
        159,
        160
        )
        """)
        st.dataframe(v, use_container_width=True)
        # st.metric("Cost Fixes", v.collect()[1][1])

    with tab3:
        st.title('Security Fixes')
        v = session.sql("""
        SELECT
            'Security Fixes' AS "Security Fixes",
            -- S.RUN_ID,
            -- S.ACTION_ID,
            -- TO_VARCHAR(DECRYPT(S.SQL_TEXT1, ADMIN.PASS_KEY(CURRENT_ACCOUNT()),'***','aes-gcm'), 'utf-8') SQL_TEXT1,
            -- S.EST_RISK_SAVINGS_CREDITS,
            -- CREATED_DT,
            COUNT(*) 
        FROM EXT_SNOPTIMIZER_SERVICES.SNOPTIMIZER S 
        WHERE S.ACTION_ID IN (
        201,
        202,
        203,
        204,
        207,
        208,
        209,
        210,
        211,
        212,
        214,
        215,
        251,
        252,
        253,
        254,
        255,
        256,
        257,
        258,
        259,
        260,
        261,
        262,
        263,
        264,
        265,
        266,
        267,
        268,
        269,
        270,
        271,
        272,
        274,
        275,
        276,
        277,
        278,
        280,
        281,
        282,
        283,
        284,
        285
        )
        """)
        st.dataframe(v, use_container_width=True)
        # st.metric("Security Fixes", v.collect()[1][1])

    with tab5:

        st.subheader('Warehouse')
        s = session.sql("""
                SELECT 
                    COUNT(*) as "Auto suspend check"
                    ,
                    LOGGED_DT
                FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_WAREHOUSES_ONGOING
                WHERE IFNULL("auto_suspend",0) = 0
                GROUP BY 2
                ORDER BY 2 DESC
                """)
        # st.dataframe(s, use_container_width=True)
        data = s.collect()
        if data:
            st.metric("No. of warehouses without auto_suspend settings", s.collect()[0][0])
        else:
            st.metric("No. of warehouses without auto_suspend settings", 0)
            st.write('All the warehouses have enabled auto_suspend setting.')

        st.divider()

        s = session.sql("""
                SELECT 
                    COUNT(*) as "Long auto suspend check"
                    ,
                    LOGGED_DT
                FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_WAREHOUSES_ONGOING
                WHERE "auto_suspend" > 300
                GROUP BY 2
                ORDER BY 2 DESC
                """)
        data = s.collect()
        if data:
            st.metric("No. of warehouses with long auto_suspend settings", s.collect()[0][0])
        else:
            st.metric("No. of warehouses with long auto_suspend settings", 0)

        st.divider()

        st.subheader('Auto clustering')
        s = session.sql("""
                SELECT 
                    date,
                    avg_daily_credits
                FROM EXT_SNOPTIMIZER_SERVICES.REPORTS_SNOPTIMIZER_111
                """)

        data = s.collect()
        if data:
            st.dataframe(s, use_container_width=True)
        else:
            st.dataframe(s, use_container_width=True)
            st.write('No objects found with auto clustring.')

        st.divider()

        st.subheader('Materialized views')
        s = session.sql("""
                SELECT 
                    date,
                    avg_daily_credits
                FROM EXT_SNOPTIMIZER_SERVICES.REPORTS_SNOPTIMIZER_113
                """)

        data = s.collect()
        if data:
            st.dataframe(s, use_container_width=True)
        else:
            st.dataframe(s, use_container_width=True)
            st.write('No materialized views objects found.')

        st.divider()

        st.subheader('Search Optimization')
        s = session.sql("""
                SELECT 
                    date,
                    avg_daily_credits
                FROM EXT_SNOPTIMIZER_SERVICES.REPORTS_SNOPTIMIZER_115
                """)

        data = s.collect()
        if data:
            st.dataframe(s, use_container_width=True)
        else:
            st.dataframe(s, use_container_width=True)
            st.write('No search optimization objects found.')

        st.divider()

        st.subheader('Snowpipe')
        s = session.sql("""
                SELECT 
                    date,
                    avg_daily_credits
                FROM EXT_SNOPTIMIZER_SERVICES.REPORTS_SNOPTIMIZER_117
                """)

        data = s.collect()
        if data:
            st.dataframe(s, use_container_width=True)
        else:
            st.dataframe(s, use_container_width=True)
            st.write('No snowpipe objects found.')

        st.divider()

        st.subheader('Replication')
        s = session.sql("""
                SELECT 
                    date,
                    avg_daily_credits
                FROM EXT_SNOPTIMIZER_SERVICES.REPORTS_SNOPTIMIZER_119
                """)

        data = s.collect()
        if data:
            st.dataframe(s, use_container_width=True)
        else:
            st.dataframe(s, use_container_width=True)
            st.write('No replication objects found.')


        st.title('Snoptimizer Details')
        s = session.sql("""
        SELECT 'CURRENT_ACCOUNT' INFO_NAME, CURRENT_ACCOUNT() INFO_VALUE UNION
        SELECT 'CURRENT_REGION' INFO_NAME, CURRENT_REGION() INFO_VALUE UNION
        SELECT 'CURRENT_USER' INFO_NAME, CURRENT_USER() INFO_VALUE UNION
        SELECT 'CURRENT_ROLE' INFO_NAME, CURRENT_ROLE() INFO_VALUE UNION
        SELECT 'CURRENT_DATABASE' INFO_NAME, CURRENT_DATABASE() INFO_VALUE UNION
        SELECT 'CURRENT_SCHEMA' INFO_NAME, CURRENT_SCHEMA() INFO_VALUE UNION
        SELECT 'CURRENT_VERSION' INFO_NAME, CURRENT_VERSION() INFO_VALUE
        """)
        st.dataframe(s, use_container_width=True)


        st.title('Assessment Details')
        s = session.sql("""
        SELECT
            WH_TEMP."name"
            ,"auto_suspend"
            ,ASSESS_QH_BY_DAYHOUR.WAREHOUSE_NAME
            ,ASSESS_QH_BY_DAYHOUR.WAREHOUSE_ID
            ,ASSESS_QH_BY_DAYHOUR.START_TIME_DATE
            ,ASSESS_QH_BY_DAYHOUR.START_TIME_HOUR_FULL
            /* CREDIT DETAILS from METERING */
            ,CREDITS_USED_BY_DAYHOUR
            ,CREDITS_USED_COMPUTE_BY_DAYHOUR
            ,CREDITS_USED_CLOUD_SERVICES_BY_DAYHOUR
            ,QUERY_COUNT_BY_DAYHOUR
            ,QUERY_COUNT_BY_DAYHOUR/CREDITS_USED_COMPUTE_BY_DAYHOUR as QUERY_COUNT_BY_CREDITS_USED_COMPUTE
            ,AVG_COMPILATION_TIME_BY_DAYHOUR
            ,AVG_EXECUTION_TIME_BYHOUR
            ,AVG_QUEUED_PROVISIONING_TIME_BY_DAYHOUR
            ,AVG_QUEUED_REPAIR_TIME_BY_DAYHOUR
            ,AVG_QUEUED_OVERLOAD_TIME_BY_DAYHOUR
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_WAREHOUSES_ONGOING as WH_TEMP, EXT_SNOPTIMIZER_SERVICES.ASSESS_QH_BY_DAYHOUR, EXT_SNOPTIMIZER_SERVICES.ASSESS_WHE_BY_DAYHOUR, EXT_SNOPTIMIZER_SERVICES.ASSESS_WHL_BY_DAYHOUR, EXT_SNOPTIMIZER_SERVICES.ASSESS_WHM_BY_DAYHOUR
        WHERE WH_TEMP."name" = ASSESS_QH_BY_DAYHOUR.WAREHOUSE_NAME

            AND ASSESS_QH_BY_DAYHOUR.START_TIME_DATE = ASSESS_WHE_BY_DAYHOUR.START_TIME_DATE AND ASSESS_QH_BY_DAYHOUR.START_TIME_HOUR_FULL = ASSESS_WHE_BY_DAYHOUR.START_TIME_HOUR_FULL
            AND ASSESS_QH_BY_DAYHOUR.WAREHOUSE_NAME = ASSESS_WHE_BY_DAYHOUR.WAREHOUSE_NAME AND ASSESS_QH_BY_DAYHOUR.WAREHOUSE_ID = ASSESS_WHE_BY_DAYHOUR.WAREHOUSE_ID

            AND ASSESS_WHE_BY_DAYHOUR.START_TIME_DATE = ASSESS_WHL_BY_DAYHOUR.START_TIME_DATE AND ASSESS_WHE_BY_DAYHOUR.START_TIME_HOUR_FULL = ASSESS_WHL_BY_DAYHOUR.START_TIME_HOUR_FULL
            AND ASSESS_WHE_BY_DAYHOUR.WAREHOUSE_NAME = ASSESS_WHL_BY_DAYHOUR.WAREHOUSE_NAME AND ASSESS_WHE_BY_DAYHOUR.WAREHOUSE_ID = ASSESS_WHL_BY_DAYHOUR.WAREHOUSE_ID

            AND ASSESS_WHL_BY_DAYHOUR.START_TIME_DATE = ASSESS_WHM_BY_DAYHOUR.START_TIME_DATE AND ASSESS_WHL_BY_DAYHOUR.START_TIME_HOUR_FULL = ASSESS_WHM_BY_DAYHOUR.START_TIME_HOUR_FULL
            AND ASSESS_WHL_BY_DAYHOUR.WAREHOUSE_NAME = ASSESS_WHM_BY_DAYHOUR.WAREHOUSE_NAME
        """)
        st.dataframe(s, use_container_width=True)

        
        s = session.sql("""
        WITH DETAILED_JOBS AS 
        (SELECT
            WAREHOUSE_NAME,
            QUERY_ID,
            TIME_SLICE(START_TIME::TIMESTAMP_NTZ, 10, 'MINUTE','START') AS INTERVAL_START,
            DATABASE_NAME,
            QUERY_TYPE,
            TOTAL_ELAPSED_TIME,
            COMPILATION_TIME,
            (QUEUED_PROVISIONING_TIME + QUEUED_REPAIR_TIME + QUEUED_OVERLOAD_TIME) AS QUEUED_TIME,
            TRANSACTION_BLOCKED_TIME,
            EXECUTION_TIME
        FROM EXT_SNOPTIMIZER_SERVICES.EXT_SNOPT_WAREHOUSES_ONGOING
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY QH ON EXT_SNOPT_WAREHOUSES_ONGOING."name" = QH.WAREHOUSE_NAME
        WHERE DATE_TRUNC('DAY', START_TIME) = CURRENT_DATE()-1
        AND EXECUTION_STATUS = 'SUCCESS'
        AND QUERY_TYPE IN ('SELECT','UPDATE','INSERT','MERGE','DELETE')
        ),
        INTERVAL_STATS AS (
        SELECT
            WAREHOUSE_NAME,
            QUERY_TYPE,
            INTERVAL_START,
            COUNT(DISTINCT QUERY_ID) AS NUMJOBS,
            MEDIAN(TOTAL_ELAPSED_TIME)/1000 AS P50_TOTAL_DURATION,
            (PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY TOTAL_ELAPSED_TIME))/1000 AS P95_TOTAL_DURATION,
            SUM(TOTAL_ELAPSED_TIME)/1000 AS SUM_TOTAL_DURATION,
            SUM(COMPILATION_TIME)/1000 AS SUM_COMPILATION_TIME,
            SUM(QUEUED_TIME)/1000 AS SUM_QUEUED_TIME,
            SUM(TRANSACTION_BLOCKED_TIME)/1000 AS SUM_TRANSACTION_BLOCKED_TIME,
            SUM(EXECUTION_TIME)/1000 AS SUM_EXECUTION_TIME,
            ROUND(SUM_COMPILATION_TIME/SUM_TOTAL_DURATION,2) AS COMPILATION_RATIO,
            ROUND(SUM_QUEUED_TIME/SUM_TOTAL_DURATION,2) AS QUEUED_RATIO,
            ROUND(SUM_TRANSACTION_BLOCKED_TIME/SUM_TOTAL_DURATION,2) AS BLOCKED_RATIO,
            ROUND(SUM_EXECUTION_TIME/SUM_TOTAL_DURATION,2) AS EXECUTION_RATIO,
            ROUND(SUM_TOTAL_DURATION/NUMJOBS,2) AS TOTAL_DURATION_PERJOB,
            ROUND(SUM_COMPILATION_TIME/NUMJOBS,2) AS COMPILATION_PERJOB,
            ROUND(SUM_QUEUED_TIME/NUMJOBS,2) AS QUEUED_PERJOB,
            ROUND(SUM_TRANSACTION_BLOCKED_TIME/NUMJOBS,2) AS BLOCKED_PERJOB,
            ROUND(SUM_EXECUTION_TIME/NUMJOBS,2) AS EXECUTION_PERJOB
        FROM DETAILED_JOBS
        GROUP BY 1,2,3
        ORDER BY 1,2,3
        )
        SELECT * FROM INTERVAL_STATS
        """)
        st.dataframe(s, use_container_width=True)
