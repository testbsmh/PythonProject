import streamlit as st
import snowflake.connector
import pandas as pd
import json
import datetime

def fetch_data(sql_query):
    conn = snowflake.connector.connect(
        user='<your_username>',
        password='<your_password>',
        account='<your_account_identifier>',
        warehouse='<your_warehouse>',
        database='<your_database>',
        schema='<your_schema>'
    )
    try:
        df = pd.read_sql(sql_query, conn)
    finally:
        conn.close()
    return df

def run_query(query_config):
    sql = f"{query_config['base_sql']} WHERE {query_config['filter']}" if query_config['filter'] else query_config['base_sql']
    return fetch_data(sql)

def main():
    st.set_page_config(layout="wide")

    if 'queries' not in st.session_state:
        st.session_state['queries'] = []

    tab1, tab2 = st.tabs(["Dashboard", "Configuration"])

    with tab1:
        st.header("Dashboard")

        if st.button("Refresh All"):
            for i, query_config in enumerate(st.session_state['queries']):
                st.session_state['queries'][i]['result'] = run_query(query_config)

        if not st.session_state['queries']:
            st.info("No queries added yet. Please add queries in the Configuration tab.")
        else:
            for i, query in enumerate(st.session_state['queries']):
                st.subheader(query['name'])
                result = query.get('result')
                if result is not None:
                    st.write(f"Number of Rows: {len(result)}")
                    st.dataframe(result)
                else:
                    st.warning("No data to display. Run the queries in the Configuration tab.")

    with tab2:
        st.header("Configuration")

        if st.button("Add New Query"):
            st.session_state['queries'].append({
                'name': f"Query {len(st.session_state['queries']) + 1}",
                'base_sql': "SELECT * FROM table",
                'filter': "",
            })

        to_remove_indexes = []
        for i, query in enumerate(st.session_state['queries']):
            with st.expander(f"Query {i+1}: {query['name']}"):
                query['name'] = st.text_input(f"Query Name {i+1}", value=query['name'], key=f"name_{i}")
                query['base_sql'] = st.text_area(f"SQL Query {i+1}", value=query['base_sql'], key=f"sql_{i}")
                query['filter'] = st.text_input(f"Filter Logic {i+1} (OPTIONAL)", value=query['filter'], key=f"filter_{i}")

                if 'import_info' in query:
                    st.write(f"Imported on: {query['import_info']}")

                if st.button(f"Test Query {i+1}", key=f"test_{i}"):
                    try:
                        test_result = run_query({'base_sql': query['base_sql'], 'filter': query['filter']})
                        st.write(f"Test succeeded: {len(test_result)} rows returned.")
                        st.dataframe(test_result)
                    except Exception as e:
                        st.error(f"Test failed: {e}")

                if st.button(f"Run Query {i+1}", key=f"run_{i}"):
                    st.session_state['queries'][i]['result'] = run_query({'base_sql': query['base_sql'], 'filter': query['filter']})

                if st.button(f"Remove Query {i+1}", key=f"remove_{i}"):
                    to_remove_indexes.append(i)

        for index in reversed(to_remove_indexes):
            del st.session_state['queries'][index]

        if st.button("Run All Queries"):
            for i, query_config in enumerate(st.session_state['queries']):
                st.session_state['queries'][i]['result'] = run_query(query_config)

        if st.button("Export Queries"):
            export_data = [{'name': q['name'], 'base_sql': q['base_sql'], 'filter': q['filter']} for q in st.session_state['queries']]
            json_data = json.dumps(export_data, indent=2)
            st.download_button(
                label="Download Queries Configuration",
                data=json_data,
                file_name='queries_config.json',
                mime='application/json'
            )

        uploaded_file = st.file_uploader("Upload Queries Configuration", type='json')
        if uploaded_file:
            imported_queries = json.load(uploaded_file)
            # Append new imported queries with metadata
            current_user = st.text_input("Enter your user name for import tagging:", value="default_user")
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for query in imported_queries:
                query['import_info'] = f"Imported by {current_user} on {current_time}"
                st.session_state['queries'].append(query)
            st.success("Queries configuration imported successfully!")

if __name__ == "__main__":
    main()
