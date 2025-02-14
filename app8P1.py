import datetime
import json
import urllib

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy import text
import io
import xlsxwriter

#pip freeze > requirements.txt
#streamlit run app7.py

def get_sqlalchemy_engine(conn_config):
    user = urllib.parse.quote_plus(conn_config['user'])
    password = urllib.parse.quote_plus(conn_config['password'])
    connection_string = (
        f"snowflake://{user}:{password}@{conn_config['account']}/"
        f"{conn_config['database']}/{conn_config['schema']}?"
        f"warehouse={conn_config['warehouse']}&role={conn_config.get('role', '')}"
    )
    return create_engine(connection_string)

def fetch_data(sql_query, conn_config, sample_size=None):
    engine = get_sqlalchemy_engine(conn_config)
    with engine.connect() as connection:
        if sample_size:
            # Use LIMIT SQL clause to fetch sample data
            sql_query = f"{sql_query} LIMIT {sample_size}"
        df = pd.read_sql(sql_query, connection)
    return df
def run_query(query_config, connections, full_fetch=False):
    if 'connection_id' not in query_config:
        st.error("Connection not specified for query.")
        return None

    conn_config = next((conn for conn in connections if conn['id'] == query_config['connection_id']), None)
    if not conn_config:
        st.error("Selected connection configuration not found.")
        return None

    sql = query_config['base_sql']
    if query_config.get('filter'):
        sql += f" WHERE {query_config['filter']}"

    try:
        # Use sample_size to control data fetching based on full_fetch flag
        sample_size = None if full_fetch else 5  # Change 5 to your desired sample size for display
        df = fetch_data(sql, conn_config, sample_size)
        if df is not None and not df.empty:
            return df
        st.warning("Query did not return any rows.")
        return None
    except Exception as e:
        st.error(f"Test failed: {e}")
        return None
def get_row_count(sql_query, conn_config):
    engine = get_sqlalchemy_engine(conn_config)
    count_query = f"SELECT COUNT(*) FROM ({sql_query}) AS total"
    with engine.connect() as connection:
        result = connection.execute(text(count_query))
        row_count = result.scalar()
    return row_count

def main():
    st.set_page_config(layout="wide")

    if 'queries' not in st.session_state:
        st.session_state['queries'] = []
    if 'connections' not in st.session_state:
        st.session_state['connections'] = []
    if 'groups' not in st.session_state:
        st.session_state['groups'] = ['Default Group']

    tab1, tab2, tab3 = st.tabs(["Dashboard", "Configuration", "Snowflake Config"])

    with tab1:
        st.header("Dashboard")

        groups = {}
        for query in st.session_state['queries']:
            group_name = query['group']
            if group_name not in groups:
                groups[group_name] = []
            groups[group_name].append(query)

        for group_name, group_queries in groups.items():
            st.subheader(f"Group: {group_name}")
            if st.button(f"Refresh Group: {group_name}"):
                for query in group_queries:
                    query['result'] = run_query(query, st.session_state['connections'])

            for i, query in enumerate(group_queries):
                st.markdown(f"**{query['name']}** - Tag: {query.get('tag', 'None')}")

                # Get and display total row count
                row_count = get_row_count(query['base_sql'], st.session_state['connections'][0])
                st.write(f"Available Rows: {row_count}")

                # Checkbox to show sample data
                show_table_checkbox = st.checkbox(f"Show Sample for {query['name']}", key=f"show_sample_{i}")

                if show_table_checkbox:
                    # Load sample data on checkbox activation
                    sample_df = run_query(query, st.session_state['connections'], full_fetch=False)
                    if sample_df is not None:
                        st.dataframe(sample_df)

                    # Button to export full data to Excel
                    if st.button(f"Export Full Data for {query['name']}", key=f"export_{i}"):
                        full_df = run_query(query, st.session_state['connections'], full_fetch=True)
                        if full_df is not None:
                            towrite = io.BytesIO()
                            with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
                                full_df.to_excel(writer, index=False, sheet_name='Sheet1')
                            towrite.seek(0)
                            st.download_button(
                                label="Download Excel",
                                data=towrite,
                                file_name=f"{query['name']}_full.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        else:
                            st.warning("No data available to export.")

    with tab2:
        st.header("Configuration")

        new_group_name = st.text_input("New Group Name", value="")
        if st.button("Add New Group") and new_group_name:
            st.session_state['groups'].append(new_group_name)
            st.success(f"Group '{new_group_name}' added!")

        if st.button("Add New Query"):
            st.session_state['queries'].append({
                'group': st.session_state['groups'][0],  # Default to first group
                'name': f"Query {len(st.session_state['queries']) + 1}",
                'base_sql': "SELECT * FROM table",
                'filter': "",
                'connection_id': None,
                'tag': '',
                'result': None
            })

        for i, query in enumerate(st.session_state['queries']):
            with st.expander(f"{query['group']} - {query['name']}"):
                query['name'] = st.text_input(f"Query Name for Query {i + 1}", value=query['name'], key=f"name_{i}")
                query['base_sql'] = st.text_area(f"SQL Query for Query {i + 1}", value=query['base_sql'],
                                                 key=f"sql_{i}")
                query['filter'] = st.text_input(f"Filter Logic for Query {i + 1} (OPTIONAL)", value=query['filter'],
                                                key=f"filter_{i}")
                query['tag'] = st.text_input(f"Tag for Query {i + 1}", value=query['tag'], key=f"tag_{i}")

                query['group'] = st.selectbox(
                    f"Select Group for Query {i + 1}",
                    options=st.session_state['groups'],
                    index=st.session_state['groups'].index(query['group']) if query['group'] in st.session_state[
                        'groups'] else 0,
                    key=f"group_select_{i}"
                )

                connection_options = {conn['id']: conn['name'] for conn in st.session_state['connections']}
                query['connection_id'] = st.selectbox(
                    f"Select Connection for Query {i + 1}",
                    options=connection_options.keys(),
                    format_func=lambda x: connection_options[x],
                    key=f"conn_select_{i}"
                )


                if st.button(f"Test Query {i + 1}", key=f"test_{i}"):
                    try:
                        test_result = run_query(query, st.session_state['connections'])
                        if test_result is not None:
                            st.write(f"Test succeeded: {test_result} rows returned.")

                            print(f"Test succeeded: {test_result} rows returned.")
                    except Exception as e:
                        st.error(f"Test failed: {e}")
                        print(f"Test failed: {e}")

                if st.button(f"Remove Query {i + 1}", key=f"remove_{i}"):
                    st.session_state['queries'].pop(i)

        if st.button("Export Queries"):
            export_data = [
                {
                    'group': q['group'],
                    'name': q['name'],
                    'base_sql': q['base_sql'],
                    'filter': q['filter'],
                    'connection_id': q['connection_id'],
                    'tag': q['tag']
                }
                for q in st.session_state['queries']
            ]
            json_data = json.dumps(export_data, indent=2)
            st.download_button(
                label="Download Queries Configuration",
                data=json_data,
                file_name='queries_config.json',
                mime='application/json'
            )

        uploaded_file = st.file_uploader("Upload Queries Configuration", type='json')
        if uploaded_file:
            try:
                imported_queries = json.load(uploaded_file)
                current_user = st.text_input("Enter your user name for import tagging:", value="default_user")
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for query in imported_queries:
                    query['import_info'] = f"Imported by {current_user} on {current_time}"
                    st.session_state['queries'].append(query)
                st.success("Queries configuration imported successfully!")

                print("Queries configuration imported successfully!")
            except json.JSONDecodeError:
                st.error("Failed to import queries due to JSON format issues.")
                print("Failed to import queries due to JSON format issues.")

    with tab3:
        st.header("Snowflake Config")

        if st.button("Add Snowflake Configuration"):
            st.session_state['connections'].append({
                'id': f"conn_{len(st.session_state['connections']) + 1}",
                'name': f"Connection {len(st.session_state['connections']) + 1}",
                'user': '',
                'password': '',
                'account': '',
                'url': '',  # Snowflake URL, optional
                'warehouse': '',
                'database': '',
                'schema': ''
            })

        for i, conn in enumerate(st.session_state['connections']):
            with st.expander(f"Connection {i + 1}: {conn['name']}"):
                conn['name'] = st.text_input(f"Connection Name {i + 1}", value=conn['name'], key=f"conn_name_{i}")
                conn['user'] = st.text_input(f"User {i + 1}", value=conn.get('user', ''), key=f"user_{i}")
                conn['password'] = st.text_input(f"Password {i + 1}", type="password", value=conn.get('password', ''),
                                                 key=f"password_{i}")
                conn['account'] = st.text_input(f"Account {i + 1}", value=conn['account'], key=f"account_{i}")
                conn['url'] = st.text_input(f"URL {i + 1} (OPTIONAL)", value=conn.get('url', ''), key=f"url_{i}")
                conn['warehouse'] = st.text_input(f"Warehouse {i + 1}", value=conn['warehouse'], key=f"warehouse_{i}")
                conn['database'] = st.text_input(f"Database {i + 1}", value=conn['database'], key=f"database_{i}")
                conn['schema'] = st.text_input(f"Schema {i + 1}", value=conn['schema'], key=f"schema_{i}")

                if st.button(f"Test Connection {i + 1}", key=f"test_conn_{i}"):
                    success, message = test_snowflake_connection(conn)
                    if success:
                        st.success(message)
                        st.spinner()
                    else:
                        st.error(message)
                        st.spinner()

                if st.button(f"Remove Connection {i + 1}", key=f"remove_conn_{i}"):
                    st.session_state['connections'].pop(i)
                    st.spinner()

        if st.button("Export Snowflake Configurations"):
            export_connections = [{key: conn[key] for key in conn if key not in ('user', 'password')} for conn in
                                  st.session_state['connections']]
            connections_json = json.dumps(export_connections, indent=2)
            st.download_button(
                label="Download Configurations",
                data=connections_json,
                file_name='snowflake_config.json',
                mime='application/json'
            )
            st.spinner()

        conn_uploaded_file = st.file_uploader("Upload Snowflake Configurations", type='json')
        if conn_uploaded_file:
            # Handle JSON parsing carefully
            try:
                imported_connections = json.load(conn_uploaded_file)
                st.session_state['connections'].extend(imported_connections)
                st.success("Snowflake configurations imported successfully!")
                st.spinner()
                print("Snowflake configurations imported successfully!")
            except json.JSONDecodeError:
                st.error("Failed to import Snowflake configurations. Please check your JSON file format.")
                print("Failed to import Snowflake configurations. Invalid JSON format.")


def test_snowflake_connection(conn_config):
    try:
        engine = get_sqlalchemy_engine(conn_config)
        with engine.connect() as connection:
            # Use text() to define the query
            result = connection.execute(text(
                "SELECT CURRENT_ACCOUNT(),CURRENT_USER(),CURRENT_WAREHOUSE(),CURRENT_DATABASE(),CURRENT_SCHEMA(),CURRENT_REGION(),CURRENT_CLIENT(),CURRENT_SESSION()")).fetchall()
            # Display a brief success message
            print(f"Connected successfully to Snowflake version: {result}")
            # Construct session details in a single line
            for session_detail in result:
                session_info = (
                    f"Session ID: {session_detail[0]}, "
                    f"User: {session_detail[1]}, "
                    f"Warehouse: {session_detail[2]}, "
                    f"Database: {session_detail[3]}, "
                    f"Schema: {session_detail[4]}, "
                    f"Region: {session_detail[5]}, "
                    f"Driver Version: {session_detail[6]}, "
                    f"Account ID: {session_detail[7]}"
                )
                st.write(f"Session details: {session_info}")
        return True, "Connection successful!"
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return False, f"Connection failed: {e}"

if __name__ == "__main__":
    main()
