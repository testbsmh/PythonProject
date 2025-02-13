import streamlit as st
import snowflake.connector
import pandas as pd


def fetch_data(sql_query):
    # Replace with your actual Snowflake credentials
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
    sql = f"{query_config['base_sql']} WHERE {query_config['filter']}" if query_config['filter'] else query_config[
        'base_sql']
    return fetch_data(sql)


def main():
    st.set_page_config(layout="wide")

    if 'queries' not in st.session_state:
        st.session_state['queries'] = []

    # Tabs for Dashboard and Configuration
    tab1, tab2 = st.tabs(["Dashboard", "Configuration"])

    with tab1:
        st.header("Dashboard")

        # Refresh All button for the dashboard
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

        # Button to add a new query
        if st.button("Add New Query"):
            st.session_state['queries'].append({
                'name': f"Query {len(st.session_state['queries']) + 1}",
                'base_sql': "SELECT * FROM table",
                'filter': "",
                'result': None
            })

        # Functionality to configure each query
        for i, query in enumerate(st.session_state['queries']):
            with st.expander(f"Query {i + 1}: {query['name']}"):
                query['name'] = st.text_input(f"Query Name {i + 1}", value=query['name'], key=f"name_{i}")
                query['base_sql'] = st.text_area(f"SQL Query {i + 1}", value=query['base_sql'], key=f"sql_{i}")
                query['filter'] = st.text_input(f"Filter Logic {i + 1} (OPTIONAL)", value=query['filter'],
                                                key=f"filter_{i}")

                # Test button to execute current query setup
                if st.button(f"Test Query {i + 1}", key=f"test_{i}"):
                    try:
                        test_result = run_query(
                            {'base_sql': st.session_state[f"sql_{i}"], 'filter': st.session_state[f"filter_{i}"]})
                        st.write(f"Test succeeded: {len(test_result)} rows returned.")
                        st.dataframe(test_result)
                    except Exception as e:
                        st.error(f"Test failed: {e}")

                # Save result in session state when the Run Query button is clicked
                if st.button(f"Run Query {i + 1}", key=f"run_{i}"):
                    st.session_state['queries'][i]['result'] = run_query(
                        {'base_sql': st.session_state[f"sql_{i}"], 'filter': st.session_state[f"filter_{i}"]})

        if st.button("Run All Queries"):
            for i, query_config in enumerate(st.session_state['queries']):
                st.session_state['queries'][i]['result'] = run_query(query_config)


if __name__ == "__main__":
    main()
