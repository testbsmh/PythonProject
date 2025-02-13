import streamlit as st
import snowflake.connector
import pandas as pd

def fetch_data(sql_query):
    # Replace with your real Snowflake credentials
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
    """Runs the query from the query_config containing SQL and filter."""
    sql = f"{query_config['base_sql']} WHERE {query_config['filter']}" if query_config['filter'] else query_config['base_sql']
    return fetch_data(sql)

def main():
    if 'queries' not in st.session_state:
        st.session_state['queries'] = []

    # Add new query configuration
    if st.button("Add Query"):
        st.session_state['queries'].append({
            'name': f"Query {len(st.session_state['queries']) + 1}",
            'base_sql': "SELECT * FROM table",
            'filter': "",
            'result': None
        })

    # Refresh all queries
    if st.button("Refresh All"):
        for i, query_config in enumerate(st.session_state['queries']):
            st.session_state['queries'][i]['result'] = run_query(query_config)

    # Display all query tiles
    for i, query in enumerate(st.session_state['queries']):
        st.text_input(f"Query Name {i+1}", value=query['name'], key=f"name_{i}")
        st.text_area(f"SQL Query {i+1}", value=query['base_sql'], key=f"sql_{i}")
        st.text_input(f"Filter Logic {i+1} (OPTIONAL)", value=query['filter'], key=f"filter_{i}")

        if st.button(f"Run Query {i+1}"):
            st.session_state['queries'][i]['result'] = run_query(query)

        # Show results if available
        result = st.session_state['queries'][i]['result']
        if result is not None:
            st.write(f"Number of Rows: {len(result)}")
            st.dataframe(result)

if __name__ == "__main__":
    main()

