import streamlit as st
import snowflake.connector
import pandas as pd
import json

# Function to connect to Snowflake and fetch data
def fetch_data():
    conn = snowflake.connector.connect(
        user='<your_username>',
        password='<your_password>',
        account='<your_account_identifier>',
        warehouse='<your_warehouse>',
        database='<your_database>',
        schema='<your_schema>'
    )

    query_table1 = "SELECT * FROM table1"
    df_table1 = pd.read_sql(query_table1, conn)

    query_table2 = "SELECT * FROM table2"
    df_table2 = pd.read_sql(query_table2, conn)

    conn.close()
    return df_table1, df_table2

# Function to compare two DataFrames
def compare_data(df1, df2):
    df_diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
    return df_diff

# Streamlit UI
def main():
    st.title("Snowflake Data Viewer")

    if st.button("Fetch Data"):
        df_table1, df_table2 = fetch_data()
        st.session_state['table1'] = df_table1
        st.session_state['table2'] = df_table2
        st.session_state['diff'] = compare_data(df_table1, df_table2)

    if 'table1' in st.session_state and 'table2' in st.session_state and 'diff' in st.session_state:
        tab1, tab2, tab3 = st.tabs(["Table 1", "Table 2", "Differences"])

        with tab1:
            st.header("Table 1 Data")
            st.dataframe(st.session_state['table1'])

        with tab2:
            st.header("Table 2 Data")
            st.dataframe(st.session_state['table2'])

        with tab3:
            st.header("Differences Between Table 1 and Table 2")
            st.dataframe(st.session_state['diff'])

if __name__ == "__main__":
    main()
