import streamlit as st  
import pandas as pd  
import sqlalchemy  
from sqlalchemy import text  
from streamlit_autorefresh import st_autorefresh  

# Define database connection parameters as a dictionary
db_config = {
    "host": "postgresql",  # Service name from docker-compose.yml or host address
    "dbname": "invoice_db",  # Database name
    "user": "my_user",  # Database user
    "password": "my_password"  # Database password
}

# Format the database connection URL using the above parameters
db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"

# Establish a database connection using SQLAlchemy's create_engine method
engine = sqlalchemy.create_engine(db_url)

# Configure the Streamlit page layout to wide mode for better data presentation
st.set_page_config(layout="wide")

# Streamlit app title
st.title('Dashboard for Invoices')

# Description text explaining the purpose of the Streamlit app
st.write("This Streamlit app connects to a PostgreSQL database, displaying data that refreshes every 5 seconds.")

# Initialize auto-refresh with a 10-second interval
st_autorefresh(interval=5000, key="data_refresh")

# Try block to handle database connection and data fetching
try:
    # Establishing a connection to the database
    with engine.connect() as conn:
        # SQL query to select the latest 10 invoices from the database
        # Assuming "CreatedTime" could be a problematic column, this is a straightforward select query
        query = text("""
            SELECT billnum, createdtime, storeid, paymentmode, totalvalue
            FROM invoices
            ORDER BY id DESC
            LIMIT 20
        """)
        # Executing the query
        result = conn.execute(query)
        # Converting the result into a pandas DataFrame for easier manipulation and display
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Displaying a message to indicate successful data fetching
    st.write("Data fetched from the database:")

    # Displaying the fetched data as a DataFrame in the Streamlit app
    st.dataframe(df)

# Exception block to handle and display errors during database connection or query execution
except Exception as e:
    # Displaying an error message followed by the specific error detail
    st.write("Error connecting to the database:")
    st.write(str(e))