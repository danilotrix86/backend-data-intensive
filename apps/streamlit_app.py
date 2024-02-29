import streamlit as st
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from streamlit_autorefresh import st_autorefresh

# Database connection parameters
db_config = {
    "host": "postgresql",  # Use the service name from docker-compose.yml
    "dbname": "invoice_db",  # Your database name
    "user": "my_user",  # Your database user
    "password": "my_password"  # Your database password
}
db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}"

# Create a database connection using SQLAlchemy
engine = sqlalchemy.create_engine(db_url)

# Streamlit application
st.title('Dashboard for Invoices')

st.write("This Streamlit app connects to a PostgreSQL database, displaying data that refreshes every 10 seconds.")

# Setup auto-refresh
st_autorefresh(interval=10000, key="data_refresh")



# Attempt to connect to the database and query data
try:
    with engine.connect() as conn:
        # Assuming "CreatedTime" is the problematic column, attempting a workaround:
        query = text("""
            SELECT billnum, createdtime, storeid, paymentmode, totalvalue
            FROM invoices
            ORDER BY id DESC
            LIMIT 10
        """)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    st.write("Data fetched from the database:")
    # Before modification
    st.set_page_config(layout="wide")

    # Your existing code for displaying the dataframe
    st.dataframe(df)

except Exception as e:
    st.write("Error connecting to the database:")
    st.write(str(e))
