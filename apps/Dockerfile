# Start from an official Python 3.8 slim image as the base
FROM python:3.8-slim

# Set /app as the working directory within the container
# All subsequent actions will be performed in this directory
WORKDIR /app

# Copy everything from the current directory on the host
# to /app in the container
COPY . /app

# Install Streamlit, psycopg2-binary for PostgreSQL support,
# SQLAlchemy for database interaction, and streamlit-autorefresh for auto-refreshing the Streamlit app
# Using --no-cache-dir option with pip install to keep the image size small
RUN pip install --no-cache-dir streamlit psycopg2-binary sqlalchemy streamlit-autorefresh

# Expose port 8501 for Streamlit applications
# This port is used by Streamlit by default and will be mapped to the host
EXPOSE 8501

# Define the command to run when the container starts
# This command starts the Streamlit application using the streamlit_app.py script
CMD ["streamlit", "run", "streamlit_app.py"]
