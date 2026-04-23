# Use the official Apache Airflow image
FROM apache/airflow:2.8.1

# Copy the requirements file
COPY requirements.txt .

# Install custom requirements (like SSH providers)
RUN pip install --no-cache-dir -r requirements.txt

# Copy your local DAGs into the container
COPY dags/ /opt/airflow/dags/

# Copy any configurations you might have
COPY airflow_variables.json /opt/airflow/airflow_variables.json

EXPOSE 8080
