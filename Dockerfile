FROM apache/airflow:2.10.5

# Switch to airflow user (official way)
USER airflow

# Copy requirements file
COPY requirements.txt .

# Install Python packages globally for airflow user
RUN pip install --no-cache-dir -r requirements.txt
