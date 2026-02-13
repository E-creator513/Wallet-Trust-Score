# Base image
FROM apache/airflow:2.8.1

# Switch to root to copy files
USER root

# Copy entrypoint (if you have custom one)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy requirements
COPY requirements.txt /requirements.txt

# Switch to airflow user before installing Python packages
USER airflow

# Upgrade pip and install packages
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Entrypoint
ENTRYPOINT ["/entrypoint.sh"]
CMD ["standalone"]
