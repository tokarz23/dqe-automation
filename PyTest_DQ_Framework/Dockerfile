FROM python:3.12-slim

WORKDIR /app

# System dependencies for psycopg2 and pyarrow
RUN apt-get update && apt-get install -y gcc libpq-dev build-essential python3-dev

# Copy both folders
COPY data_dev ./pipeline_folder
COPY PyTest_DQ_Framework ./pytest_folder

# Install all requirements (merge if possible, or install both)
COPY data_dev/requirements.txt ./pipeline-requirements.txt
COPY PyTest_DQ_Framework/requirements.txt ./pytest-requirements.txt
RUN pip install --upgrade pip && \
    pip install -r pipeline-requirements.txt && \
    pip install -r pytest-requirements.txt

# Default command (can be overridden)
CMD ["bash"]