from quay.io/astronomer/astro-runtime:10.0.0-python-3.11

RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-bigquery==3.1.3 &&\
    pip install --no-cache-dir soda-core-scientific==3.1.3 && deactivate
