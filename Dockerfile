FROM python:3.13.8-slim

# RUN apt-get update && apt-get install -y iputils-ping gdal-bin aptitude libpq-dev libgdal-dev libsqlite3-mod-spatialite gcc g++ curl
RUN apt-get update
# Install UV

RUN pip install uv

# RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal && export C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app

# Copy only the dependency files first
COPY pyproject.toml uv.lock /app/

# Install dependencies

# RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi
RUN uv pip install -r pyproject.toml --no-cache-dir --system

COPY . /app

# ENV SPATIALITE_LIBRARY_PATH=/usr/lib/aarch64-linux-gnu/mod_spatialite

# Expose the application port (FastAPI default is 8000)
EXPOSE 8888

CMD ["uvicorn", "main:app","--host", "0.0.0.0", "--port", "88"]