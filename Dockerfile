FROM python:3.11-slim-bullseye

RUN apt-get update && apt-get -y upgrade \
    && apt-get install gcc -y  \
    && pip install --upgrade pip

# Set separate working directory for easier debugging.
WORKDIR /app

RUN pip install 'poetry==1.8.3'
COPY pyproject.toml poetry.lock ./
# Install the dependencies first, so that we can cache them.
RUN poetry install

# Copy everything. (Note: If needed, we can use .dockerignore to limit what's copied.)
COPY . .

# Install again, now that we've copied the jinjat package files. Otherwise,
# Jinjat itself won't be installed.
RUN poetry install

EXPOSE 8084
ENV UNIVERSQL_HOST=0.0.0.0

ENTRYPOINT ["poetry", "run", "universql"]
