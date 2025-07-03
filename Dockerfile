FROM python:3.11-slim-bullseye

RUN apt-get update && apt-get -y upgrade \
    && apt-get install gcc curl libpq-dev python3-dev -y  \
    && pip install --upgrade pip

# Install uv
RUN pip install uv

# Set separate working directory for easier debugging.
WORKDIR /app

# Copy everything first. (Note: If needed, we can use .dockerignore to limit what's copied.)
COPY . .

# Install dependencies using uv
RUN uv sync --frozen

# Ensure frontend build directory exists (will be empty if not built)
RUN mkdir -p dist frontend/dist ui/dist web/dist client/dist app/dist

# Install the project
RUN uv pip install -e .

EXPOSE 5432 8084
ENV SERVER_HOST=0.0.0.0
ENV USE_LOCALCOMPUTING_COM=1

ENTRYPOINT ["uv", "run", "universql"]