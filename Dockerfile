# syntax=docker/dockerfile:1
#
# Combined Senzing file load + redo driver (Python) on the official runtime image.
#
#   docker build -t brian/sz_file_combined_consumer .                        # both DB backends
#   docker build --build-arg WITH_MSSQL=0    -t brian/sz_file_combined_consumer:pg    .
#   docker build --build-arg WITH_POSTGRES=0 -t brian/sz_file_combined_consumer:mssql .
#
# Keep the engine config and the runtime image on the SAME Senzing version: a
# config written by a newer engine referencing plugins absent in an older
# runtime fails at redo time with SENZ0087.

ARG BASE_IMAGE=senzing/senzingsdk-runtime:4.4.1
FROM ${BASE_IMAGE}
ARG BASE_IMAGE
RUN echo "Building from base image: ${BASE_IMAGE}"

LABEL org.opencontainers.image.title="sz_file_combined_consumer" \
      org.opencontainers.image.description="Combined Senzing file load + redo driver (Python)" \
      org.opencontainers.image.licenses="Apache-2.0"

USER root

# Backend selection (build-time). Default = both on. At least one is required.
# libpostgresqlplugin.so reaches PostgreSQL via libpq5 (preinstalled in the base
# image); libmssqlplugin.so reaches SQL Server via the Microsoft ODBC driver
# (installed here from the Debian 13 / trixie MS repo). Debian's own unixODBC is
# built with --enable-fastvalidate — keep it; do NOT substitute Microsoft's
# Ubuntu unixODBC build (~10x slower under many engine threads).
ARG WITH_POSTGRES=1
ARG WITH_MSSQL=1

RUN apt-get update \
 && apt-get -y install --no-install-recommends \
      ca-certificates curl gnupg apt-transport-https python3 python3-pip \
 && if [ "$WITH_POSTGRES" != 1 ] && [ "$WITH_MSSQL" != 1 ]; then \
        echo "ERROR: enable at least one of WITH_POSTGRES / WITH_MSSQL" >&2; exit 1; fi \
 && if [ "$WITH_POSTGRES" != 1 ]; then apt-get -y purge libpq5; fi \
 && if [ "$WITH_MSSQL" = 1 ]; then \
        curl -sSL -o /tmp/packages-microsoft-prod.deb https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb \
     && dpkg -i /tmp/packages-microsoft-prod.deb \
     && rm -f /tmp/packages-microsoft-prod.deb \
     && apt-get update \
     && ACCEPT_EULA=Y apt-get -y install --no-install-recommends msodbcsql18 unixodbc \
     && printf '[MSSQL]\nDriver = ODBC Driver 18 for SQL Server\nAutoTranslate = No\n' > /etc/odbc.ini ; fi \
 && apt-get -y clean \
 && rm -rf /var/lib/apt/lists/*

# Install the driver as a regular package (pulls the abstract `senzing` SDK from
# PyPI); the native binding `senzing_core` comes from the runtime image.
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY sz_file_combined_consumer ./sz_file_combined_consumer
RUN python3 -m pip install --break-system-packages --no-cache-dir . \
 && apt-get -y purge python3-pip && apt-get -y autoremove && rm -rf /root/.cache

ENV PYTHONPATH=/opt/senzing/er/sdk/python \
    PYTHONUNBUFFERED=1

# Memory mitigation (GDEV-4294): under sustained giant-component load glibc
# retains freed compare/scoring buffers at arena high-water and RSS balloons.
# Forcing >128 KB allocations through mmap returns them to the OS on free.
# Stopgap until the engine-side fix ships; unset if the per-alloc mmap cost
# outweighs the RSS benefit. Do NOT LD_PRELOAD jemalloc/tcmalloc (SIGSEGVs libSz).
ENV MALLOC_MMAP_THRESHOLD_=131072 \
    MALLOC_TRIM_THRESHOLD_=131072

USER 1001
ENTRYPOINT ["sz_file_combined_consumer"]
