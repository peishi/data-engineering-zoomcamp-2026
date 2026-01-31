# Run the following command in terminal directly since it is bad practice to put your secrets in your Dockerfile, but I wrote the Dockerfile for practice

# docker run -it --rm \
#   -e POSTGRES_USER="root" \
#   -e POSTGRES_PASSWORD="root" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v ny_taxi_postgres_data:/var/lib/postgresql \
#   -p 5432:5432 \
#   postgres:18


FROM postgres:18

# ENV POSTGRES_USER="root"
# ENV POSTGRES_PASSWORD="root"
# ENV POSTGRES_DB="ny_taxi"

# VOLUME ny_taxi_postgres_data:/var/lib/postgresql

# EXPOSE 5432 