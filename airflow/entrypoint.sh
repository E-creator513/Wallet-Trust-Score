#!/bin/bash


airflow db init


if ! airflow users list | grep -q "^admin"; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
else
    echo "Admin user already exists, skipping creation."
fi


if ! airflow users list | grep -q "^user2"; then
    echo "Creating user2..."
    airflow users create \
        --username user2 \
        --firstname User \
        --lastname Two \
        --role Admin \
        --email user2@example.com \
        --password mypassword123
else
    echo "User2 already exists, skipping creation."
fi

# Запуск Airflow
exec airflow "$@"
