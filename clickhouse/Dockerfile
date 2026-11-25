FROM clickhouse/clickhouse-server:24.8

# Конфиги встроены в образ (без bind mount — проект поднимается одинаково на любой машине)
COPY clickhouse/config.d/ /etc/clickhouse-server/config.d/
COPY clickhouse/users.d/ /etc/clickhouse-server/users.d/
