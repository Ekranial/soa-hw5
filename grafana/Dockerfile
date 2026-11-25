FROM grafana/grafana:11.2.2

# Плагин ClickHouse и provisioning встроены в образ: без ручной настройки и без bind mount
ENV GF_PATHS_PLUGINS=/var/lib/grafana-plugins
USER root
RUN mkdir -p /var/lib/grafana-plugins \
    && grafana cli --pluginsDir /var/lib/grafana-plugins plugins install grafana-clickhouse-datasource \
    && chown -R grafana:root /var/lib/grafana-plugins
USER grafana

COPY grafana/provisioning/ /etc/grafana/provisioning/
COPY grafana/dashboards/ /etc/grafana/dashboards/
