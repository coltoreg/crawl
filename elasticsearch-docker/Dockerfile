FROM bitnami/elasticsearch:latest

USER root
RUN apt-get update && apt-get install -y curl

# 下載並安裝 IK Analysis 插件
RUN cd /opt/bitnami/elasticsearch/bin && \
    ./elasticsearch-plugin install https://get.infini.cloud/elasticsearch/analysis-ik/8.17.3 --batch

# 確保權限正確
RUN chown -R 1001:1001 /opt/bitnami/elasticsearch/

# 切回正確的使用者
USER 1001