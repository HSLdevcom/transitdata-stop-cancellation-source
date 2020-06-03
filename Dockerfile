FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-stop-cancellation-source.jar /usr/app/transitdata-stop-cancellation-source.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh
CMD ["/start-application.sh"]
