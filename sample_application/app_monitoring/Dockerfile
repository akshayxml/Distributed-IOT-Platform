FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ENV KAFKA_ADDRESS '20.51.210.82:9092'
ADD app_monitoring.py .
ADD server_details.json .
CMD ["python", "-u","app_monitoring.py"]
