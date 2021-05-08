FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ENV KAFKA_ADDRESS 'localhost:9092'
ADD scheduler.py .
CMD ["python", "-u","scheduler.py"]
