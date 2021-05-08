FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ENV KAFKA_ADDRESS 'localhost:9092'
ADD deployer.py .
CMD ["python", "-u","deployer.py"]
