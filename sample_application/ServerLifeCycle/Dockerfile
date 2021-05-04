FROM python:3
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
ADD waterContent.py .
ADD platform_libfile.py .
CMD ["python", "-u","waterContent.py","606a2577a1627ca61870e6cb"]