FROM python:3.8
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . .
# Expose the Flask port (optional, as it's defined in app.py)
EXPOSE $PORT

# Set environment variable for Flask port
ENV PORT=5000
CMD ["python", "app.py"]