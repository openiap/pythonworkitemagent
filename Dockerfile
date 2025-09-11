FROM python:latest
LABEL anonymous="true"
LABEL name="pythonworkitemagent"
LABEL non_web="true"
LABEL idle_timeout="-1"
COPY . /app
WORKDIR /app
RUN pip install -t . -r requirements.txt
ENV PYTHONPATH=/app
EXPOSE 3000
ENTRYPOINT ["python", "/app/main.py"]