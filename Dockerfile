FROM python:latest
LABEL anonymous="true"
LABEL name="pythonworkitemagent"
LABEL description="Python template for processing workitems of a workitem queue in OpenCore"
COPY . /app
WORKDIR /app
RUN pip install -t . -r requirements.txt
ENV PYTHONPATH=/app
EXPOSE 3000
ENTRYPOINT ["python", "/app/main.py"]