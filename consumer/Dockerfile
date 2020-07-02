FROM python:3.7-slim AS PyBase

ENV PYTHONUNBUFFERED 1
# ENV USEPORT 5000
# EXPOSE $USEPORT
# # Install underlying dependencies

# RUN apt update
# RUN apt install -y unixodbc-dev
# RUN apt install -y gcc g++

RUN pip install pipenv
COPY /consumer/ /app/


WORKDIR  /app/
RUN pipenv install --system --deploy --ignore-pipfile
RUN ls -la
# CMD uvicorn main:app --host 0.0.0.0 --port $USEPORT --workers $(nproc --all)
CMD python UserEventAggregator.py