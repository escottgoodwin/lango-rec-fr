# python runtime
FROM python:3.7-slim

# working directory
WORKDIR /app

# copy current directory into the container
ADD . /app

# install requirements
RUN pip install -r requirements.txt

# make port 8000 available to the world outside

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 serve:application