# Start from the official python base image
FROM python:3.9

# Set the current working directory to /code
WORKDIR /code

# Copy the requirements.txt file into the container at /code
COPY ./requirements.txt /code/requirements.txt

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the models directory into the container
COPY ./models /code/models

# Copy the entire app directory into the container
COPY ./app /code/app

# Expose the port the app runs on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]


