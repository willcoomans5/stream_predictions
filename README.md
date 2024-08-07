# Stream Predictions for Tracks on Digital Streaming Platforms (DSPs)

## Project Overview

This project aims to create several models designed to predict song streaming trends. We have defined two models that predict a song's streams during its next full week and one model that predicts a song's streams during its third week. We leverage algorithms such as linear regression, random forests, and support vector machines. We also experiment with various features such as social media data in social_media_preds.ipynb, though those models have not been stored.

We have also build a RESTful API using FastAPI, which provides endpoints for each of our models' predictions. The API has been containerized with Docker.

### Dataset

- `csv_files/snowflake_data.csv`
- `csv_files/alltime_socials.csv`
- `csv_files/three_week_data.csv`
  
In the music world, streaming weeks start on Friday and end on Thursday. Thus, each of our csv files only contains records from Thursday. This is to ensure our target variable captures a full weeks worth of data.

### Target Variable

- **One Week Predictions**: The number of streams a song receives during its next full week. If we have a record for a song from Thursday, August 1, 2024, our target variable is the song's total streams from August 2 - 8.
- **Three Week Predictions**: The number of streams a song receives during its third full week from today. If we have a record for a song from Thursday, August 1, 2024, our target variable is the song's total streams from August 16 - 22.

## Data Pipeline

The data pipeline performs:
- Cleansing
- Missing value interpolation
- Transformations
- Scaling
- PCA

## Models

### One-Week Trend Prediction

For predicting one-week trends:
- **Linear Regression Model**
- **Random Forest Model**

### Three-Week Trend Prediction

For predicting three-week trends:
- **Linear Regression Model**

## RESTful API

A RESTful API was built using FastAPI to provide endpoints for each of our defined models. This API allows users to get predictions for both one-week and three-week trends by sending appropriate data to the endpoints.

### Endpoints

- `/oneweek/predict`: Predict one-week streaming trends.
- `/threeweek/predict`: Predict three-week streaming trends.

### Example Request

**One-Week Prediction:**
```bash
curl -X POST "http://localhost:8000/oneweek/predict" -H "Content-Type: application/json" -d '{
  "popularity": 80,
  "days_since_release": 100,
  "day_0": 1000,
  "day_1": 900,
  "day_2": 800,
  "day_3": 750,
  "day_4": 700,
  "day_5": 650,
  "day_6": 600
}'
```

## Docker

The Dockerfile contains all of the information for the Docker image. I have pushed the image to the repository `willcoomans/test-image` on Docker Hub. To run the container:

Pull the image from Docker Hub
``` bash
docker pull willcoomans/test-image:latest
```

Run the container on your local machine
```bash
docker run -p 8000:8000 willcoomans/test-image:latest
```

Open the URL `http://localhost:8000` in your browser. To interact with the API directly, you may access `http://localhost8000/docs`



