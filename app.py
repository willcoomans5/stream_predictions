import nbimporter
from song import Song
import uvicorn
from fastapi import FastAPI
import pandas as pd
import numpy as np
import joblib

app = FastAPI()

# the decorator @app.get('/') indicates that the function main() is in charge of handling GET requests that go to the path '/'
one_week_lr = (joblib.load('models/lr_streampreds.joblib')).best_estimator_
one_week_rfr = (joblib.load('models/rfr_streampreds.joblib')).best_estimator_
three_week_lr = (joblib.load('models/three_week_preds.joblib')).best_estimator_


@app.get('/')
def index():
    return {"message": "Stream predictions"}


def oneweek_preprocess(request: Song):
    song_df = pd.DataFrame([request.dict()])

    for i in range(7):
        # Add 1 to prevent taking the log of 0
        song_df[f'day_{i}'] += 1
        song_df[f'log day_{i}'] = np.log(song_df[f'day_{i}'])

    # Add 1 to prevent taking the log of 0
    song_df['log days_since_release'] = np.log(
        song_df['days_since_release'] + 1)

    song_df['1_day_%_change'] = 100 * \
        (song_df['day_0'] - song_df['day_1']) / song_df['day_1']
    song_df['3_day_%_change'] = 100 * \
        (song_df['day_0'] - song_df['day_3']) / song_df['day_3']
    song_df['6_day_%_change'] = 100 * \
        (song_df['day_0'] - song_df['day_6']) / song_df['day_6']

    features = ['popularity', 'log days_since_release', 'log day_0', 'log day_1',
                'log day_2', 'log day_3', 'log day_4', 'log day_5', 'log day_6',
                '1_day_%_change', '3_day_%_change', '6_day_%_change']

    return song_df[features]


def threeweek_preprocess(data: Song):
    song_data = data.dict()
    for i in range(21):
        song_data[f'day_{i}'] = np.log(song_data[f'day_{i}'] + 1)
    song_data['days_since_release'] = np.log(song_data['days_since_release'])
    return pd.DataFrame({key: [value] for key, value in song_data.items})


@app.post('/oneweek/predict')
def predict(data: Song):
    df = oneweek_preprocess(data)
    lr_preds = np.e**(one_week_lr.predict(df)[0])
    rfr_preds = np.e**(one_week_rfr.predict(df)[0])
    return {"lr_preds": lr_preds, "rfr_preds": rfr_preds}


@app.post('/threeweek/predict')
def predict(data: Song):
    df = oneweek_preprocess(data)
    lr_preds = np.e**(one_week_lr.predict(df)[0])
    rfr_preds = np.e**(one_week_rfr.predict(df)[0])
    return {"lr_preds": lr_preds, "rfr_preds": rfr_preds}
