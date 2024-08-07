from fastapi.testclient import TestClient
from app import app  # Adjust the import based on your app structure
# Adjust based on your actual model imports
from song import OneWeekSong, ThreeWeekSong
import pytest

client = TestClient(app)

# Test for the index route


def test_index():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Stream predictions"}

# Test for one week prediction


def test_oneweek_predict():
    data = OneWeekSong(
        popularity=50,
        days_since_release=10,
        day_0=100,
        day_1=90,
        day_2=80,
        day_3=70,
        day_4=60,
        day_5=50,
        day_6=40
    )

    response = client.post("/oneweek/predict", json=data.model_dump())
    assert response.status_code == 200
    assert "lr_preds" in response.json()
    assert "rfr_preds" in response.json()

# Test for three week prediction


def test_threeweek_predict():
    data = ThreeWeekSong(
        popularity=50,
        days_since_release=10,
        day_0=100,
        day_1=90,
        day_2=80,
        day_3=70,
        day_4=60,
        day_5=50,
        day_6=40,
        day_7=30,
        day_8=20,
        day_9=10,
        day_10=5,
        day_11=3,
        day_12=2,
        day_13=1,
        day_14=0,
        day_15=0,
        day_16=0,
        day_17=0,
        day_18=0,
        day_19=0,
        day_20=0
    )
    response = client.post("/threeweek/predict", json=data.model_dump())
    assert response.status_code == 200
    assert "three_week_preds" in response.json()

# Test for invalid input (negative values)


def test_oneweek_predict_invalid():
    data = OneWeekSong(
        popularity=-10,
        days_since_release=10,
        day_0=100,
        day_1=90,
        day_2=80,
        day_3=70,
        day_4=60,
        day_5=50,
        day_6=40
    )
    with pytest.raises(ValueError) as exc_info:
        client.post("/oneweek/predict", json=data.model_dump())


def test_threeweek_predict_invalid():
    data = ThreeWeekSong(
        popularity=50,
        days_since_release=10,
        day_0=100,
        day_1=90,
        day_2=80,
        day_3=70,
        day_4=60,
        day_5=50,
        day_6=40,
        day_7=30,
        day_8=20,
        day_9=10,
        day_10=5,
        day_11=3,
        day_12=2,
        day_13=1,
        day_14=0,
        day_15=0,
        day_16=0,
        day_17=0,
        day_18=0,
        day_19=0,
        day_20=-1  # Invalid negative value
    )
    with pytest.raises(ValueError) as exc_info:
        client.post("/threeweek/predict", json=data.model_dump())
