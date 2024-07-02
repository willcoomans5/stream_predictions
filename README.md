# Stream Predictions for Tracks on Digital Streaming Platforms (DSPs)

In this project, we are predicting the number of streams a song will have in one week using various machine learning models (linear regression, random forests, LSTMs).

We have defined a data pipeline that performs all necessary cleansing, transformations, and missing value interpolation. Then, we define a ML pipeline, which when passed into the GridSearchCV instance, returns the optimal model.
