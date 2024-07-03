# Stream Predictions for Tracks on Digital Streaming Platforms (DSPs)

In this project, we predict the number of streams a song will have after it's next full week of streaming. In the music world, streaming weeks start on Friday and end on Thursday. In our query, we have only included records after January 1, 2024 to limit the size of our dataset. Additionally, our dataset only includes streaming data recorded on Thursdays. This is so that each record captures an entire weeks worth of data, i.e. Friday, Saturday, Sunday, Monday, Tuesday, Wednesday, and Thursday (day of the record).

The target variable that we will predict is the number of streams a song will have in it's next full week of streaming. For example, if we are looking at a record for a song from Thursday, July 4, 2024, our goal is to predict the sum of streams that song received from July 5 - 11.

We have defined a data pipeline that performs all necessary cleansing, transformations, and missing value interpolation. Then, we train two separate Linear Regression and Random Forest Regressor Models. Finally, we compare the results of our two models.


