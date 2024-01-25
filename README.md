# NHL-Player-Prediction-System
A ML system used for predicting NHL player goals and assists in a season. There is one notebook for predicting goals, goals_predictive_model.ipynb, and another for predicting assists, assists_predictive_model.ipynb. Both notebooks use the data from the player_data.csv file which was created by the raw_data_preprocessing.py file. The .py file merges all csv files from the raw_data folder and includes the data engineering conducted for creating the player_data.csv file.

## About the System
Each system is nearly identical in code, the only significant difference is the target variable that is used in each. This system used 6 different models, support vector regression, linear regression, elasticNet, gradient boosting regression, random forest regression, and a neural network to determine the best-performing model after hyperparameter tuning.

**Evaluation Metrics:** R squared, mae, and rmse were used to evaluate model performance during training. I also created a function in each notebook, calculate_accuracy, that calculates the accuracy of the models with a tolerance. Since accuracy is not used as a metric in regression problems, this function doesn't look for exact predictions in goals and assists. Instead, it checks to see if the predicted number of goals/assists is within a specified tolerance. For example, if a player has 20 goals scored and the tolerance is 5 (+/-5 goals of the actual value), then any predicted value within 15 and 25 would be considered correct when measuring accuracy. 

## Best Model Performance
Before reading this section, I'd encourage readers to understand what tolerance means in this context by reading the **Evaluation Metrics** explanation in **About the System**.

**Goals:** The best-performing model was the random forest regressor. When using the test dataset, it achieved a 99.51% accuracy with a tolerance of 5, a 98.78% accuracy with a tolerance of 3, and a 92.44% accuracy with a tolerance of 1. Additionally, R2=0.99, mae=1.13, rmse=1.06. When using the whole dataset and a tolerance of 1, it achieved a 97.36% accuracy.

**Assists:** Again, the best-performing model was the random forest regressor. Using the test dataset, it achieved a 78.9% accuracy with a tolerance of 5. No further tolerances were tested because this tolerance was already quite low. As for the other metrics on the test set, R2=0.83, mae=26.14, rmse=5.11. However, when predicting accuracy on the whole dataset, the model achieved a 92.22% accuracy with a tolerance of 5 and an 83.39% accuracy with a tolerance of 3. The much higher accuracy in the overall dataset with a tolerance of 5 might indicate that the model is overfitting/memorizing the data rather than generalizing it well, so that is a potential issue to look into.

## Issues
There are no issues when running the system but that doesn't mean it is without its flaws. 
1. The player statistics used as independent variables for the prediction system are very difficult to find and the only site I managed to see most of them on was moneypuck.com. I think this would make it difficult to make accurate goal/assist forecasts prior to a season due to the scarcity of these stats.
2. Unless there is an accurate model for predicting all independent variables for an upcoming season, this system would probably only be useful somewhere around halfway through the season.
3. I suspect this model is pretty bad at identifying breakout players which is what I think most teams would be interested in a model like this for, but perhaps it can help in fantasy hockey leagues.
4. There are other issues I have thought of but the most important ones have been explained and I want to move on to other projects and wrap this README file up!
