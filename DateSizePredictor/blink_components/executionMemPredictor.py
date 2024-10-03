import blink_components.executionMemoryParser as executionMemoryParser
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from sklearn.model_selection import LeaveOneOut


def prepareDataframe():
    sizeDict = executionMemoryParser.getExecutionMemories()
    scales = []
    sizes = []
    for scale, size in sizeDict.items():
        scales.append(scale)
        sizes.append(size)
    dataInput = {'scale': scales, 'sizes': sizes}
    df = pd.DataFrame(dataInput)
    return df
  
df = prepareDataframe()
   
def model(x, theta0, theta1):
    return theta0 + theta1 * x

x_data = df['scale'].values
y_data = df['sizes'].values

params, covariance = curve_fit(model, x_data, y_data, bounds=(0, [np.inf, np.inf]))
theta0, theta1 = params

loo = LeaveOneOut()
errors = []

for train_index, test_index in loo.split(df):
    train, test = df.iloc[train_index], df.iloc[test_index]
    params, _ = curve_fit(model, train['scale'], train['sizes'], bounds=(0, [np.inf, np.inf]))
    prediction = model(test['scale'].values, *params)
    error = np.sqrt(np.mean((prediction - test['sizes'].values) ** 2))
    errors.append(error)

average_rmse = np.mean(errors)
actual_scale = 1000
predicted_size = model(actual_scale, *params)

print(f"Fitted parameters: theta0 = {theta0}, theta1 = {theta1}")
print(f"Average RMSE: {average_rmse}")
print(f"Predicted size for scale {actual_scale}: {predicted_size}")


