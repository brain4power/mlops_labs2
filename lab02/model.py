import cudf
import pandas as pd
from clearml import Task
from pycaret.regression import *

task = Task.init(project_name="lab02", task_name="TPS_Jan_2022")

train = cudf.read_csv("data/train.csv", index_col="row_id").to_pandas()
test = cudf.read_csv("data/test.csv", index_col="row_id").to_pandas()

train["date"] = pd.to_datetime(train["date"])
train["year"] = train["date"].dt.year
train["month"] = train["date"].dt.month
train["day"] = train["date"].dt.day
train["dayofweek"] = train["date"].dt.dayofweek
train["dayofmonth"] = train["date"].dt.days_in_month
train["dayofyear"] = train["date"].dt.dayofyear
train["weekday"] = train["date"].dt.weekday

test["date"] = pd.to_datetime(test["date"])
test["year"] = test["date"].dt.year
test["month"] = test["date"].dt.month
test["day"] = test["date"].dt.day
test["dayofweek"] = test["date"].dt.dayofweek
test["dayofmonth"] = test["date"].dt.days_in_month
test["dayofyear"] = test["date"].dt.dayofyear
test["weekday"] = test["date"].dt.weekday

train.drop("date", axis=1, inplace=True)
test.drop("date", axis=1, inplace=True)

reg = setup(
    data=train,
    target="num_sold",
    data_split_shuffle=False,
    use_gpu=True,
    silent=True,
    n_jobs=-1,
)

N = 7
top = compare_models(sort="MAPE", n_select=N)

blend = blend_models(top)
# predict_model(blend)

final_blend = finalize_model(blend)
result = predict_model(final_blend)

unseen_predictions_blend = predict_model(final_blend, data=test)

sub = pd.DataFrame(
    list(zip(test.index, unseen_predictions_blend.Label)),
    columns=["row_id", "num_sold"],
)

task.upload_artifact(name="submission", artifact_object=sub)

# sub.to_csv("data/submission.csv", index=False)
