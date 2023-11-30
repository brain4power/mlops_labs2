import logging

import numpy as np
import pandas as pd
from sklearn.compose import make_column_transformer
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error as mse
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split

# Project
import mlflow

logger = logging.getLogger(__name__)


TEST_SIZE = 0.3
RANDOM_STATE = 26
DATA_URI = "https://raw.githubusercontent.com/tidyverse/ggplot2/main/data-raw/diamonds.csv"


def _to_numpy(df: pd.DataFrame) -> np.ndarray:
    y = df.values
    y = np.expand_dims(y, axis=1)
    return y


def _separate_target_data(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    y = df[column_name]
    df.drop(columns=[column_name], inplace=True)
    return y


def _split_features(df: pd.DataFrame) -> tuple:
    column_names = df.columns.to_list()
    cat_columns = []
    num_columns = []

    for column_name in column_names:
        if (df[column_name].dtypes == "int64") or (df[column_name].dtypes == "float64"):
            num_columns += [column_name]
        else:
            cat_columns += [column_name]
    return num_columns, cat_columns


def read_data(**kwargs):
    df = pd.read_csv(DATA_URI)
    data_train, data_test = train_test_split(df, test_size=TEST_SIZE, random_state=RANDOM_STATE)
    logger.info("Data reading successfully done")
    return data_train, data_test


def preprocess_data(**kwargs) -> tuple:
    ti = kwargs["ti"]
    x_train, x_test = ti.xcom_pull(task_ids="read_data")

    y_train = _to_numpy(_separate_target_data(x_train, "price"))
    y_test = _to_numpy(_separate_target_data(x_test, "price"))

    num_cols, cat_cols = _split_features(x_train)

    preprocessors = make_column_transformer(
        (StandardScaler(), num_cols), (OneHotEncoder(drop="if_binary", handle_unknown="ignore"), cat_cols)
    )

    x_train = preprocessors.fit_transform(x_train)
    x_test = preprocessors.transform(x_test)

    return x_train, y_train, x_test, y_test


def prepare_model(**kwargs):
    ti = kwargs["ti"]
    x_train, y_train, _, _ = ti.xcom_pull(task_ids="preprocess_data")

    params = dict(
        fit_intercept=True,
        n_jobs=-1,
    )
    model = LinearRegression(**params)
    model.fit(x_train, y_train)

    # mlflow
    mlflow.set_tracking_uri("http://mlflow_server:5000")
    try:
        # Creating an experiment
        mlflow.create_experiment("demo_data_process_flow")
    except Exception as e:
        logging.info(f"Got exception when mlflow.create_experiment: {e}")
    # Setting the environment with the created experiment
    experiment = mlflow.set_experiment("demo_data_process_flow")
    with mlflow.start_run(experiment_id=experiment.experiment_id) as run:
        mlflow.log_params(params)

        result = mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="models",
            # signature=signature,
            registered_model_name="LinearRegression-reg-model",
        )
        return result.model_uri


def check_model(**kwargs):
    ti = kwargs["ti"]
    _, _, x_test, y_test = ti.xcom_pull(task_ids="preprocess_data")
    model_uri = ti.xcom_pull(task_ids="prepare_model")
    logger.info(f"model_uri: {model_uri}")

    mlflow.set_tracking_uri("http://mlflow_server:5000")

    model = mlflow.pyfunc.load_model(model_uri=model_uri)

    y_pred = model.predict(x_test)

    experiment = mlflow.set_experiment("demo_data_process_flow_check_model")
    with mlflow.start_run(experiment_id=experiment.experiment_id) as run:
        mlflow.log_metrics(
            {"MSE": mse(y_test, y_pred), "RMSE": mse(y_test, y_pred, squared=False), "R2": r2_score(y_test, y_pred)}
        )
