import streamlit as st
import numpy as np
import pandas as pd
import catboost as cb
from sklearn.model_selection import train_test_split
import plotly.express as px
import pickle

import matplotlib.pyplot as plt


CLIMATE_TABLE_NAME = "climate"
CRIME_TABLE_NAME = "california_crime"
HOUSING_TABLE_NAME = "housing"

HOST = "postgres_streams"
PORT = "5432"
USER = "postgres"
PASSWORD = "postgres"
DB = "crime"


def create_dashboards():
    st.markdown("# Crime dashboard")

    conn_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}'

    from sqlalchemy import create_engine
    conn = create_engine(conn_string, echo=True).connect()

    st.markdown("## California climate by month")
    query = f"SELECT * FROM {CLIMATE_TABLE_NAME}" \
            f" WHERE state = 'California'" \
            f"AND year>1999"
    climate = pd.read_sql(query, con=conn).drop(["unique_key", "state"], axis=1)
    st.dataframe(climate)

    climate['date'] = pd.to_datetime(climate.drop(['averagetemperature', 'averagetemperatureuncertainty'], axis=1))
    fig = px.line(climate, x=['date'], y='averagetemperature')
    st.plotly_chart(fig)

    climate['month'] = 'climate' + climate['month'].astype(str)
    climate = pd.pivot_table(climate, values='averagetemperature', index=['year'], columns='month').reset_index()
    st.dataframe(climate)

    st.markdown("## California crime by year and city")
    query = f"SELECT * FROM {CRIME_TABLE_NAME}"
    crime = pd.read_sql(query, con=conn).drop("_c0", axis=1)
    st.dataframe(crime.drop(['geotypevalue', 'region_code'], axis=1))

    city_for_crime_plot = st.selectbox("Выберите город", crime['geoname'])
    filtered_crime = crime[crime['geoname'] == city_for_crime_plot]
    fig = px.line(filtered_crime, x='reportyear', y='rate')
    st.plotly_chart(fig)

    st.markdown("## Housing by month and city")
    query = f"SELECT * FROM {HOUSING_TABLE_NAME}"
    housing = pd.read_sql(query, con=conn)
    st.dataframe(housing)

    st.markdown("## Connection between avg_housing and rate")

    total = pd.merge(crime, housing, left_on=["reportyear", "geoname"], right_on=["reportyear", "regionname"])
    total = pd.merge(total, climate, left_on=["reportyear"], right_on=["year"])
    total = total.drop(["geotype", "regionname"], axis=1)

    total['avg_housing'] = sum([total[f'month{i+1}'] for i in range(12)])/12
    fig = px.scatter(total, x='avg_housing', y='rate')
    st.plotly_chart(fig)

    geoname = st.selectbox("Выберите город", total['geoname'].unique())
    filtered_total = total[total['geoname'] == geoname]
    filtered_total['avg_housing'] = sum([filtered_total[f'month{i+1}'] for i in range(12)])/12
    fig = px.scatter(filtered_total, x='avg_housing', y='rate')
    st.plotly_chart(fig)

    st.markdown("## Catboost model predictions")
    x = total.drop(["rate", "geoname", "region_name", 'avg_housing', "reportyear"], axis=1)
    y = total['rate']

    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=5)

    model = cb.CatBoostRegressor(loss_function='RMSE')
    is_trained_model = st.checkbox("Train new model")
    if is_trained_model:
        model.fit(X_train, y_train)
        model.save_model("model_name")
    model.load_model("model_name")

    y_predicted = model.predict(x)
    total['predict'] = y_predicted
    fig = px.scatter(total, x='predict', y='rate')
    st.plotly_chart(fig)

    y_predicted = model.predict(X_test)
    selected = st.number_input("Выберите пример из выборки", value=0, max_value=len(y_predicted))
    st.dataframe(X_test.iloc[[selected]])
    st.dataframe(y_test.iloc[[selected]])
    st.text(y_predicted[selected])
    if is_trained_model:
        feature_importance = model.feature_importances_
        with open("feature_importance.pickle", "wb") as file:
            pickle.dump(feature_importance, file)
    with open("feature_importance.pickle", "rb") as file:
        feature_importance = pickle.load(file)
    sorted_idx = np.argsort(feature_importance)
    fig = plt.figure(figsize=(12, 6))
    plt.barh(range(len(sorted_idx)), feature_importance[sorted_idx], align='center')
    plt.yticks(range(len(sorted_idx)), np.array(X_test.columns)[sorted_idx])
    st.pyplot(fig)


if __name__ == '__main__':
    create_dashboards()
