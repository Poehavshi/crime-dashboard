import streamlit as st
import numpy as np
import pandas as pd
import catboost as cb
from sklearn.model_selection import train_test_split
import datetime

CLIMATE_TABLE_NAME = "climate"
CRIME_TABLE_NAME = "california_crime"
HOUSING_TABLE_NAME = "housing"


HOST = "postgres_streams"
PORT = "5432"
USER = "postgres"
PASSWORD = "postgres"
DB = "crime"


def create_time_series_plot(df, geoname, report_year):
    df = df[df['geoname']==geoname]
    df = df[df['reportyear']==report_year]


def create_dashboards():
    """
    Streamlit app creation
    """
    st.markdown("# Crime dashboard")

    conn_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}'

    from sqlalchemy import create_engine
    conn = create_engine(conn_string, echo=True).connect()

    st.markdown("## Climate")
    query = f"SELECT * FROM {CLIMATE_TABLE_NAME}"
    climate = pd.read_sql(query, con=conn).set_index(["state", "year"])
    st.dataframe(climate)

    st.markdown("## Crime")
    query = f"SELECT * FROM {CRIME_TABLE_NAME}"
    crime = pd.read_sql(query, con=conn)
    st.dataframe(crime)

    st.markdown("## Housing")
    query = f"SELECT * FROM {HOUSING_TABLE_NAME}"
    housing = pd.read_sql(query, con=conn)
    st.dataframe(housing)

    total = pd.merge(crime, housing, left_on=["reportyear", "geoname"], right_on=["reportyear", "regionname"])
    st.dataframe(total)

    total = total.drop(["_c0", "geotype", "regionname"], axis=1)
    st.dataframe(total)

    total.drop(["region_name", "geoname"], axis=1, inplace=True)
    y = total['rate']
    x = total.drop("rate", axis=1)

    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=5)
    model = cb.CatBoostRegressor(loss_function='RMSE')
    # model.fit(X_train, y_train)
    # model.save_model("model_name")

    model.load_model("model_name")

    y_predicted = model.predict(X_test)

    selected = st.number_input("Выберите пример из выборки", value=0, max_value=len(y_predicted))
    st.dataframe(X_test.iloc[[selected]])
    st.dataframe(y_test.iloc[[selected]])
    st.text(y_predicted[selected])


if __name__ == '__main__':
    create_dashboards()