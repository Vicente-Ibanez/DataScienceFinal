import pandas as pd
import numpy as np
import requests
import json
from pandas import json_normalize


# Writing Introduction









# function to access and clean the data. Takes 3 parameters: the state, county, and site code
def get_site_data(state_code, county_code, site_code):
    # the url for the EPA's data is used as the basis for the url link. The API account information is placed after that. It is just missing the location codes
    url = "https://aqs.epa.gov/data/api/dailyData/bySite?email=vibanez@falcon.bentley.edu&key=ambergazelle82&param=44201&bdate=20210101&edate=20211231&"
    # the location codes are concatenated together with the specfications for the API. This is the part of the url that tells the API where the data source should be geographically
    site_selected = "state=" + state_code + "&county=" + county_code + "&site=" + site_code

    # the EPA's API is requested, with the url concatenated with the location url (site_selected)
    response = requests.get(url+site_selected)

    # the data sent back from the API is read as json data, because that is the format the API uses.
    data_aqi = response.json()
    # Since the data file is dictionaries in dictionaries, it is difficult to read. This (function json_normalize), "flattens"
    # the data, meaning that the dictionaries inside dictionaries are taken out and given their own column headings,
    # so that they can be accessed without using numerous paramteres listed one after another.
    data_temporary = json_normalize(data_aqi, "Data")

    # now only the columns we need are saved in the dataframe.
    data_temporary = data_temporary[["date_local", "aqi", "county"]]

    # Any rows missing aqi data are dropped, as the purpose of this data is for the aqi scores.
    data_temporary.drop(data_temporary[data_temporary['aqi'].isnull()].index, inplace = True)
    # The data is grouped by date, in order to stack duplicate dates, with the aqi averaged out for all rows with the same date.
    data_temporary = data_temporary.groupby("date_local", as_index = False).agg({"aqi":'mean', "county":"first"}).round(1)
    # this modified dataframe is returned
    return data_temporary

# request for a dataframe from the EPA's API, using the state code, county code, and site code discoved above.
df_2021_MA = get_site_data("25", "017", "0009")
df_2021_NY = get_site_data("36", "119", "2004")
print(df_2021_MA.head(5))
print(df_2021_NY.head(5))

# combining the data from mass and new york
df_aqi_2021 = pd.concat([df_2021_MA,df_2021_NY])
# renaming the columns
df_aqi_2021.rename(columns={"date_local": "Date",'aqi': 'AQI', "county": "county Name"}, inplace=True)

# changing the date column to be date time data type in order to reformat the date
df_aqi_2021['Date'] = pd.to_datetime(df_aqi_2021.Date)
# reformatting the date to month/day/year
df_aqi_2021['Date'] = df_aqi_2021['Date'].dt.strftime("%m/%d/%y")

# printing the data to observe the columns and data
print(df_aqi_2021.head())
print(df_aqi_2021.tail())

# air quality index data for 2020, 2019, and 2018.
df_aqi_2020 = pd.read_csv("daily_aqi_by_county_2020.csv")
df_aqi_2019 = pd.read_csv("daily_aqi_by_county_2019.csv")
df_aqi_2018 = pd.read_csv("daily_aqi_by_county_2018.csv")

# looking at the columns and entries
print(df_aqi_2020.head())
print(df_aqi_2019.head())
print(df_aqi_2018.head())

# importing the running data
df_running = pd.read_csv("Activities.csv")
# looking at the running dat aheads
print(df_running.head())

# concat air quality data frames from 2020, 2019, and 2018.
air_quality = [df_aqi_2020, df_aqi_2019, df_aqi_2018]
df_quality = pd.concat(air_quality)

print(df_quality.head())

def county_selector(df):
    df = df.loc[((df["county Name"] == "Westchester") & (df["State Name"] == "New York")) | ((df["county Name"] == "Middlesex") & (df["State Name"] == "Massachusetts"))]
    return df

# Selec the county
df_quality = county_selector(df_quality)
# combine the tw oair quality data frames
df_quality_data = pd.concat([df_aqi_2021, df_quality])

# print the unique counties
print(df_quality_data["county Name"].unique())
print(df_quality_data.head(2))
print(df_quality_data.tail(2))


def unnecessary_column_remover(df):
    df = df.drop(columns=["State Code", "County Code", "Defining Site",
                          "Number of Sites Reporting", "Defining Parameter", "State Name"])
    return df

df_quality_data = unnecessary_column_remover(df_quality_data)

print(df_quality_data.head())

df_running = df_running.drop(columns=["Favorite", "Surface Interval", "Decompression", "Min Temp", "Max Temp", "Min Elevation", "Max Elevation", "Activity Type",
                                      "Calories", "Elev Gain", "Elev Loss", "Avg Vertical Ratio", "Avg Vertical Oscillation", "Grit", "Flow", "Dive Time" , "Training Stress Score®"])
print(df_running.head())


df_running['Date'] = pd.to_datetime(df_running.Date)
print(df_running["Date"].dtype)

df_running["Date"] = df_running['Date'].dt.strftime("%m/%d/%y")
print(df_running.head())

print(df_running["Title"].unique())


df_running["Title"] = df_running["Title"].replace({"Ossining Running": "Westchester","Croton-on-Hudson Running": "Westchester",
                                                   "Mt Pleasant Running": "Westchester","Somers Running": "Westchester",
                                                   "Ossining - Running": "Westchester", "New York Running": "Westchester",
                                                   "White Plains Running":"Westchester","Ithaca Running":"Westchester",
                                                   "Cortlandt Running": "Westchester", "Westport Running":"Middlesex",
                                                   "Lexington Running": "Middlesex", "Waltham Running": "Middlesex"})
print(df_running.head(20))

df_running.rename(columns={"Title":"county Name"}, inplace=True)

x = 0

for run in df_running["county Name"]:
    y = 0
    if run != "Running":
        previous_title = run
    for run2 in df_running["county Name"]:
        if run2 == "Running":
            if y == x+1:
                # replace row with "previous_title"
                df_running.at[y,'county Name']= previous_title

        y = y+1

    x = x +1
print(df_running["county Name"].head(20))

df_running['Date'] = pd.to_datetime(df_running.Date)
df_quality_data['Date'] = pd.to_datetime(df_quality_data.Date)
print(df_running["Date"].dtype)
print(df_quality_data["Date"].dtype)


df_merge = df_running.merge(df_quality_data, left_on=["Date", "county Name"], right_on=["Date", "county Name"], how="left")
print(df_merge.head())


# Ensure that each time column is in the format for larger times followed by smaller times and is the time data type
df_merge["Avg Pace"] = (pd.to_datetime(df_merge["Avg Pace"].str.strip(), format='%M:%S'))
print(df_merge["Avg Pace"].dtype)

df_merge["Time"] = (pd.to_datetime(df_merge["Time"].str.strip([":","."]), format='%H:%M:%S.%f'))
print(df_merge["Time"].dtype)

df_merge["Best Pace"] = (pd.to_datetime(df_merge["Best Pace"].str.strip([":","."]), format='%M:%S.%f'))
print(df_merge["Best Pace"].dtype)

df_merge["Avg_Pace_Min"] = df_merge["Avg Pace"].dt.strftime("%M.%S")
df_merge["Avg_Pace_Min"] = df_merge["Avg_Pace_Min"].astype("float")
print(df_merge["Avg_Pace_Min"].head())
print(df_merge["Avg_Pace_Min"].dtype)


df_merge["Date"] = pd.to_datetime(df_merge["Date"])
print(df_merge["Date"].dtype)

df_merge["Distance"] = df_merge["Distance"].astype("float")
print(df_merge["Distance"].dtype)

df_merge["Avg Run Cadence"] = df_merge["Avg Run Cadence"].astype("int")
print(df_merge["Avg Run Cadence"].dtype)

df_merge["Avg Stride Length"] = df_merge["Avg Stride Length"].astype("int")
print(df_merge["Avg Stride Length"].dtype)

print(df_merge.isnull().sum())

df_merge.dropna(subset=['AQI'], inplace=True)
print(df_merge.isnull().sum())

import streamlit as st
import matplotlib.pyplot as plt
import altair as alt

data = df_merge
##############################
# stream lit stuff
##############################

st.title("Arushi and Vicente's Data Science Final Project")



plt.hist(data["AQI"])
plt.xlabel('AQI Scores')
plt.ylabel("Frequency of Score")
plt.title('Distrubtion of AQI scores')
plt.show()




###########
st.write("Distance and Air Quality Scatter Plot")
# defining the range for the graph
yrange= [0,25]
# creating the chart, with circle marks, an x axis of AQI, and y of distance.
chart=alt.Chart(data).mark_circle().encode(x='AQI',y='Distance',tooltip=['AQI','Distance'])
# plots the chart to streamlit.
st.altair_chart(chart)
####################



# Creates a scatterplot with AQI for x, and distance for y. This is for the report.
plt.scatter(data['AQI'],data['Distance'])
# the axis and title labels are adjusted accordingly
plt.title("Distance and Air Quality Scatter Plot")
plt.xlabel("AQI")
plt.ylabel("Distance in Miles")
plt.figure(figsize=((10,8)))
plt.show()
#######################
# import the package to perform the correlation coefficient calculations
from scipy.stats import pearsonr
# saves the AQI column as numeric leaving non numbers
data["AQI"] = np.nan_to_num(data["AQI"])
distance_dat = data["Distance"]
air_quality_dat = data["AQI"]

# calculates correlation coefficient between distance and AQI
cor_disatnce_qui = pearsonr(distance_dat, air_quality_dat)
# Prints results
print(cor_disatnce_qui)
st.write(cor_disatnce_qui)
st.write("Correlation Coefficient")
########################
st.write("Cadence Across Air Quality")
# defining the range for the graph
# creating the chart, with circle marks, an x axis of AQI, and y of distance.
chart=alt.Chart(data).mark_circle().encode(x='AQI',y='Avg Run Cadence',tooltip=['AQI','Avg Run Cadence']).configure_mark(
    opacity=0.2,
    color='green')
# plots the chart to streamlit.
st.altair_chart(chart)
########################
# saves the AQI column as numeric leaving non numbers
data["AQI"] = np.nan_to_num(data["AQI"])
# saves the columns as variables to put into the function
average_cadence = data["Avg Run Cadence"]
air_quality_dat = data["AQI"]
# the function to calculate the correlation coefficient
cor_cadence_qui = pearsonr(average_cadence, air_quality_dat)
print(cor_cadence_qui)
st.write(cor_cadence_qui)
st.write("Correlation Coefficient")

# creates plot on streamlit
st.write("Speed and Air Quality")
chart=alt.Chart(data).mark_circle().encode(x='AQI',y='Avg_Pace_Min',tooltip=['AQI','Avg_Pace_Min']).configure_mark(
    opacity=0.2,
    color='orange')
# Shows the plot on streamlit
st.altair_chart(chart)
# creates the labels for y, x, and title
plt.title("Speed and Air Quality")
plt.ylabel("Average Pace in Miles Per Hour")
plt.xlabel("AQI score")
# creates scatterplot to see in jupter
plt.scatter(data['AQI'],data['Avg_Pace_Min'], color="orange")
# controls figure size
plt.figure(figsize=((20,16)))
# st.pyplot()
# shows figure in jupyter
plt.show()

data["AQI"] = np.nan_to_num(data["AQI"])
average_stride_length = data["Avg_Pace_Min"]
air_quality_dat = data["AQI"]

# the function to calculate the correlation coefficient
cor_avg_stri_qui = pearsonr(average_stride_length, air_quality_dat)
print(cor_avg_stri_qui)
st.write(cor_avg_stri_qui)
st.write("Correlation Coefficient")






plt.hist(data["AQI"])
plt.xlabel('AQI Scores')
plt.ylabel("Frequency of Score")
plt.title('Distrubtion of AQI scores')
plt.show()

########











# creates a scatter plot with AQI and avg Run Cadence
plt.plot(data["AQI"], data["Avg Run Cadence"], 'o', color='green', markersize=5)
# limits y axis to everything above 110, to help see the data better
plt.ylim(110)
# creates labels
plt.title("Cadence Across Air Qualities")
plt.xlabel("Air Quailty Index")
plt.ylabel("Average Cadence")
# shows the plot
plt.show()


def introduction()


def mainTest():
    introduction()
    
    
mainTest()    
    


