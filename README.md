# Crime dashboard
## Description
It's a small data engineering project to analyze relation between crime, housing and climate data. Main goal of this project is 
creating of small data analytics tool to find safe and comfortable neighborhood to live due or to understand when and where you need to be ready for police call
to statistics and modern technologies

Some common use cases to analyse with this project:

- How is the crime rate in a particular case based on weather?
  - Are there more crimes in summer or winter?
- How is the crime rate based on properties prices?
- Are property rates higher in colder climates or warmer climates?

## Datasets

- Crime [[Violent crime rate 2020](https://data.world/chhs/99bc1fea-c55c-4377-bad8-f00832fd195d)]
- Climate [[Global climate Change Data 2016](https://data.world/data-society/global-climate-change-data)] [[California climate zones](https://data.amerigeoss.org/dataset/california-building-climate-zones)]
- Housing data [[Zillow home values index](https://www.zillow.com/research/data/)]
- Maybe some additional data scraped from websites in future

## How to run

The only one to launch whole project is docker-compose, so to run project you need:

1 Build all images for docker-compose:

`docker-compose build`

2 Run project

`docker-compose up`