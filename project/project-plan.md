# Project Plan

## Title
<!-- Give your project a short title. -->
Development of driving accident numbers among young drivers with the introduction of BF17 (Begleitetes Fahren ab 17)

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
- How did the numbers of driving accidents caused by young drivers in the "Probezeit" develop with the introduction of BF17 in the year of 2011?
- Especially: Has the accident number been noticeably reduced, or has it not really changed at all?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
In the year 2011 BF17 (Begleitetes Fahren ab 17) was introduced in whole Germany. Young people participating in this program can get a driving license already at the age of 17, one year before the regular permission age, and can drive as long as they are accompanied by an adult companion.\
The benefit of this program was said to be a reduction of driving accidents because young drivers get more expertise together with a experienced companion.

In this project I try to find out if this hope can be confirmed by looking at the number of driving accidents among young drivers in combination with the numbers of BF17 participants in the last years. The question is if a correlation can be found in those two numbers: Has a increasing number of BF17 participants lead to less driving accidents among young people?

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource 1: Number of driving permissions on probation over the last years
* Metadata URL: https://www.kba.de/DE/Statistik/Kraftfahrer/Fahrerlaubnisse/FahrerlaubnisProbe/2023/2023_fe_fap_tabellen.html?nn=3511180&fromStatistic=3511180&yearFilter=2023&fromStatistic=3511180&yearFilter=2023
* Data URL: https://www.kba.de/DE/Statistik/Kraftfahrer/Fahrerlaubnisse/FahrerlaubnisProbe/fahrerlaubnisprobe_node.html
* Data Type: XLSX

Detailed statistics of driving permissions on probation (= new driving licenses) in Germany of the Kraftfahrbundesamt.
Grouped by year, license type and age, so that the numbers of car driving permissions with 17 (= BF17) can be calculated.


### Datasource 2: Number of driving accidents over the last years
* Metadata URL: - (included in data file)
* Data URL: https://www-genesis.destatis.de/genesis//online?operation=table&code=46241-0011&bypass=true&levelindex=1&levelid=1698497814414#abreadcrumb
* Data Type: XLSX

Number of driving accidents and information about the people who caused the accidents in Germany grouped by year.

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Create data pipeline to load online data sources [https://github.com/Domi020/made/issues/1]
    1. Create script to read XLSX files [https://github.com/Domi020/made/issues/2]
    2. Create script to download files [https://github.com/Domi020/made/issues/3]
2. Merge Excel files to a single data frame object per data source [https://github.com/Domi020/made/issues/6]
    1. Select correct Excel files [https://github.com/Domi020/made/issues/4]
    2. Combine multiple Excel sheets to one data frame [https://github.com/Domi020/made/issues/5]
3. Prepare data for visualization and metrics [https://github.com/Domi020/made/issues/9]
    1. Analyse needed attributes [https://github.com/Domi020/made/issues/7]
    2. Data cleansing and quality check [https://github.com/Domi020/made/issues/8]
4. Create test suite [https://github.com/Domi020/made/issues/14]
    1. Implement test for file download [https://github.com/Domi020/made/issues/10]
    2. Implement test for Excel loading [https://github.com/Domi020/made/issues/11]
    3. Implement test for data preparation [https://github.com/Domi020/made/issues/12]
    4. Implement system test [https://github.com/Domi020/made/issues/13]
5. Perform data analysis by creating proper diagrams and metrics [https://github.com/Domi020/made/issues/15]
6. Create final report [https://github.com/Domi020/made/issues/16]
7. Include license [https://github.com/Domi020/made/issues/17]
8. Prepare README [https://github.com/Domi020/made/issues/18]

