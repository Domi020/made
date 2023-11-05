# Project Plan

## Title
<!-- Give your project a short title. -->
Development of driving accident numbers among young drivers with the introduction of BF17 (Begleitetes Fahren ab 17)

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
- How did the numbers of driving accidents caused by young drivers in the "Probezeit" develop with the introduction of BF17 in the year of 2011?
- Especially: Has the accident been noticeably reduced, or has it not really changed at all?

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

1. Example Issue [#1][i1]
2. ...


- Pipeline to import data
    - script to read XLSX
    - script to download file
- prepare data
    - select correct excel files
    - combine to one dataframe
    - data preparation
        - analyse attributes
        - cleansing
- display data as diagrams
    - select


[i1]: https://github.com/jvalue/made-template/issues/1
