{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "e71c3d08",
   "metadata": {},
   "source": [
    "### Quiz Questions\n",
    "\n",
    "Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.\n",
    "\n",
    "1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?\n",
    "- 128.3 MiB\n",
    "- **134.5 MiB**\n",
    "- 364.7 MiB\n",
    "- 692.6 MiB"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "2278aadf",
   "metadata": {},
   "source": [
    "q1-q2.yaml"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "de6a74af",
   "metadata": {},
   "source": [
    "2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?\n",
    "- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` \n",
    "- **`green_tripdata_2020-04.csv`**\n",
    "- `green_tripdata_04_2020.csv`\n",
    "- `green_tripdata_2020.csv`"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "23381411",
   "metadata": {}  
  },
  {
   "cell_type": "markdown",
   "id": "94d5af02",
   "metadata": {},
   "source": [
    "3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?\n",
    "- 13,537.299\n",
    "- **24,648,499**\n",
    "- 18,324,219\n",
    "- 29,430,127"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "04283277",
   "metadata": {}   
  },
  {
   "cell_type": "markdown",
   "id": "4c1a8589",
   "metadata": {},
   "source": [
    "4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?\n",
    "- 5,327,301\n",
    "- 936,199\n",
    "- **1,734,051**\n",
    "- 1,342,034"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "80c56354",
   "metadata": {}   
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c93f1acd",
   "metadata": {},
   "source": [
    "5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?\n",
    "- 1,428,092\n",
    "- 706,911\n",
    "- **1,925,152**\n",
    "- 2,561,031"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "f5d076b9",
   "metadata": {}   
  },
  {
   "cell_type": "markdown",
   "id": "d2a4f5ce",
   "metadata": {},
   "source": [
    "6) How would you configure the timezone to New York in a Schedule trigger?\n",
    "- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  \n",
    "- **Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**\n",
    "- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration\n",
    "- Add a `location` property set to `New_York` in the `Schedule` trigger configuration "
   ]
  },
  {
   "cell_type": "markdown",
   "id": "a1d2a5ac",
   "metadata": {}   
  }
 ],
 "metadata": {
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}