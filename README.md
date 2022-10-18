# README

## Introduction

Stravatracker helps you to track your strava workouts. The program consists of 3 components bundled in a program

1. The firstrun module, which guides you through the setup and configuration of the Strava API
2. The update module, which extracts activites from Strava into a csv
3. The analysis module, which analyses the raw csv files and produces
  1. An excel-formatted CSV
  2. Tables of yearly and monthly progress
  3. Graphs showing yearly progress and weekly averages

The Strava API code was based on [franchyze923](https://github.com/franchyze923/Code_From_Tutorials/tree/master/Strava_Api)

## Setup

0. Ensure python is installed
1. Unzip the folder in your computer
2. Use terminal and cd into the folder

> cd ~/Downloads/stravatracker

3. Type the following commands in

> pip3 install -r requirements.txt
> mkdir data

4. To run the file

> python3 stravatracker/stravatracker.py

## Usage

On first run, you will be prompted to set up the Strava API through the web browser. This will trigger the initial download of files.

The Strava API has a rate-limit of 100 requests per 15 minutes and 1000 requests per day. As such, it will take multiple updates to complete the download of data. The program will let you know when the rate limit has been exceeded, and the remaining time before it can be run again.

