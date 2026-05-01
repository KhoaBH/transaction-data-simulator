# Transaction Data Simulator

This Node.js script reads CSV data from `src/` and sends each row to Azure Event Hubs.

## Requirements

- Node.js 18 or later
- An Azure Event Hubs namespace
- An Event Hub named `event-hub`, or update `eventHubName` in [index.js](index.js)

## Install

```bash
npm install
```

## Configure

Create a `.env` file in the project root:

```env
AZURE_CONNECTION_STRING=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<key-value>;EntityPath=<event-hub-name>
```

## Run

```bash
node index.js
```

## Input file

The script reads:

```text
src/Final_Iowa_Liquor_Sales2022.csv
```

## Progress

The app stores the last sent row in `progress.json` so it can resume after a restart.
