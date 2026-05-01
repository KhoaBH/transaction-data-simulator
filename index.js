const { EventHubProducerClient } = require("@azure/event-hubs");
const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");

require("dotenv").config(); 


const connectionString = process.env.AZURE_CONNECTION_STRING;
const eventHubName = "event-hub";
const filePath = path.join(__dirname, "src", "Final_Iowa_Liquor_Sales2022.csv");
const progressPath = path.join(__dirname, "progress.json");
const ROWS_PER_SECOND = 1000;

async function main() {
  const producer = new EventHubProducerClient(connectionString, eventHubName);
  const records = [];

  let lastIndex = -1;
  if (fs.existsSync(progressPath)) {
    lastIndex = JSON.parse(fs.readFileSync(progressPath, "utf8")).lastIndex;
    console.log(`Tiep tuc tu dong: ${lastIndex + 1}`);
  }

  console.log("Dang nap toan bo dataset...");

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (data) => records.push(data))
    .on("end", async () => {
      console.log(`Da load ${records.length} dong. Bat dau day du lieu...`);

      let currentIndex = lastIndex + 1;
      let isProcessing = false;

      const interval = setInterval(async () => {
        if (isProcessing) return;

        if (currentIndex >= records.length) {
          console.log("Hoan thanh!");
          clearInterval(interval);
          await producer.close();
          return;
        }

        isProcessing = true;

        try {
          let sentThisTick = 0;
          let batch = await producer.createBatch();

          while (
            currentIndex < records.length &&
            sentThisTick < ROWS_PER_SECOND
          ) {
            const raw = records[currentIndex];
            const transaction = {
              SalesId: raw["sales_id"],
              SalesDate: raw["date"],
              StoreId: parseInt(raw["store_id"]) || 0,
              StoreName: raw["store_name"],
              City: raw["city"],
              County: raw["county"],
              CategoryId: parseInt(raw["category_id"]) || 0,
              CategoryName: raw["category_name"],
              VendorId: parseInt(raw["vendor_id"]) || 0,
              VendorName: raw["vendor_name"],
              ProductId: parseInt(raw["product_id"]) || 0,
              ProductName: raw["product_name"],
              Pack: parseInt(raw["pack"]) || 0,
              BottleVolumeMl: parseInt(raw["bottle_volume(ml)"]) || 0,
              RetailPrice: parseFloat(raw["retail_price"]) || 0,
              BottlesSold: parseInt(raw["bottles_sold"]) || 0,
              SaleDollars: parseFloat(raw["sale_(dollars)"]) || 0,
              VolumeSoldLiters: parseFloat(raw["volume_sold(liters)"]) || 0,
              DateId: parseInt(raw["date_id"]) || 0,
              EventTimestamp: new Date().toISOString(),
            };

            if (!batch.tryAdd({ body: transaction })) {
              await producer.sendBatch(batch);
              batch = await producer.createBatch();

              if (!batch.tryAdd({ body: transaction })) {
                console.error(`Bo qua dong ${currentIndex + 1}: event qua lon`);
                currentIndex++;
                continue;
              }
            }

            currentIndex++;
            sentThisTick++;
          }

          if (batch.count > 0) {
            await producer.sendBatch(batch);
          }

          fs.writeFileSync(
            progressPath,
            JSON.stringify({ lastIndex: currentIndex - 1 }),
          );

          console.log(
            `Da gui ${sentThisTick} dong trong 1 giay. Tong da gui: ${currentIndex}/${records.length}`,
          );
        } catch (err) {
          console.error("Loi gui:", err.message);
        } finally {
          isProcessing = false;
        }
      }, 1000);
    });
}

main().catch(console.error);
