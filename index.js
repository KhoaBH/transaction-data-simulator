const { EventHubProducerClient } = require("@azure/event-hubs");
const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");

require('dotenv').config(); // Nạp biến môi trường từ file .env
const { EventHubProducerClient } = require("@azure/event-hubs");
// ... các thư viện khác giữ nguyên

// Gọi biến từ .env thay vì viết trực tiếp
const connectionString = process.env.AZURE_CONNECTION_STRING;
const eventHubName = "event-hub";
const filePath = path.join(__dirname, "src", "Final_Iowa_Liquor_Sales2022.csv");
const progressPath = path.join(__dirname, "progress.json");

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

      const interval = setInterval(async () => {
        if (currentIndex >= records.length) {
          console.log("Hoan thanh!");
          clearInterval(interval);
          await producer.close();
          return;
        }

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
          BottleVolumeMl: parseInt(raw["bottle_volume (ml)"]) || 0,
          RetailPrice: parseFloat(raw["retail_price"]) || 0,
          BottlesSold: parseInt(raw["bottles_sold"]) || 0,
          SaleDollars: parseFloat(raw["sale_(dollars)"]) || 0,
          VolumeSoldLiters: parseFloat(raw["volume_sold(liters)"]) || 0,
          DateId: parseInt(raw["date_id"]) || 0,
          EventTimestamp: new Date().toISOString(),
        };

        try {
          const batch = await producer.createBatch();
          batch.tryAdd({ body: transaction });
          await producer.sendBatch(batch);
          console.log(`[${currentIndex + 1}] Sent: ${transaction.SalesId}`);

          fs.writeFileSync(
            progressPath,
            JSON.stringify({ lastIndex: currentIndex }),
          );
          currentIndex++;
        } catch (err) {
          console.error("Loi gui:", err.message);
        }
      }, 1000);
    });
}

main().catch(console.error);
