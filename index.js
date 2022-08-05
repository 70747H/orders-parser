const fs = require("fs");
const csvParser = require("csv-parser");
const { Parser } = require('json2csv');

const result_0 = [];
const result_1 = [];
const parsedOrders = {};
let ordersCount = 0;

function calculatePopularBrand(brands) {
    let max = 0;
    let popular = '';
    for (const key in brands) {
        if(brands[key] > max) {
            popular = key;
            max = brands[key];
        }
    }
    return popular;
}

function calculateAvgAndPopularBrand(orders, count) {
    for (const key in orders) {
        result_0.push({
            productName: key,
            average: orders[key].total / count,
        });
        result_1.push({
            productName: key,
            popularBrand: calculatePopularBrand(orders[key].brands)
        });
    }
}

function generateOutputFiles() {
    const options = { header: false, quote: '' };
    let parser = new Parser(options);

    const csv_0 = parser.parse(result_0);
    fs.writeFile(`0_${process.env.dataFile}`, csv_0, err => {
        if (err) {
          console.error(err);
        }
      });
    
      parser = new Parser(options);
    const csv_1 = parser.parse(result_1);
    fs.writeFile(`1_${process.env.dataFile}`, csv_1, err => {
        if (err) {
        console.error(err);
        }
    });
}

fs.createReadStream(process.env.dataFile)
  .pipe(csvParser({ headers: ['id', 'area', 'name', 'quantity', 'brand'] }))
  .on("data", (data) => {
    ordersCount ++;
    if(!parsedOrders[data.name]) {
        parsedOrders[data.name] = { total: Number(data.quantity), brands: {} };
    }else {
        parsedOrders[data.name].total += Number(data.quantity);
    }

    if(!parsedOrders[data.name].brands[data.brand]) {
        parsedOrders[data.name].brands[data.brand] = 1;
    }else {
        parsedOrders[data.name].brands[data.brand] += 1;
    }
  })
  .on("end", () => {
    calculateAvgAndPopularBrand(parsedOrders, ordersCount);
    generateOutputFiles()
  });
