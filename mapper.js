const fs = require('fs');
const csv = require('csv-parser');
const pool = require('./db');

const mallsFile = './data/1252_malls_final.json';
const brandsFile = './data/Offlinedump-new.csv';

function normalize(text) {
    if (!text) return '';
    return text
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, ' ')
        .replace(/\b(store|exclusive|outlet|world|mall|ltd|pvt|private|limited|shop)\b/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

async function loadBrands() {
    return new Promise((resolve) => {
        const brands = [];
        fs.createReadStream(brandsFile)
            .pipe(csv())
            .on('data', (row) => {
                const brandName = row['Brand Name']?.trim();
                const productId = row['Product ID'];
                const variations = row['Variations']
                    ? row['Variations'].split(',')
                    : [brandName];

                variations.forEach((v) => {
                    brands.push({
                        brandName,
                        productId,
                        variation: normalize(v)
                    });
                });
            })
            .on('end', () => resolve(brands));
    });
}

async function processMalls() {
    const malls = JSON.parse(fs.readFileSync(mallsFile));
    const brands = await loadBrands();

    for (const mall of malls) {
        const mallName = mall.Name;
        const city = mall.City;
        const state = mall.State;
        const directory = mall.directory || [];

        const foundBrands = {};

        for (const store of directory) {
            const cleanStore = normalize(store);

            for (const brand of brands) {
                if (
                    brand.variation &&
                    cleanStore.includes(brand.variation)
                ) {
                    if (!foundBrands[brand.brandName]) {
                        foundBrands[brand.brandName] = {
                            brand_name: brand.brandName,
                            product_id: brand.productId,
                            matched_store: store
                        };
                    }
                }
            }
        }

        const productList = Object.values(foundBrands);

        // Skip mall if no brand found
        if (productList.length === 0) continue;

        await saveToDatabase(mallName, city, state, productList);
    }

    console.log('Mapping completed successfully');
}

async function saveToDatabase(mallName, city, state, products) {
    const query = `
        INSERT INTO mall_brand_mapping
        (mall_name, city, state, products)
        VALUES (?, ?, ?, ?)
    `;

    await pool.execute(query, [
        mallName,
        city,
        state,
        JSON.stringify(products)
    ]);
}

processMalls().catch(console.error);