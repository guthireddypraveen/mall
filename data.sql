CREATE TABLE mall_brand_mapping (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mall_name VARCHAR(255),
    city VARCHAR(150),
    state VARCHAR(150),
    products JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);