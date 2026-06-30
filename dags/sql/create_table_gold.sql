CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.HOUSE_SALES_GOLD (
    id BIGINT,
    date TIMESTAMP,
    price DOUBLE PRECISION,
    bathrooms INT,
    bedrooms INT,
    sqft_living DOUBLE PRECISION,
    sqft_lot DOUBLE PRECISION,
    floors DOUBLE PRECISION,
    waterfront BOOLEAN,
    view INT,
    condition DOUBLE PRECISION,
    grade DOUBLE PRECISION,
    sqft_above DOUBLE PRECISION,
    sqft_basement DOUBLE PRECISION,
    yr_built INT,
    yr_renovated INT,
    zipcode VARCHAR(20),
    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,
    sqft_living15 DOUBLE PRECISION,
    sqft_lot15 DOUBLE PRECISION,
    idade_imovel INT,
    foi_renovado BOOLEAN,
    tem_porao BOOLEAN,
    classificacao_tamanho_imovel VARCHAR(15)
);
