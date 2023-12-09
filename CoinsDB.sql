CREATE TABLE Cryptos (
    symbol VARCHAR(50) AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    marketCap DECIMAL(18, 2),
    price DECIMAL(18, 2),
    change DECIMAL(18, 2),
    24hVolume DECIMAL(18, 2)
);