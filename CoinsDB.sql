CREATE TABLE Cryptos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(50),
    name VARCHAR(100),
    marketCap DECIMAL(18, 2),
    price DECIMAL(18, 2),
    change DECIMAL(18, 2),
    volume24h DECIMAL(18, 2)
);