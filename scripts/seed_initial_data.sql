-- FX Data Platform - Seed Data Script
-- Populates initial test data into the database

-- Seed users (10 example users)
INSERT INTO users (name, cpf, email, state, tier, avg_monthly_volume) VALUES
('João Silva', '12345678901', 'joao@example.com', 'SP', 'standard', 5000.00),
('Maria Santos', '23456789012', 'maria@example.com', 'RJ', 'gold', 15000.00),
('Carlos Oliveira', '34567890123', 'carlos@example.com', 'MG', 'platinum', 50000.00),
('Ana Costa', '45678901234', 'ana@example.com', 'SP', 'standard', 3000.00),
('Pedro Souza', '56789012345', 'pedro@example.com', 'BA', 'gold', 12000.00),
('Lucia Ferreira', '67890123456', 'lucia@example.com', 'RS', 'standard', 2000.00),
('Roberto Alves', '78901234567', 'roberto@example.com', 'SP', 'gold', 25000.00),
('Fernanda Lima', '89012345678', 'fernanda@example.com', 'PR', 'platinum', 75000.00),
('Ricardo Gomes', '90123456789', 'ricardo@example.com', 'MG', 'standard', 4000.00),
('Beatriz Martins', '01234567890', 'beatriz@example.com', 'SP', 'gold', 18000.00);

-- We cannot seed transactions and exchange_rates here because they need CURRENT_TIMESTAMP
-- They will be populated by the Python seed script (scripts/seed_data.py)

-- Create sequence for batch ID generation (optional)
CREATE SEQUENCE IF NOT EXISTS batch_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
