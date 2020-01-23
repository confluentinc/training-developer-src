-- timestamp doesn't do what I was hoping 
-- https://stackoverflow.com/questions/1035980/update-timestamp-when-row-is-updated-in-postgresql
CREATE TABLE driver (
    id SERIAL PRIMARY KEY not null,
    driverkey varchar(50) not null,
    firstname varchar(50) not null,
    lastname varchar(50) not null,
    make varchar(50) not null,
    model varchar(50) not null,
    timestamp timestamp default current_timestamp not null
);

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-1', 'Randall', 'Palmer', 'Toyota', 'Topic');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-2', 'Razı', 'İnönü', 'Nissan', 'Offset');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-3', '美加子', '桐山', 'VW', 'Broker');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-4', 'David', 'Chandler', 'VW', 'Broker');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-5', 'James', 'King', 'Nissan', 'Offset');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-6', 'William', 'Peterson', 'GM', 'Compaction');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-7', 'Matthew', 'Reyes', 'Hyundai', 'Replica');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-8', 'Tezol', 'Manço', 'Nissan', 'Offset');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-9', '加奈', '小林', 'GM', 'Compaction');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-10', 'Aaron', 'Gill', 'Hyundai', 'Replica');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-11', 'Kathy', 'White', 'Nissan', 'Offset');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-12', 'Brenda', 'Schmidt', 'GM', 'Compaction');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-13', 'Kevin', 'Cook', 'Hyundai', 'Replica');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-14', 'Anna', 'Salazar', 'VW', 'Broker');