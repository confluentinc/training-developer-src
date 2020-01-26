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
    ('driver-1', 'Randall', 'Palmer', 'Toyota', 'Offset');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-2', 'Razı', 'İnönü', 'Nissan', 'Narkhede');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-3', '美加子', '桐山', 'Subaru', 'Shapira');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-4', 'David', 'Chandler', 'Subaru', 'Shapira');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-5', 'James', 'King', 'Nissan', 'Narkhede');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-6', 'William', 'Peterson', 'GM', 'Berglund');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-7', 'Matthew', 'Reyes', 'Hyundai', 'Palino');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-8', 'Tezol', 'Manço', 'Nissan', 'Narkhede');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-9', '加奈', '小林', 'GM', 'Berglund');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-10', 'Aaron', 'Gill', 'Hyundai', 'Palino');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-11', 'Kathy', 'White', 'Nissan', 'Narkhede');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-12', 'Brenda', 'Schmidt', 'GM', 'Berglund');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-13', 'Kevin', 'Cook', 'Hyundai', 'Palino');

INSERT INTO driver (driverkey, firstname, lastname, make, model) VALUES
    ('driver-14', 'Anna', 'Salazar', 'Subaru', 'Shapira');