CREATE TABLE orders AS
    FROM VALUES
        (11, 'west', 'carol', 150),
        (12, 'west', 'dave', 250),
        (13, 'west', 'eve', 180),
        (14, 'west', 'frank', 200),
        (15, 'west', 'george', 160)
    v(id, region, customer, amount);

CALL quack_identify(
    name => 'server2',
    provider => 'docker',
    region => 'west',
    meta => '{"lab": "lab2", "shard": 2}'
);

CALL quack_serve(
    'quack:0.0.0.0:9494',
    token = 'server2_secret',
    allow_other_hostname => true
);
