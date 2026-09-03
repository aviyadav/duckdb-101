CREATE TABLE orders AS
    FROM VALUES
        (1, 'east', 'alice', 100),
        (2, 'east', 'bob', 200),
        (3, 'east', 'carol', 150),
        (4, 'east', 'dave', 250),
        (5, 'east', 'eve', 180)
    v(id, region, customer, amount);

CALL quack_identify(
    name => 'server1',
    provider => 'docker',
    region => 'east',
    meta => '{"lab": "lab2", "shard": 1}'
);

CALL quack_serve(
    'quack:0.0.0.0:9494',
    token = 'server1_secret',
    allow_other_hostname => true
);
