CREATE SECRET server1_credentials (
    TYPE quack,
    SCOPE 'quack:server1:9494',
    TOKEN 'server1_secret'
);

CREATE SECRET server2_credentials (
    TYPE quack,
    SCOPE 'quack:server2:9494',
    TOKEN 'server2_secret'
);

ATTACH 'quack:server1:9494' AS server1 (TYPE quack, DISABLE_SSL true);
ATTACH 'quack:server2:9494' AS server2 (TYPE quack, DISABLE_SSL true);

CREATE VIEW all_orders AS
    SELECT *, 'server1' AS source FROM server1.orders
    UNION ALL
    SELECT *, 'server2' AS source FROM server2.orders;

CALL quack_identify(
    name => 'coordinator',
    provider => 'docker',
    meta => '{"lab": "lab2", "role": "coordinator"}'
);

CALL quack_serve(
    'quack:0.0.0.0:9494',
    token = 'coord_secret',
    allow_other_hostname => true
);
