CREATE SECRET coordinator_credentials (
    TYPE quack,
    SCOPE 'quack:localhost:9493',
    TOKEN 'coord_secret'
);

ATTACH 'quack:localhost:9493' AS coord (TYPE quack, DISABLE_SSL true);

-- Federated view assembled by the coordinator
FROM coord.all_orders ORDER BY id;

-- Who am I talking to?
FROM coord.query('FROM whoami()');

-- Reach individual shards through the coordinator session
FROM coord.query('FROM server1.orders ORDER BY id');
FROM coord.query('FROM server2.orders ORDER BY id');
