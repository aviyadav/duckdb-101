CREATE SECRET (
    TYPE quack,
    TOKEN 'super_secret'
);

ATTACH 'quack:localhost' AS remote;
FROM remote.hello;

SELECT * FROM remote.hello;
