-- Remise a zero du schema applicatif.
-- Convention du projet : chaque instruction est terminee par un "/" seul sur sa
-- ligne (style SQL*Plus). C'est ce separateur qu'utilise le lanceur Python, ce
-- qui permet de melanger sans ambiguite du DDL et des blocs PL/SQL.

BEGIN
  FOR t IN (
    SELECT table_name
    FROM   user_tables
    WHERE  table_name IN (
             'ORDER_ITEMS', 'ORDERS', 'ADDRESSES', 'CUSTOMERS',
             'PRODUCTS', 'CATEGORIES', 'PAYMENT_METHODS', 'COUNTRIES'
           )
  ) LOOP
    EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' CASCADE CONSTRAINTS PURGE';
  END LOOP;
END;
/
