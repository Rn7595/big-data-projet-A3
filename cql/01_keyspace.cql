-- ---------------------------------------------------------------------------
-- Keyspace de l'application.
--
-- SimpleStrategy avec un facteur de replication de 1 : le cluster de
-- demonstration est un noeud unique, il n'y a donc rien a repliquer. Toute
-- autre valeur produirait un keyspace incapable de satisfaire ses propres
-- exigences de coherence.
--
-- En production, on ecrirait :
--   NetworkTopologyStrategy avec RF = 3 par datacenter, et des lectures comme
--   des ecritures en LOCAL_QUORUM. Avec RF = 3, QUORUM vaut 2 : la regle
--   R + W > RF (2 + 2 > 3) garantit qu'une lecture voit toujours la derniere
--   ecriture, tout en tolerant la perte d'un noeud.
--
-- C'est le choix du snitch GossipingPropertyFileSnitch dans docker-compose.yml
-- qui rendrait ce passage possible sans reconfiguration.
-- ---------------------------------------------------------------------------

CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
